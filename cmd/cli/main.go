package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"easyrun/internal/types"

	"github.com/urfave/cli/v2"
)

var leaderAddr string

func main() {
	app := &cli.App{
		Name:  "easyrun",
		Usage: "EasyRun CLI",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "leader",
				Usage:       "Leader address",
				Value:       "localhost:9080",
				Destination: &leaderAddr,
				EnvVars:     []string{"EASYRUN_LEADER"},
			},
		},
		Commands: []*cli.Command{
			runCommand(),
			stopCommand(),
			statusCommand(),
			agentsCommand(),
			logsCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Run a job",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "name", Required: true},
			&cli.StringFlag{Name: "command", Required: true},
			&cli.StringFlag{Name: "artifact", Usage: "Artifact URL"},
			&cli.IntFlag{Name: "cpu", Usage: "CPU shares"},
			&cli.StringFlag{Name: "memory", Usage: "Memory limit (e.g., 512M, 1G)"},
			&cli.StringSliceFlag{Name: "env", Usage: "Environment variables (KEY=VALUE)"},
		},
		Action: runJob,
	}
}

func stopCommand() *cli.Command {
	return &cli.Command{
		Name:      "stop",
		Usage:     "Stop a job",
		ArgsUsage: "<job-id>",
		Action:    stopJob,
	}
}

func statusCommand() *cli.Command {
	return &cli.Command{
		Name:   "status",
		Usage:  "Show cluster status",
		Action: showStatus,
	}
}

func agentsCommand() *cli.Command {
	return &cli.Command{
		Name:      "agents",
		Usage:     "List agents or show agent details",
		ArgsUsage: "[agent-id]",
		Action:    listAgents,
	}
}

func runJob(c *cli.Context) error {
	job := types.Job{
		Name:      c.String("name"),
		Command:   c.String("command"),
		CPUShares: c.Int("cpu"),
	}

	if artifact := c.String("artifact"); artifact != "" {
		job.Artifact = &types.Artifact{
			URL: artifact,
		}
	}

	if mem := c.String("memory"); mem != "" {
		memBytes, err := parseMemory(mem)
		if err != nil {
			return err
		}
		job.MemoryLimit = memBytes
	}

	if envs := c.StringSlice("env"); len(envs) > 0 {
		job.Env = make(map[string]string)
		for _, env := range envs {
			for i, ch := range env {
				if ch == '=' {
					job.Env[env[:i]] = env[i+1:]
					break
				}
			}
		}
	}

	resp, err := doRequest("POST", "/v1/jobs", job)
	if err != nil {
		return err
	}

	var result map[string]string
	if err := json.Unmarshal(resp, &result); err != nil {
		return err
	}

	fmt.Printf("Job '%s' dispatched with ID %s\n", job.Name, result["id"])
	return nil
}

func stopJob(c *cli.Context) error {
	if c.NArg() < 1 {
		return fmt.Errorf("job id required")
	}
	jobID := c.Args().First()

	_, err := doRequest("DELETE", "/v1/jobs/"+jobID, nil)
	if err != nil {
		return err
	}

	fmt.Printf("Job '%s' stop requested\n", jobID)
	return nil
}

func showStatus(c *cli.Context) error {
	resp, err := doRequest("GET", "/v1/status", nil)
	if err != nil {
		return err
	}

	var status struct {
		Agents       int                      `json:"agents"`
		TotalTasks   int                      `json:"total_tasks"`
		RunningTasks int                      `json:"running_tasks"`
		TasksByAgent map[string][]*types.Task `json:"tasks_by_agent"`
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return err
	}

	// Fetch jobs for expected vs running display
	jobsResp, err := doRequest("GET", "/v1/jobs", nil)
	if err != nil {
		return err
	}

	var jobs []*types.Job
	if err := json.Unmarshal(jobsResp, &jobs); err != nil {
		return err
	}

	fmt.Printf("Leader:  %s\n", leaderAddr)
	fmt.Printf("Agents:  %d\n", status.Agents)
	fmt.Printf("Tasks:   %d running / %d total\n", status.RunningTasks, status.TotalTasks)
	fmt.Println()

	// Count running tasks per job
	runningPerJob := make(map[string]int)
	for _, tasks := range status.TasksByAgent {
		for _, task := range tasks {
			if task.State == "running" {
				runningPerJob[task.JobID]++
			}
		}
	}

	// Show jobs with expected vs running
	if len(jobs) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "JOB ID\tNAME\tRUNNING\tSTATUS")
		for _, job := range jobs {
			expected := job.Count
			if expected == -1 {
				expected = status.Agents
			}
			if expected == 0 {
				expected = 1
			}
			running := runningPerJob[job.ID]
			statusStr := "OK"
			if running < expected {
				statusStr = "DEGRADED"
			}
			expectedStr := fmt.Sprintf("%d", expected)
			if job.Count == -1 {
				expectedStr = fmt.Sprintf("all(%d)", status.Agents)
			}
			fmt.Fprintf(w, "%s\t%s\t%d / %s\t%s\n",
				job.ID, job.Name, running, expectedStr, statusStr)
		}
		w.Flush()
		fmt.Println()
	}

	if len(status.TasksByAgent) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "AGENT\tTASK\tJOB\tPORTS\tSTATE")
		for agent, tasks := range status.TasksByAgent {
			for _, task := range tasks {
				// Format ports as "http:8080,grpc:9090"
				portsStr := ""
				for name, port := range task.Ports {
					if portsStr != "" {
						portsStr += ","
					}
					portsStr += fmt.Sprintf("%s:%d", name, port)
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
					agent, task.ID, task.JobName, portsStr, task.State)
			}
		}
		w.Flush()
	}
	return nil
}

func listAgents(c *cli.Context) error {
	resp, err := doRequest("GET", "/v1/agents", nil)
	if err != nil {
		return err
	}

	var agents []*types.Agent
	if err := json.Unmarshal(resp, &agents); err != nil {
		return err
	}

	// If agent ID provided, show details for that agent
	if c.NArg() > 0 {
		agentID := c.Args().First()
		var agent *types.Agent
		for _, a := range agents {
			if a.ID == agentID {
				agent = a
				break
			}
		}
		if agent == nil {
			return fmt.Errorf("agent %s not found", agentID)
		}
		return showAgentDetails(agent)
	}

	// List all agents
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tENDPOINT\tLAST SEEN")
	for _, agent := range agents {
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			agent.ID, agent.Endpoint, agent.LastSeen.Format("15:04:05"))
	}
	w.Flush()
	return nil
}

func showAgentDetails(agent *types.Agent) error {
	fmt.Printf("Agent:    %s\n", agent.ID)
	fmt.Printf("Endpoint: %s\n", agent.Endpoint)
	fmt.Printf("LastSeen: %s\n", agent.LastSeen.Format("15:04:05"))
	fmt.Println()

	// Fetch capacity from agent directly
	capResp, err := http.Get(agent.Endpoint + "/capacity")
	if err != nil {
		fmt.Printf("Capacity: (unavailable - %v)\n", err)
		return nil
	}
	defer capResp.Body.Close()

	if capResp.StatusCode != http.StatusOK {
		fmt.Printf("Capacity: (unavailable - status %d)\n", capResp.StatusCode)
		return nil
	}

	var cap struct {
		CPUCores    int    `json:"cpu_cores"`
		MemoryBytes uint64 `json:"memory_bytes"`
	}
	if err := json.NewDecoder(capResp.Body).Decode(&cap); err != nil {
		fmt.Printf("Capacity: (unavailable - %v)\n", err)
		return nil
	}

	// Fetch status to calculate used resources
	statusResp, err := doRequest("GET", "/v1/status", nil)
	if err != nil {
		fmt.Printf("CPU:      %d cores\n", cap.CPUCores)
		fmt.Printf("Memory:   %.1f GB\n", float64(cap.MemoryBytes)/(1024*1024*1024))
		return nil
	}

	var status struct {
		TasksByAgent map[string][]*types.Task `json:"tasks_by_agent"`
	}
	json.Unmarshal(statusResp, &status)

	// Fetch jobs for resource info
	jobsResp, _ := doRequest("GET", "/v1/jobs", nil)
	var jobs []*types.Job
	json.Unmarshal(jobsResp, &jobs)

	jobMap := make(map[string]*types.Job)
	for _, j := range jobs {
		jobMap[j.ID] = j
	}

	// Calculate used resources
	var usedCPU int
	var usedMem uint64
	if tasks, ok := status.TasksByAgent[agent.ID]; ok {
		for _, t := range tasks {
			if t.State == "running" {
				if job := jobMap[t.JobID]; job != nil {
					usedCPU += job.CPUShares
					usedMem += job.MemoryLimit
				}
			}
		}
	}

	totalShares := cap.CPUCores * 1024
	usedCores := float64(usedCPU) / 1024
	totalGB := float64(cap.MemoryBytes) / (1024 * 1024 * 1024)
	usedGB := float64(usedMem) / (1024 * 1024 * 1024)

	fmt.Printf("CPU:      %.1f / %d cores (%.0f / %d shares)\n", usedCores, cap.CPUCores, float64(usedCPU), totalShares)
	fmt.Printf("Memory:   %.1f / %.0f GB\n", usedGB, totalGB)

	return nil
}

func logsCommand() *cli.Command {
	return &cli.Command{
		Name:      "logs",
		Usage:     "Stream task logs",
		ArgsUsage: "<task-id>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "stream",
				Usage: "Log stream (stdout or stderr)",
				Value: "stdout",
			},
		},
		Action: streamLogs,
	}
}

func streamLogs(c *cli.Context) error {
	taskID := c.Args().First()
	if taskID == "" {
		return fmt.Errorf("task ID required")
	}

	stream := c.String("stream")
	if stream != "stdout" && stream != "stderr" {
		return fmt.Errorf("stream must be stdout or stderr")
	}

	// Get cluster status to find which agent has this task
	resp, err := doRequest("GET", "/v1/status", nil)
	if err != nil {
		return err
	}

	var status struct {
		TasksByAgent map[string][]*types.Task `json:"tasks_by_agent"`
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return err
	}

	// Find task and its agent
	var agentEndpoint string
	for agent, tasks := range status.TasksByAgent {
		for _, task := range tasks {
			if task.ID == taskID {
				// Extract agent endpoint from agent ID or use direct connection
				// For now, assume agent is hostname:port format
				agentEndpoint = agent
				break
			}
		}
		if agentEndpoint != "" {
			break
		}
	}

	if agentEndpoint == "" {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Stream logs from agent
	url := fmt.Sprintf("http://%s/logs/%s/%s", agentEndpoint, taskID, stream)
	resp2, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to connect to agent: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		return fmt.Errorf("agent returned status %d", resp2.StatusCode)
	}

	// Stream SSE events to stdout
	scanner := bufio.NewScanner(resp2.Body)
	for scanner.Scan() {
		line := scanner.Text()
		// SSE format: "data: <content>"
		if strings.HasPrefix(line, "data: ") {
			fmt.Print(strings.TrimPrefix(line, "data: "))
		}
	}

	return scanner.Err()
}

// HTTP helpers

func doRequest(method, path string, body any) ([]byte, error) {
	url := fmt.Sprintf("http://%s%s", leaderAddr, path)

	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp map[string]string
		json.Unmarshal(respBody, &errResp)
		if msg, ok := errResp["error"]; ok {
			return nil, fmt.Errorf("%s", msg)
		}
		return nil, fmt.Errorf("request failed: %s", resp.Status)
	}

	return respBody, nil
}

// Memory parsing helpers

func parseMemory(s string) (uint64, error) {
	if len(s) == 0 {
		return 0, nil
	}

	unit := s[len(s)-1]
	numStr := s[:len(s)-1]

	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory value: %s", s)
	}

	switch unit {
	case 'K', 'k':
		return num * 1024, nil
	case 'M', 'm':
		return num * 1024 * 1024, nil
	case 'G', 'g':
		return num * 1024 * 1024 * 1024, nil
	default:
		return strconv.ParseUint(s, 10, 64)
	}
}
