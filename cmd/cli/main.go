package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"easyrun/internal/types"

	"github.com/urfave/cli/v2"
)

var leaderAddr string

func main() {
	app := &cli.App{
		Name:  "run",
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
			deployCommand(),
			deleteCommand(),
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

func deployCommand() *cli.Command {
	return &cli.Command{
		Name:  "deploy",
		Usage: "Deploy or update a job (upsert based on name)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "name", Required: true},
			&cli.StringFlag{Name: "command", Usage: "Command to run (required for process jobs)"},
			&cli.StringFlag{Name: "image", Usage: "Docker image (uses Docker instead of process)"},
			&cli.StringSliceFlag{Name: "artifact", Usage: "Artifact URL, or match::URL for platform-specific (e.g. node.arch=arm64::https://example.com/app-arm64.tar.gz)"},
			&cli.IntFlag{Name: "cpu", Usage: "CPU shares"},
			&cli.StringFlag{Name: "memory", Usage: "Memory limit (e.g., 512M, 1G)"},
			&cli.StringSliceFlag{Name: "env", Usage: "Environment variables (KEY=VALUE)"},
			&cli.StringSliceFlag{Name: "affinity", Usage: "Node affinity constraints (key=value, e.g. node.arch=arm64)"},
			&cli.StringFlag{Name: "update-policy", Usage: "Update policy: rolling (default), recreate, or blue-green", Value: "rolling"},
			&cli.StringFlag{Name: "check-type", Usage: "Health check type: http, tcp, or file"},
			&cli.StringFlag{Name: "check-path", Usage: "Health check path (HTTP endpoint or file path)"},
			&cli.StringFlag{Name: "check-port", Usage: "Health check port name (for http/tcp, default: http)"},
			&cli.IntFlag{Name: "check-failures", Usage: "Consecutive failures before unhealthy (default: 3)"},
		},
		Action: deployJob,
	}
}

func deleteCommand() *cli.Command {
	return &cli.Command{
		Name:      "delete",
		Usage:     "Delete a job and all its tasks",
		ArgsUsage: "<job-name>",
		Action:    deleteJob,
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

func deployJob(c *cli.Context) error {
	job := types.Job{
		Name:         c.String("name"),
		Command:      c.String("command"),
		Image:        c.String("image"),
		CPUShares:    c.Int("cpu"),
		UpdatePolicy: types.UpdatePolicy(c.String("update-policy")),
	}

	if job.Command == "" && job.Image == "" {
		return fmt.Errorf("either --command or --image is required")
	}

	if artifacts := c.StringSlice("artifact"); len(artifacts) > 0 {
		for _, art := range artifacts {
			a := types.Artifact{}
			if idx := strings.Index(art, "::"); idx > 0 {
				// match::URL format (e.g. "node.arch=arm64,node.os=linux::https://example.com/app.tar.gz")
				a.URL = art[idx+2:]
				a.Match = make(map[string]string)
				for _, kv := range strings.Split(art[:idx], ",") {
					for i, ch := range kv {
						if ch == '=' {
							a.Match[kv[:i]] = kv[i+1:]
							break
						}
					}
				}
			} else {
				a.URL = art
			}
			job.Artifacts = append(job.Artifacts, a)
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

	if c.IsSet("check-type") || c.IsSet("check-path") {
		job.HealthCheck = &types.HealthCheck{
			Type:             c.String("check-type"),
			Path:             c.String("check-path"),
			Port:             c.String("check-port"),
			FailureThreshold: c.Int("check-failures"),
		}
	}

	if affinities := c.StringSlice("affinity"); len(affinities) > 0 {
		job.Affinity = make(map[string]string)
		for _, a := range affinities {
			for i, ch := range a {
				if ch == '=' {
					job.Affinity[a[:i]] = a[i+1:]
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

	// Show appropriate message based on operation
	status := result["status"]
	switch status {
	case "updated":
		policy := result["policy"]
		if policy == "" {
			policy = "rolling"
		}
		fmt.Printf("Job '%s' updated (ID %s, policy=%s)\n", job.Name, result["id"], policy)
	case "pending":
		fmt.Printf("Job '%s' stored (ID %s) — pending dispatch: %s\n", job.Name, result["id"], result["error"])
	default:
		fmt.Printf("Job '%s' dispatched with ID %s\n", job.Name, result["id"])
	}

	return nil
}

func deleteJob(c *cli.Context) error {
	if c.NArg() < 1 {
		return fmt.Errorf("job name required")
	}
	jobName := c.Args().First()

	_, err := doRequest("DELETE", "/v1/jobs/"+jobName, nil)
	if err != nil {
		return err
	}

	fmt.Printf("Job '%s' deleted\n", jobName)
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
				runningPerJob[task.JobName]++
			}
		}
	}

	// Show jobs with expected vs running
	if len(jobs) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tRUNNING\tSTATUS")
		for _, job := range jobs {
			expected := job.Count
			if expected == -1 {
				expected = status.Agents
			}
			if expected == 0 {
				expected = 1
			}
			running := runningPerJob[job.Name]
			statusStr := "OK"
			if running < expected {
				statusStr = "DEGRADED"
			}
			expectedStr := fmt.Sprintf("%d", expected)
			if job.Count == -1 {
				expectedStr = fmt.Sprintf("all(%d)", status.Agents)
			}
			fmt.Fprintf(w, "%s\t%d / %s\t%s\n",
				job.Name, running, expectedStr, statusStr)
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

	// Fetch capacity from agent directly (includes live usage)
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
		CPUCores        int               `json:"cpu_cores"`
		MemoryBytes     uint64            `json:"memory_bytes"`
		CPUUsedShares   int               `json:"cpu_used_shares"`
		MemoryUsedBytes uint64            `json:"memory_used_bytes"`
		TasksRunning    int               `json:"tasks_running"`
		Attributes      map[string]string `json:"attributes"`
	}
	if err := json.NewDecoder(capResp.Body).Decode(&cap); err != nil {
		fmt.Printf("Capacity: (unavailable - %v)\n", err)
		return nil
	}

	usedCores := float64(cap.CPUUsedShares) / 1024
	totalShares := cap.CPUCores * 1024
	totalGB := float64(cap.MemoryBytes) / (1024 * 1024 * 1024)
	usedGB := float64(cap.MemoryUsedBytes) / (1024 * 1024 * 1024)

	fmt.Printf("Tasks:    %d running\n", cap.TasksRunning)
	fmt.Printf("CPU:      %.1f / %d cores (%.0f / %d shares)\n", usedCores, cap.CPUCores, float64(cap.CPUUsedShares), totalShares)
	fmt.Printf("Memory:   %.1f / %.0f GB\n", usedGB, totalGB)

	if len(cap.Attributes) > 0 {
		fmt.Println()
		fmt.Println("Attributes:")
		keys := make([]string, 0, len(cap.Attributes))
		for k := range cap.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %s = %s\n", k, cap.Attributes[k])
		}
	}

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
		_ = json.Unmarshal(respBody, &errResp)
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
