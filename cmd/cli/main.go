package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
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
		Name:   "agents",
		Usage:  "List agents",
		Action: listAgents,
	}
}

func runJob(c *cli.Context) error {
	job := types.Job{
		Name:      c.String("name"),
		Command:   c.String("command"),
		CPUShares: c.Int("cpu"),
	}

	if artifact := c.String("artifact"); artifact != "" {
		job.ArtifactURL = artifact
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
		Agents       int                          `json:"agents"`
		TotalTasks   int                          `json:"total_tasks"`
		RunningTasks int                          `json:"running_tasks"`
		TasksByAgent map[string][]*types.Task     `json:"tasks_by_agent"`
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return err
	}

	fmt.Printf("Leader:  %s\n", leaderAddr)
	fmt.Printf("Agents:  %d\n", status.Agents)
	fmt.Printf("Tasks:   %d running / %d total\n", status.RunningTasks, status.TotalTasks)
	fmt.Println()

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

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tENDPOINT\tLAST SEEN")
	for _, agent := range agents {
		fmt.Fprintf(w, "%s\t%s\t%s\n",
			agent.ID, agent.Endpoint, agent.LastSeen.Format("15:04:05"))
	}
	w.Flush()
	return nil
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
