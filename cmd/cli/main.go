package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"hop/internal/types"
	"hop/pkg/httputil"
)

var (
	leaderAddr string
	apiKey     string
)

func main() {
	flag.StringVar(&leaderAddr, "leader", envOr("HOP_LEADER", "localhost:9080"), "Leader address")
	flag.StringVar(&apiKey, "api-key", os.Getenv("HOP_API_KEY"), "API key for authentication")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: run [flags] <command> [args]\n\nCommands:\n  apply    Create or update a job (upsert by name)\n  delete   Delete a job and all its tasks\n  status   Show cluster status\n  agents   List agents or show agent details\n  logs     Stream task logs\n\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var err error
	switch args[0] {
	case "apply":
		err = runApply(args[1:])
	case "delete":
		err = runDelete(args[1:])
	case "status":
		err = runStatus()
	case "agents":
		err = runAgents(args[1:])
	case "logs":
		err = runLogs(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func runApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	name := fs.String("name", "", "Job name (required)")
	command := fs.String("command", "", "Command to run")
	image := fs.String("image", "", "Docker image")
	cpu := fs.Int("cpu", 0, "CPU shares")
	memory := fs.String("memory", "", "Memory limit (e.g., 512M, 1G)")
	priorityFlag := fs.Int("priority", -1, "Scheduling priority (0=highest, omit to append at end)")
	updatePolicy := fs.String("update-policy", "rolling", "Update policy if job exists: rolling, recreate, or blue-green")
	checkType := fs.String("check-type", "", "Health check type: http, tcp, or file")
	checkPath := fs.String("check-path", "", "Health check path")
	checkPort := fs.String("check-port", "", "Health check port name")
	checkFailures := fs.Int("check-failures", 0, "Consecutive failures before unhealthy")

	var envFlags, artifactFlags, affinityFlags []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--env" || args[i] == "-env":
			if i+1 < len(args) {
				envFlags = append(envFlags, args[i+1])
				args = append(args[:i], args[i+2:]...)
				i--
			}
		case args[i] == "--artifact" || args[i] == "-artifact":
			if i+1 < len(args) {
				artifactFlags = append(artifactFlags, args[i+1])
				args = append(args[:i], args[i+2:]...)
				i--
			}
		case args[i] == "--affinity" || args[i] == "-affinity":
			if i+1 < len(args) {
				affinityFlags = append(affinityFlags, args[i+1])
				args = append(args[:i], args[i+2:]...)
				i--
			}
		}
	}

	fs.Parse(args)

	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	if *command == "" && *image == "" {
		return fmt.Errorf("either --command or --image is required")
	}

	job := buildJob(*name, *command, *image, *cpu, *memory, *priorityFlag,
		envFlags, artifactFlags, affinityFlags,
		*checkType, *checkPath, *checkPort, *checkFailures, *updatePolicy)

	resp, err := doRequest("POST", "/v1/jobs", job)
	if err != nil {
		return err
	}

	var result map[string]string
	if err := json.Unmarshal(resp, &result); err != nil {
		return err
	}

	switch result["status"] {
	case "updating":
		fmt.Printf("Job '%s' updating (policy=%s)\n", job.Name, result["policy"])
	case "pending":
		fmt.Printf("Job '%s' stored — pending dispatch: %s\n", job.Name, result["error"])
	default:
		fmt.Printf("Job '%s' dispatched\n", job.Name)
	}
	return nil
}

// buildJob constructs a Job from CLI flags
func buildJob(name, command, image string, cpu int, memory string, priorityFlag int,
	envFlags, artifactFlags, affinityFlags []string,
	checkType, checkPath, checkPort string, checkFailures int,
	updatePolicy string) types.Job {

	job := types.Job{
		Name:         name,
		Command:      command,
		Image:        image,
		CPUShares:    cpu,
		UpdatePolicy: types.UpdatePolicy(updatePolicy),
	}

	if priorityFlag >= 0 {
		p := priorityFlag
		job.Priority = &p
	}

	for _, art := range artifactFlags {
		a := types.Artifact{}
		if idx := strings.Index(art, "::"); idx > 0 {
			a.URL = art[idx+2:]
			a.Match = parseKV(art[:idx])
		} else {
			a.URL = art
		}
		job.Artifacts = append(job.Artifacts, a)
	}

	if memory != "" {
		memBytes, err := parseMemory(memory)
		if err == nil {
			job.MemoryLimit = memBytes
		}
	}

	if len(envFlags) > 0 {
		job.Env = parseKV(strings.Join(envFlags, ","))
	}

	if len(affinityFlags) > 0 {
		job.Affinity = parseKV(strings.Join(affinityFlags, ","))
	}

	if checkType != "" || checkPath != "" {
		job.HealthCheck = &types.HealthCheck{
			Type:             checkType,
			Path:             checkPath,
			Port:             checkPort,
			FailureThreshold: checkFailures,
		}
	}

	return job
}

func runDelete(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("job name required")
	}
	_, err := doRequest("DELETE", "/v1/jobs/"+args[0], nil)
	if err != nil {
		return err
	}
	fmt.Printf("Job deleted (%s)\n", args[0])
	return nil
}

func runStatus() error {
	resp, err := doRequest("GET", "/v1/status", nil)
	if err != nil {
		return err
	}

	var status struct {
		Agents       int            `json:"agents"`
		Settling     bool           `json:"settling"`
		TotalPlaced  int            `json:"total_placed"`
		Placed       map[string]int `json:"placed"`
	}
	if err := json.Unmarshal(resp, &status); err != nil {
		return err
	}

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
	fmt.Printf("Placed:  %d total\n", status.TotalPlaced)
	if status.Settling {
		fmt.Println("Status:  settling...")
	}
	fmt.Println()

	if len(jobs) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tPLACED\tEXPECTED\tSTATUS")
		for _, job := range jobs {
			expected := job.Count
			if expected == -1 {
				expected = status.Agents
			}
			if expected == 0 {
				expected = 1
			}
			placed := status.Placed[job.Name]
			statusStr := "OK"
			if placed < expected {
				statusStr = "DEGRADED"
			}
			expectedStr := fmt.Sprintf("%d", expected)
			if job.Count == -1 {
				expectedStr = fmt.Sprintf("all(%d)", status.Agents)
			}
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", job.Name, placed, expectedStr, statusStr)
		}
		w.Flush()
		fmt.Println()
	}

	return nil
}

func runAgents(args []string) error {
	resp, err := doRequest("GET", "/v1/agents", nil)
	if err != nil {
		return err
	}

	var agents []*types.Agent
	if err := json.Unmarshal(resp, &agents); err != nil {
		return err
	}

	if len(args) > 0 {
		agentID := args[0]
		for _, a := range agents {
			if a.ID == agentID {
				return showAgentDetails(a)
			}
		}
		return fmt.Errorf("agent %s not found", agentID)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tENDPOINT\tLAST SEEN")
	for _, agent := range agents {
		fmt.Fprintf(w, "%s\t%s\t%s\n", agent.ID, agent.Endpoint, agent.LastSeen.Format("15:04:05"))
	}
	w.Flush()
	return nil
}

func showAgentDetails(agent *types.Agent) error {
	fmt.Printf("Agent:    %s\n", agent.ID)
	fmt.Printf("Endpoint: %s\n", agent.Endpoint)
	fmt.Printf("LastSeen: %s\n", agent.LastSeen.Format("15:04:05"))
	fmt.Println()

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

func runLogs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	stream := fs.String("stream", "stdout", "Log stream (stdout or stderr)")
	fs.Parse(args)

	taskID := fs.Arg(0)
	if taskID == "" {
		return fmt.Errorf("task ID required")
	}

	if *stream != "stdout" && *stream != "stderr" {
		return fmt.Errorf("stream must be stdout or stderr")
	}

	// Find which agent has this task via cluster status
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

	var agentEndpoint string
	for agent, tasks := range status.TasksByAgent {
		for _, task := range tasks {
			if task.ID == taskID {
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

	url := fmt.Sprintf("http://%s/logs/%s/%s", agentEndpoint, taskID, *stream)
	resp2, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to connect to agent: %w", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		return fmt.Errorf("agent returned status %d", resp2.StatusCode)
	}

	scanner := bufio.NewScanner(resp2.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			fmt.Print(strings.TrimPrefix(line, "data: "))
		}
	}
	return scanner.Err()
}

// parseKV parses "k=v,k2=v2" into a map
func parseKV(s string) map[string]string {
	m := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(pair, "="); ok {
			m[k] = v
		}
	}
	return m
}

func doRequest(method, path string, body any) ([]byte, error) {
	url := fmt.Sprintf("http://%s%s", leaderAddr, path)

	var reqBody io.Reader
	var bodyBytes []byte
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyBytes = data
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	httputil.SignRequest(req, apiKey, bodyBytes)

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
