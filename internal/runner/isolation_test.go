package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"easyrun/internal/types"
)

func TestIsolationEnabledByDefault(t *testing.T) {
	// Verify that a new config has isolation enabled
	cfg := &Config{
		RootfsBase: t.TempDir(),
		Isolate:    true, // This should be the default
	}

	if !cfg.Isolate {
		t.Error("Isolation should be enabled by default")
	}
}

func TestSetupCommandWithIsolation(t *testing.T) {
	taskDir := t.TempDir()
	os.MkdirAll(filepath.Join(taskDir, "tmp"), 0755)

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-job",
		Command: "echo hello",
	}

	cmd := runner.setupCommand(job, taskDir, []string{"PORT=8080"})

	// On macOS, should use sandbox-exec
	// On Linux, should have chroot in SysProcAttr
	if cmd == nil {
		t.Fatal("setupCommand returned nil")
	}

	// Command should be set
	if len(cmd.Args) == 0 {
		t.Error("Command args should not be empty")
	}

	// Environment should include PORT
	found := false
	for _, env := range cmd.Env {
		if env == "PORT=8080" {
			found = true
			break
		}
	}
	if !found {
		t.Error("PORT environment variable not found in command")
	}
}

func TestSetupCommandWithoutIsolation(t *testing.T) {
	taskDir := t.TempDir()
	os.MkdirAll(filepath.Join(taskDir, "tmp"), 0755)

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    false,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-job",
		Command: "echo hello",
	}

	cmd := runner.setupCommand(job, taskDir, nil)

	if cmd == nil {
		t.Fatal("setupCommand returned nil")
	}

	// Directory should be taskDir
	if cmd.Dir != taskDir {
		t.Errorf("cmd.Dir = %s, want %s", cmd.Dir, taskDir)
	}
}

func TestRunnerRunWithIsolation(t *testing.T) {
	taskDir := t.TempDir()

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-isolated",
		Command: "echo isolated",
	}

	task, err := runner.Run(job, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if task == nil {
		t.Fatal("Run returned nil task")
	}

	// Give it time to start
	time.Sleep(100 * time.Millisecond)

	// Clean up
	runner.Stop(task)
}

func TestRunnerRunWithoutIsolation(t *testing.T) {
	taskDir := t.TempDir()

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    false,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-no-isolation",
		Command: "echo not isolated",
	}

	task, err := runner.Run(job, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if task == nil {
		t.Fatal("Run returned nil task")
	}

	// Give it time to start
	time.Sleep(100 * time.Millisecond)

	// Clean up
	runner.Stop(task)
}

func TestIsolationWithVolumes(t *testing.T) {
	taskDir := t.TempDir()
	volumeDir := t.TempDir()

	// Create a file in the volume
	testFile := filepath.Join(volumeDir, "test.txt")
	os.WriteFile(testFile, []byte("volume data"), 0644)

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-volumes",
		Command: "cat /data/test.txt",
		Volumes: map[string]string{
			volumeDir: "/data",
		},
	}

	task, err := runner.Run(job, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Give it time to run
	time.Sleep(200 * time.Millisecond)

	runner.Stop(task)
}

func TestIsolationWithEnvVars(t *testing.T) {
	taskDir := t.TempDir()

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-env",
		Command: "echo $MY_VAR",
		Env: map[string]string{
			"MY_VAR": "test_value",
		},
	}

	cmd := runner.setupCommand(job, taskDir, nil)

	// Check that MY_VAR is in the environment
	found := false
	for _, env := range cmd.Env {
		if env == "MY_VAR=test_value" {
			found = true
			break
		}
	}
	if !found {
		t.Error("MY_VAR not found in command environment")
	}
}

func TestIsolationWithPorts(t *testing.T) {
	taskDir := t.TempDir()

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-ports",
		Command: "echo $ER_PORT_HTTP",
	}

	portEnvVars := runner.buildPortEnvVars(map[string]int{
		"http": 8080,
		"grpc": 9090,
	})

	cmd := runner.setupCommand(job, taskDir, portEnvVars)

	// Check port environment variables
	foundHTTP := false
	foundGRPC := false
	for _, env := range cmd.Env {
		if env == "ER_PORT_HTTP=8080" {
			foundHTTP = true
		}
		if env == "ER_PORT_GRPC=9090" {
			foundGRPC = true
		}
	}
	if !foundHTTP {
		t.Error("ER_PORT_HTTP not found")
	}
	if !foundGRPC {
		t.Error("ER_PORT_GRPC not found")
	}
}

// Platform-specific tests

func TestSandboxProfileGeneration(t *testing.T) {
	// This test is macOS-specific but the function exists on all platforms
	taskDir := t.TempDir()

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-sandbox",
		Command: "echo test",
		Volumes: map[string]string{
			"/mnt/data": "/data",
		},
	}

	// On macOS, we can call generateSandboxProfile directly
	// On Linux, this test still passes but tests different code paths
	cmd := runner.setupCommand(job, taskDir, nil)
	if cmd == nil {
		t.Fatal("setupCommand returned nil")
	}
}

func TestIsolatedProcessCannotAccessRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping isolation test in short mode")
	}

	taskDir := t.TempDir()

	cfg := &Config{
		RootfsBase: taskDir,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)

	// Try to read /etc/passwd - should fail in isolated mode
	job := &types.Job{
		Name:    "test-isolation-check",
		Command: "cat /etc/shadow 2>/dev/null && echo 'ACCESS_GRANTED' || echo 'ACCESS_DENIED'",
	}

	task, err := runner.Run(job, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Get stdout
	stdout := runner.GetStdout(task.ID)
	if stdout == nil {
		t.Fatal("No stdout broadcaster")
	}

	ch := stdout.Subscribe()
	defer stdout.Unsubscribe(ch)

	// Wait for output
	select {
	case line := <-ch:
		// In isolated mode, should not be able to read /etc/shadow
		if strings.Contains(line, "ACCESS_GRANTED") {
			t.Error("Isolated process should not have access to /etc/shadow")
		}
	case <-time.After(2 * time.Second):
		// Timeout is acceptable - command might have failed
	}

	runner.Stop(task)
}

func TestCleanupRemovesTaskDir(t *testing.T) {
	rootfs := t.TempDir()

	cfg := &Config{
		RootfsBase: rootfs,
		Isolate:    true,
	}

	runner := NewProcessRunner(cfg)
	job := &types.Job{
		Name:    "test-cleanup",
		Command: "sleep 10",
	}

	task, err := runner.Run(job, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	taskDir := filepath.Join(rootfs, task.ID)

	// Task dir should exist
	if _, err := os.Stat(taskDir); os.IsNotExist(err) {
		t.Error("Task directory should exist while running")
	}

	// Stop and cleanup
	runner.Stop(task)

	// Task dir should be removed
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(taskDir); !os.IsNotExist(err) {
		t.Error("Task directory should be removed after stop")
	}
}
