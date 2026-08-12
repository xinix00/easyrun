//go:build !tamago

// De docker-kant van de meting, plus de vraag of er hier een docker ís. Apart
// omdat het de énige plek in dit pakket is die os/exec nodig heeft, en HopOS'
// kern importeert dit pakket: een bare-metal image hoort geen procesuitvoerder
// mee te linken voor een daemon die daar niet bestaat.
//
// De tamago-kant staat in docker_tamago.go en zegt eerlijk "geen docker".

package agent

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// dockerPresent zegt of er een docker-binary op het pad staat. Het antwoord
// gaat als node-attribuut `node.docker` naar de leider, die er jobs op plaatst.
func dockerPresent() bool {
	_, err := exec.LookPath("docker")
	return err == nil
}

// getDockerUsage returns CPU% and Mem% of total host for a docker container.
// Uses docker stats with percentage format to avoid parsing human-readable byte strings.
func getDockerUsage(taskID string) (cpuPercent float64, memPercent float64, err error) {
	containerName := "hop-" + taskID
	out, err := exec.Command("docker", "stats", "--no-stream", "--format",
		"{{.CPUPerc}} {{.MemPerc}}", containerName).Output()
	if err != nil {
		return 0, 0, err
	}

	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected docker stats output: %q", string(out))
	}

	// Parse CPU% (e.g. "142.50%")
	cpuPercent, err = strconv.ParseFloat(strings.TrimSuffix(fields[0], "%"), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse cpu %q: %w", fields[0], err)
	}

	// Parse Mem% (e.g. "12.50%")
	memPercent, err = strconv.ParseFloat(strings.TrimSuffix(fields[1], "%"), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse mem %q: %w", fields[1], err)
	}

	return cpuPercent, memPercent, nil
}
