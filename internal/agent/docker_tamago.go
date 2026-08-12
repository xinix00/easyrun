//go:build tamago

// De tamago-kant: op een HopOS-node bestaat geen docker en geen os/exec. De
// twee functies bestaan zodat de rest van het pakket één vorm heeft op elk
// platform — zie docker_host.go voor de echte implementatie en de reden dat de
// scheiding er is.

package agent

import "errors"

// dockerPresent is op bare metal altijd false: het node-attribuut node.docker
// wordt "false", en de leider plaatst er dus geen docker-jobs op.
func dockerPresent() bool { return false }

// getDockerUsage bestaat niet op bare metal; de monitor logt de fout en houdt
// het bij zijn eigen meting.
func getDockerUsage(string) (float64, float64, error) {
	return 0, 0, errors.New("docker stats are not available on this platform")
}
