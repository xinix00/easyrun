package runner

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"easyrun/internal/types"
)

// downloadHTTP downloads from HTTP/HTTPS URL
func downloadHTTP(artifact *types.Artifact, destPath string) error {
	req, err := http.NewRequest("GET", artifact.URL, nil)
	if err != nil {
		return err
	}

	// Add custom headers
	for k, v := range artifact.Headers {
		req.Header.Set(k, v)
	}

	// Helper: Basic Auth from username/password
	if artifact.Auth["username"] != "" && artifact.Auth["password"] != "" {
		auth := base64.StdEncoding.EncodeToString(
			[]byte(artifact.Auth["username"] + ":" + artifact.Auth["password"]),
		)
		req.Header.Set("Authorization", "Basic "+auth)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
