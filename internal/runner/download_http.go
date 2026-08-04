package runner

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/xinix00/hop/internal/types"
)

const downloadTimeout = 5 * time.Minute

// downloadHTTP downloads from HTTP/HTTPS URL
func downloadHTTP(artifact *types.Artifact, destPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact.URL, nil)
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

	client := &http.Client{}
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
