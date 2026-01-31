package runner

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"easyrun/internal/types"
)

// downloadArtifact downloads and extracts an artifact to destDir
func downloadArtifact(artifact *types.Artifact, destDir string) error {
	if artifact == nil || artifact.URL == "" {
		return nil // No artifact to download
	}

	// Parse URL scheme
	u, err := url.Parse(artifact.URL)
	if err != nil {
		return fmt.Errorf("invalid artifact URL: %w", err)
	}

	// Download to temp file
	tempFile := filepath.Join(destDir, ".download.tar.gz")
	defer os.Remove(tempFile)

	// Route to appropriate downloader based on scheme
	switch u.Scheme {
	case "http", "https":
		if err := downloadHTTP(artifact, tempFile); err != nil {
			return fmt.Errorf("HTTP download failed: %w", err)
		}
	case "s3":
		if err := downloadS3(artifact, tempFile); err != nil {
			return fmt.Errorf("S3 download failed: %w", err)
		}
	default:
		return fmt.Errorf("unsupported URL scheme: %s (use http://, https://, or s3://)", u.Scheme)
	}

	// Extract tar.gz to destDir
	if err := extractTarGz(tempFile, destDir); err != nil {
		return fmt.Errorf("extract failed: %w", err)
	}

	return nil
}

// extractTarGz extracts a .tar.gz file to destDir
func extractTarGz(tarGzPath, destDir string) error {
	file, err := os.Open(tarGzPath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)

		// Security: prevent path traversal
		if !filepath.HasPrefix(target, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in tar: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			dir := filepath.Dir(target)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}

			f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}

	return nil
}
