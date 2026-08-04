package runner

import (
	"archive/tar"
	"archive/zip"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/xinix00/hop/internal/types"
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

	// Determine if extraction is needed
	shouldExtract := artifact.Extract != ""
	var tempFile string

	if shouldExtract {
		tempFile = filepath.Join(destDir, ".download."+artifact.Extract)
	} else {
		// Raw file: use filename override or basename from URL
		name := artifact.Filename
		if name == "" {
			name = filepath.Base(u.Path)
		}
		tempFile = filepath.Join(destDir, name)
	}

	if shouldExtract {
		defer os.Remove(tempFile)
	}

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

	// Extract if needed
	if shouldExtract {
		switch artifact.Extract {
		case "tar.gz", "tgz":
			if err := extractTarGz(tempFile, destDir); err != nil {
				return fmt.Errorf("extract tar.gz failed: %w", err)
			}
		case "tar.bz2", "tbz2":
			if err := extractTarBz2(tempFile, destDir); err != nil {
				return fmt.Errorf("extract tar.bz2 failed: %w", err)
			}
		case "zip":
			if err := extractZip(tempFile, destDir); err != nil {
				return fmt.Errorf("extract zip failed: %w", err)
			}
		default:
			return fmt.Errorf("unsupported extract type: %s (use tar.gz, tar.bz2, or zip)", artifact.Extract)
		}
	} else {
		// Raw file: make executable
		if err := os.Chmod(tempFile, 0755); err != nil {
			return fmt.Errorf("chmod failed: %w", err)
		}
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

	return extractTar(gzr, destDir)
}

// extractTarBz2 extracts a .tar.bz2 file to destDir
func extractTarBz2(tarBz2Path, destDir string) error {
	file, err := os.Open(tarBz2Path)
	if err != nil {
		return err
	}
	defer file.Close()

	bzr := bzip2.NewReader(file)
	return extractTar(bzr, destDir)
}

// extractTar extracts a tar archive from reader to destDir
func extractTar(r io.Reader, destDir string) error {
	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Skip root directory entry
		name := filepath.Clean(header.Name)
		if name == "." {
			continue
		}

		target := filepath.Join(destDir, header.Name)

		// Security: prevent path traversal
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)+string(os.PathSeparator)) {
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

// extractZip extracts a .zip file to destDir
func extractZip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		name := filepath.Clean(f.Name)
		if name == "." {
			continue
		}

		target := filepath.Join(destDir, f.Name)

		// Security: prevent path traversal
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in zip: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
			continue
		}

		// Create file
		dir := filepath.Dir(target)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}

		outFile, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		rc.Close()
		outFile.Close()

		if err != nil {
			return err
		}
	}

	return nil
}
