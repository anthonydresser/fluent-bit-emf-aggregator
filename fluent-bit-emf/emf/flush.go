package emf

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (a *EMFAggregator) init_file_flush(outputPath string) error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputPath, 0600); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}
	// Create filename with timestamp and dimension hash
	filename := filepath.Join(outputPath,
		fmt.Sprintf("emf_aggregate_%d.json",
			time.Now().Unix()))
	// Create file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", filename, err)
	}

	// Create encoder with indentation for readability
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "")
	a.file_encoder = encoder
	a.file = file
	a.flusher = a.flush_file
	return nil
}

func (a *EMFAggregator) flush_file(data map[string]interface{}) (int64, error) {
	// Encode the map
	size_prior, err := a.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %v", a.file.Name(), err)
	}
	if err := a.file_encoder.Encode(data); err != nil {
		return 0, fmt.Errorf("failed to write to file %s: %v", a.file.Name(), err)
	}
	size_after, err := a.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %v", a.file.Name(), err)
	}

	return size_after.Size() - size_prior.Size(), nil
}

// func flush_cloudwatch(data map[string]interface{}) (int64, error) {

// }
