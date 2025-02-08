package emf

import (
	"encoding/json"
	"fmt"
	"os"
)

func (a *EMFAggregator) init_file_flush(outputPath string) error {
	// Create file1
	var file *os.File
	if _, err := os.Stat(outputPath); err != nil {
		file, err = os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %v", outputPath, err)
		}
	} else {
		file, err = os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", outputPath, err)
		}
	}

	// Create encoder with indentation for readability
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "")
	a.file_encoder = encoder
	a.file = file
	a.flusher = a.flush_file
	return nil
}

func (a *EMFAggregator) flush_file(events []EMFEvent) (int, int, error) {
	// Encode the map
	size_prior, err := a.file.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to stat file %s: %v", a.file.Name(), err)
	}
	// we have to encode these one at a time so they are individual events rather than a json array
	count := 0
	for _, event := range events {
		if err := a.file_encoder.Encode(event); err != nil {
			return 0, 0, fmt.Errorf("failed to write to file %s: %v", a.file.Name(), err)
		}
		count++
	}
	err = a.file.Sync()

	if err != nil {
		return 0, 0, fmt.Errorf("failed to sync file %s: %v", a.file.Name(), err)
	}

	size_after, err := a.file.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to stat file %s: %v", a.file.Name(), err)
	}

	return int(size_after.Size() - size_prior.Size()), count, nil
}
