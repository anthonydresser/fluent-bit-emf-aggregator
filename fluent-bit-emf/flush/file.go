package flush

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
)

type fileFlusher struct {
	file         *os.File
	file_encoder *json.Encoder
}

func init_file_flush(outputPath string) (*fileFlusher, error) {
	// Create file1
	var file *os.File
	if _, err := os.Stat(outputPath); err != nil {
		file, err = os.Create(outputPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create file %s: %v", outputPath, err)
		}
	} else {
		file, err = os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %v", outputPath, err)
		}
	}

	flusher := &fileFlusher{}
	// Create encoder with indentation for readability
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "")
	flusher.file_encoder = encoder
	flusher.file = file
	return flusher, nil
}

func (f *fileFlusher) Flush(events []*common.EMFEvent) error {
	for _, event := range events {
		if err := f.file_encoder.Encode(event); err != nil {
			return fmt.Errorf("failed to write to file %s: %v", f.file.Name(), err)
		}
	}
	err := f.file.Sync()

	if err != nil {
		return fmt.Errorf("failed to sync file %s: %v", f.file.Name(), err)
	}

	return nil
}
