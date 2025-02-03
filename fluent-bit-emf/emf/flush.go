package emf

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
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
	// Create file1
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

func (a *EMFAggregator) init_cloudwatch_flush(groupName string, streamName string, endpoint *string) error {
	a.cloudwatch_client = cloudwatchlogs.New(cloudwatchlogs.Options{
		BaseEndpoint: endpoint,
	})
	_, error := a.cloudwatch_client.CreateLogStream(context.Background(), &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &groupName,
		LogStreamName: &streamName,
	})
	if error != nil {
		return fmt.Errorf("failed to create log stream: %v", error)
	}
	a.flusher = a.flush_cloudwatch
	return nil
}

func (a *EMFAggregator) flush_cloudwatch(data map[string]interface{}) (int64, error) {
	timestamp := time.Now().UnixMilli()
	message, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal data: %v", err)
	}
	messageStr := string(message)
	_, err = a.cloudwatch_client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  &a.cloudwatch_log_group_name,
		LogStreamName: &a.cloudwatch_log_stream_name,
		LogEvents: []types.InputLogEvent{
			{
				Timestamp: &timestamp,
				Message:   &messageStr,
			},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to put log events: %v", err)
	}
	return int64(len(message)), nil
}
