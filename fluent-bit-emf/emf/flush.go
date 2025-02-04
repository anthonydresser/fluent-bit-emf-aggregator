package emf

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
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

func (a *EMFAggregator) flush_file(events []map[string]interface{}) (int64, int64, error) {
	// Encode the map
	size_prior, err := a.file.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to stat file %s: %v", a.file.Name(), err)
	}
	// we have to encode these one at a time so they are individual events rather than a json array
	count := int64(0)
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

	return size_after.Size() - size_prior.Size(), count, nil
}

func (a *EMFAggregator) init_cloudwatch_flush(groupName string, streamName string, endpoint *string) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load default config: %v", err)
	}
	if endpoint != nil {
		cfg.BaseEndpoint = endpoint
	}
	a.cloudwatch_client = cloudwatchlogs.NewFromConfig(cfg)
	_, err = a.cloudwatch_client.CreateLogStream(context.Background(), &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &groupName,
		LogStreamName: &streamName,
	})
	if err != nil {
		return fmt.Errorf("failed to create log stream: %v", err)
	}
	a.cloudwatch_log_group_name = groupName
	a.cloudwatch_log_stream_name = streamName
	a.flusher = a.flush_cloudwatch
	return nil
}

func (a *EMFAggregator) flush_cloudwatch(events []map[string]interface{}) (int64, int64, error) {
	timestamp := time.Now().UnixMilli()
	messages := make([]types.InputLogEvent, 0)
	count := int64(0)
	for _, event := range events {
		if event == nil {
			continue
		}
		marshalled, err := json.Marshal(event)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to marshal event: %v", err)
		}
		marshalledString := string(marshalled)
		messages = append(messages, types.InputLogEvent{
			Timestamp: &timestamp,
			Message:   &marshalledString,
		})
		count++
	}
	_, err := a.cloudwatch_client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  &a.cloudwatch_log_group_name,
		LogStreamName: &a.cloudwatch_log_stream_name,
		LogEvents:     messages,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to put log events: %v", err)
	}
	size := int64(0)
	for _, message := range messages {
		size += int64(len(*message.Message))
	}
	return size, count, nil
}
