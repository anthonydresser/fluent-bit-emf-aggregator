package emf

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func (a *EMFAggregator) init_cloudwatch_flush(groupName string, streamName string, endpoint string, protocol string) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load default config: %v", err)
	}
	if endpoint != "" {
		destination := "https://"
		if protocol != "" {
			destination = protocol + "://"
		}
		destination += endpoint
		cfg.BaseEndpoint = &destination
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
