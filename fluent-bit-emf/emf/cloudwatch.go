package emf

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
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
	var totalSize int64 = 0
	var totalCount int64 = 0

	// Create batches that respect CloudWatch Logs limits
	var currentBatch []types.InputLogEvent
	var currentBatchSize int64 = 0
	const maxBatchSize int64 = 1024 * 1024 // 1MB in bytes
	const maxEventSize int64 = 256 * 1024  // 256KB in bytes

	for _, event := range events {
		if event == nil {
			continue
		}

		marshalled, err := json.Marshal(event)
		if err != nil {
			return totalSize, totalCount, fmt.Errorf("failed to marshal event: %v", err)
		}

		marshalledString := string(marshalled)
		eventSize := int64(len(marshalled))

		if eventSize > maxEventSize {
			log.Warn().Printf("dropping event that is too large to send, was %d", eventSize)
			continue
		}

		// If adding this event would exceed batch size, flush current batch
		if currentBatchSize+eventSize > maxBatchSize {
			size, err := a.send_cloudwatch_batch(currentBatch)
			if err != nil {
				return totalSize, totalCount, err
			}
			totalSize += size
			totalCount += int64(len(currentBatch))

			// Reset batch
			currentBatch = make([]types.InputLogEvent, 0)
			currentBatchSize = 0
		}

		// Add event to current batch
		currentBatch = append(currentBatch, types.InputLogEvent{
			Timestamp: &timestamp,
			Message:   &marshalledString,
		})
		currentBatchSize += eventSize
	}

	// Send final batch if not empty
	if len(currentBatch) > 0 {
		size, err := a.send_cloudwatch_batch(currentBatch)
		if err != nil {
			return totalSize, totalCount, err
		}
		totalSize += size
		totalCount += int64(len(currentBatch))
	}

	return totalSize, totalCount, nil
}

// Helper function to send a batch of events
func (a *EMFAggregator) send_cloudwatch_batch(batch []types.InputLogEvent) (int64, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	_, err := a.cloudwatch_client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  &a.cloudwatch_log_group_name,
		LogStreamName: &a.cloudwatch_log_stream_name,
		LogEvents:     batch,
	})

	if err != nil {
		return 0, fmt.Errorf("failed to put log events: %v", err)
	}

	var batchSize int64 = 0
	for _, event := range batch {
		batchSize += int64(len(*event.Message))
	}

	return batchSize, nil
}
