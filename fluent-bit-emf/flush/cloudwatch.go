package flush

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

const (
	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	// stolen from https://github.com/aws/amazon-cloudwatch-logs-for-fluent-bit/blob/mainline/cloudwatch/cloudwatch.go
	perEventBytes          = 26
	maximumBytesPerPut     = 1048576
	maximumLogEventsPerPut = 10000
	maximumBytesPerEvent   = 1024 * 256 //256KB
	maximumTimeSpanPerPut  = time.Hour * 24
	truncatedSuffix        = "[Truncated...]"
	maxGroupStreamLength   = 512
)

type cloudwatchFlusher struct {
	cloudwatch_client          *cloudwatchlogs.Client
	cloudwatch_log_group_name  string
	cloudwatch_log_stream_name string
}

func init_cloudwatch_flush(groupName string, streamName string, endpoint string, protocol string) (*cloudwatchFlusher, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load default config: %v", err)
	}
	if endpoint != "" {
		destination := "https://"
		if protocol != "" {
			destination = protocol + "://"
		}
		destination += endpoint
		cfg.BaseEndpoint = &destination
	}
	flusher := &cloudwatchFlusher{}
	flusher.cloudwatch_client = cloudwatchlogs.NewFromConfig(cfg)
	_, err = flusher.cloudwatch_client.CreateLogStream(context.Background(), &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &groupName,
		LogStreamName: &streamName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create log stream: %v", err)
	}
	flusher.cloudwatch_log_group_name = groupName
	flusher.cloudwatch_log_stream_name = streamName
	return flusher, nil
}

func (f *cloudwatchFlusher) Flush(events []common.EMFEvent) error {
	// Create batches that respect CloudWatch Logs limits
	currentBatch := make([]types.InputLogEvent, 0, maximumLogEventsPerPut)
	currentBatchSize := 0

	for _, event := range events {
		marshalled, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %v", err)
		}

		data := string(marshalled)

		if (len(data) + perEventBytes) > maximumBytesPerEvent {
			log.Warn().Printf("dropping event that is too large to send, was %d\n", len(data))
			continue
		}

		// If adding this event would exceed batch size, flush current batch
		if (currentBatchSize+len(data)+perEventBytes) > maximumBytesPerPut || len(currentBatch) == maximumLogEventsPerPut {
			err := f.send_cloudwatch_batch(currentBatch)
			if err != nil {
				return err
			}

			// Reset batch
			currentBatch = make([]types.InputLogEvent, 0, maximumLogEventsPerPut)
			currentBatchSize = 0
		}

		timestamp := time.Now().UnixMilli()
		// Add event to current batch
		currentBatch = append(currentBatch, types.InputLogEvent{
			Timestamp: &timestamp,
			Message:   &data,
		})
		currentBatchSize += len(data) + perEventBytes
	}

	// Send final batch if not empty
	if len(currentBatch) > 0 {
		err := f.send_cloudwatch_batch(currentBatch)
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper function to send a batch of events
func (f *cloudwatchFlusher) send_cloudwatch_batch(batch []types.InputLogEvent) error {
	if len(batch) == 0 {
		return nil
	}

	_, err := f.cloudwatch_client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  &f.cloudwatch_log_group_name,
		LogStreamName: &f.cloudwatch_log_stream_name,
		LogEvents:     batch,
	})

	if err != nil {
		return fmt.Errorf("failed to put log events: %v", err)
	}

	return nil
}
