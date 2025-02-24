package common

import "time"

type PluginOptions struct {
	OutputPath         string
	AggregationPeriod  time.Duration
	LogGroupName       string
	LogStreamName      string
	CloudWatchEndpoint string
	Protocol           string
}
