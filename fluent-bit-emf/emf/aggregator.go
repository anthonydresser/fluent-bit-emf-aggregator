package emf

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/options"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

type Stats struct {
	InputLength  int64
	InputRecords int64
}

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	AggregationPeriod time.Duration
	LastFlush         time.Time
	// Map of dimension hash -> metric name -> aggregated values
	metrics map[string]map[string]*AggregatedValue
	// Store metadata and metric definitions
	metadataStore   map[string]map[string]interface{}
	definitionStore map[string]MetricDefinition
	Stats           Stats

	// flushing helpers
	flusher func([]map[string]interface{}) (int64, int64, error)
	// file flushing
	file_encoder *json.Encoder
	file         *os.File
	// cloudwatch flushing
	cloudwatch_client          *cloudwatchlogs.Client
	cloudwatch_log_group_name  string
	cloudwatch_log_stream_name string
}

type AggregatedValue struct {
	// for when only the value is specified; not a struct
	// making this a pointer allows us to distinguish between
	// an unset value and a 0 value
	Value *float64 `json:"Value,omitempty"`
	// otherwise
	Values []float64
	Counts []int64
	Sum    float64
	Count  int64
	Min    float64
	Max    float64
}

func NewEMFAggregator(options options.PluginOptions) (*EMFAggregator, error) {
	aggregator := &EMFAggregator{
		AggregationPeriod: options.AggregationPeriod,
		metrics:           make(map[string]map[string]*AggregatedValue),
		metadataStore:     make(map[string]map[string]interface{}),
		definitionStore:   make(map[string]MetricDefinition),
		LastFlush:         time.Now(),
	}

	if options.OutputPath != "" {
		err := aggregator.init_file_flush(options.OutputPath)
		if err != nil {
			return nil, err
		}
	} else if options.LogGroupName != "" && options.LogStreamName != "" {
		err := aggregator.init_cloudwatch_flush(options.LogGroupName, options.LogStreamName, options.CloudWatchEndpoint, options.Protocol)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("no output configured")
	}

	return aggregator, nil
}

func (a *EMFAggregator) AggregateMetric(emf *EMFMetric) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create dimension hash for grouping
	dimHash := createDimensionHash(emf.Dimensions)

	// Initialize or update metadata store
	if _, exists := a.metadataStore[dimHash]; !exists {
		a.metadataStore[dimHash] = make(map[string]interface{})
	}

	// Store AWS metadata
	if emf.AWS != nil {
		a.metadataStore[dimHash]["_aws"] = emf.AWS
	}

	// Store extra fields
	for key, value := range emf.Dimensions {
		// Only update if the field doesn't exist or is empty
		if _, exists := a.metadataStore[dimHash][key]; !exists {
			a.metadataStore[dimHash][key] = value
		}
	}

	// Initialize metric map for this dimension set if not exists
	if _, exists := a.metrics[dimHash]; !exists {
		a.metrics[dimHash] = make(map[string]*AggregatedValue)
	}

	// Aggregate each metric
	for name, value := range emf.MetricData {
		if _, exists := a.metrics[dimHash][name]; !exists {
			a.metrics[dimHash][name] = &AggregatedValue{}
		}

		metric := a.metrics[dimHash][name]

		// Handle different value types
		if value.Values != nil {
			if metric.Value == nil && metric.Values == nil {
				// first time we are seeing this metric
				metric.Values = value.Values
				metric.Count = value.Count
				metric.Counts = value.Counts
				metric.Sum = value.Sum
				metric.Min = value.Min
				metric.Max = value.Max
			} else if metric.Value != nil {
				// we have seen this metric before, but only the value was set
				// convert to struct and aggregate
				index := -1
				// find the existing value in the new values
				for existingIndex := range value.Values {
					if value.Values[existingIndex] == *metric.Value {
						index = existingIndex
					}
				}
				metric.Values = value.Values
				metric.Counts = value.Counts

				if index != -1 {
					metric.Counts[index]++
				} else {
					metric.Values = append(metric.Values, *metric.Value)
					metric.Counts = append(metric.Counts, 1)
				}
				metric.Sum = *metric.Value + value.Sum
				metric.Count = 1 + value.Count
				metric.Min = min(value.Min, *metric.Value)
				metric.Max = max(value.Max, *metric.Value)
				metric.Value = nil
			} else {
				// we have seen this metric before and it is already in the form of a struct
				for _, v := range value.Values {
					index := -1
					for existingIndex := range metric.Values {
						if metric.Values[existingIndex] == v {
							index = existingIndex
						}
					}
					if index != -1 {
						metric.Counts[index]++
					} else {
						metric.Values = append(metric.Values, v)
						metric.Counts = append(metric.Counts, 1)
					}
					metric.Sum += v
					metric.Count++
					metric.Min = min(metric.Min, v)
					metric.Max = max(metric.Max, v)
					metric.Value = nil
				}
			}
		} else if value.Value != nil {
			v := convertToFloat64(value.Value)
			if metric.Value == nil && metric.Values == nil {
				// we have no seen this value before
				metric.Value = &v
			} else if metric.Value != nil {
				// we have seen this metric before but in the form of a number
				metric.Counts = make([]int64, 0)
				metric.Values = make([]float64, 0)
				if *metric.Value == v {
					metric.Values = append(metric.Values, v)
					metric.Counts = append(metric.Counts, 2)
				} else {
					metric.Values = append(metric.Values, v, *metric.Value)
					metric.Counts = append(metric.Counts, 1, 1)
				}
				metric.Sum = *metric.Value + v
				metric.Min = min(*metric.Value, v)
				metric.Max = max(*metric.Value, v)
				metric.Count = 2
				metric.Value = nil
			} else {
				// we have seen this number before and we have a struct already
				index := -1
				for existingIndex := range metric.Values {
					if metric.Values[existingIndex] == v {
						index = existingIndex
					}
				}
				if index != -1 {
					metric.Counts[index]++
				} else {
					metric.Values = append(metric.Values, v)
					metric.Counts = append(metric.Counts, 1)
				}
				metric.Sum += v
				metric.Count++
				metric.Min = min(metric.Min, v)
				metric.Max = max(metric.Max, v)
			}
		}
	}
}

func (a *EMFAggregator) Flush() error {
	log.Println("[ info] [emf-aggregator] Flushing")
	a.mu.Lock()
	defer a.mu.Unlock()

	outputEvents := make([]map[string]interface{}, 0)

	for dimHash, metricMap := range a.metrics {
		// Get the metadata for this dimension set
		metadata, exists := a.metadataStore[dimHash]
		if !exists {
			log.Printf("[ warn] [emf-aggregator] No metadata found for dimension hash %s\n", dimHash)
			continue
		}

		// Convert AWS metadata to proper types
		awsMetadata, hasAWS := metadata["_aws"].(*AWSMetadata)
		// Skip if no AWS metadata is available
		if !hasAWS {
			log.Printf("[ warn] [emf-aggregator] No AWS metadata found for dimension hash %s\n", dimHash)
			continue
		}

		// Create output map with string keys
		outputMap := make(map[string]interface{})

		// Add AWS metadata
		outputMap["_aws"] = awsMetadata

		// Add all metric values
		for name, value := range metricMap {
			if value.Value != nil {
				// Single value
				outputMap[name] = *value.Value
			} else if value.Values != nil {
				// Array of values
				outputMap[name] = value
			}
		}

		// Add all extra fields from metadata
		for key, value := range metadata {
			// Skip special fields we've already handled
			if key != "_aws" {
				// Convert any map[interface{}]interface{} to map[string]interface{}
				outputMap[key] = convertToStringKeyMap(value)
			}
		}

		outputEvents = append(outputEvents, outputMap)
	}

	size, count, err := a.flusher(outputEvents)

	if err != nil {
		return fmt.Errorf("error flushing: %w", err)
	}

	size_percentage := int(float64(a.Stats.InputLength-size) / float64(a.Stats.InputLength) * 100)
	count_percentage := int(float64(a.Stats.InputRecords-count) / float64(a.Stats.InputRecords) * 100)

	log.Printf("[ info] [emf-aggregator] Compressed %d bytes into %d bytes or %d%%; and %d Records into %d or %d%%\n", a.Stats.InputLength, size, size_percentage, a.Stats.InputRecords, count, count_percentage)

	// Reset metrics after successful flush
	a.metrics = make(map[string]map[string]*AggregatedValue)
	a.metadataStore = make(map[string]map[string]interface{})
	a.LastFlush = time.Now()
	a.Stats.InputLength = 0
	a.Stats.InputRecords = 0

	log.Println("[ info] [emf-aggregator] Completed Flushing")
	return nil
}

// Helper function to convert interface{} maps to string key maps
func convertToStringKeyMap(v interface{}) interface{} {
	switch v := v.(type) {
	case map[interface{}]interface{}:
		strMap := make(map[string]interface{})
		for key, value := range v {
			strMap[fmt.Sprintf("%v", key)] = convertToStringKeyMap(value)
		}
		return strMap
	case []interface{}:
		for i, val := range v {
			v[i] = convertToStringKeyMap(val)
		}
		return v
	case map[string]interface{}:
		for key, value := range v {
			v[key] = convertToStringKeyMap(value)
		}
		return v
	default:
		return v
	}
}

// Helper functions
func createDimensionHash(dimensions map[string]string) string {
	// Create a slice to hold the sorted key-value pairs
	pairs := make([]string, 0, len(dimensions))

	// Convert map entries to sorted slice
	for k, v := range dimensions {
		pairs = append(pairs, k+"="+v)
	}

	// Sort the pairs to ensure consistent ordering
	sort.Strings(pairs)

	// Join all pairs with a delimiter
	return strings.Join(pairs, ";")
}
