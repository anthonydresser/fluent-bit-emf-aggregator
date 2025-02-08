package emf

/*
#include <stdlib.h>
#include <stdint.h>
#include "fluent-bit/flb_plugin.h"
*/
import (
	"C"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/options"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/fluent/fluent-bit-go/output"
)

type Stats struct {
	InputLength  int64
	InputRecords int64
}

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	AggregationPeriod time.Duration
	// Map of dimension hash -> metric name -> aggregated values
	metrics map[string]map[string]*AggregatedValue
	// Store metadata and metric definitions
	metadataStore   map[string]map[string]interface{}
	definitionStore map[string]MetricDefinition
	stats           Stats

	// flushing helpers
	flusher func([]map[string]interface{}) (int64, int64, error)
	// file flushing
	file_encoder *json.Encoder
	file         *os.File
	// cloudwatch flushing
	cloudwatch_client          *cloudwatchlogs.Client
	cloudwatch_log_group_name  string
	cloudwatch_log_stream_name string
	Task                       *ScheduledTask
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

	aggregator.Task = NewScheduledTask(options.AggregationPeriod, aggregator.flush)

	return aggregator, nil
}

func (a *AggregatedValue) merge(metric MetricValue) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().Printf("Recovered in merge %v: %v\n%v", r, a, metric)
		}
	}()
	if a.Value == nil && a.Values == nil {
		// this is easy, just initialize ourselves with the metric
		a.Count = metric.Count
		a.Counts = metric.Counts
		a.Max = metric.Max
		a.Min = metric.Min
		a.Sum = metric.Sum
		a.Value = metric.Value
		a.Values = metric.Values
		return
	}
	if a.Value != nil {
		// the simpliest thing we can do is initialize a full object with ourselves then call merge again to do a full merge
		a.Count = 1
		a.Counts = []int64{1}
		a.Max = *a.Value
		a.Min = *a.Value
		a.Sum = *a.Value
		// this has to happen first so we destroy our reference AFTER we initialize
		a.Values = []float64{*a.Value}
		a.Value = nil
		a.merge(metric)
		return
	}
	// at this point we know we are a fulling initialized object
	if metric.Value != nil {
		// the easiest thing to do here would be to create a new full metricValue object and call merge with that
		metric.Count = 1
		metric.Counts = []int64{1}
		metric.Max = *metric.Value
		metric.Min = *metric.Value
		metric.Sum = *metric.Value
		// this has to happen first so we destroy our reference AFTER we initialize
		metric.Values = []float64{*metric.Value}
		metric.Value = nil
		a.merge(metric)
		return
	}
	// at this point we know we are trying to merge 2 full objects
	// lets just make the assumption that our aggregator value has more entries, this will be the case 99 percent of the time
	for index, value := range metric.Values {
		// check if we have this value already
		existingIndex := -1
		for i, testVal := range a.Values {
			if value == testVal {
				existingIndex = i
				break
			}
		}
		if existingIndex != -1 {
			// we have already seen this value, all we need to do is increment
			a.Counts[existingIndex] += metric.Counts[index]
			// we can also do some short circuiting since we know it won't cause a new max or min
		} else {
			// this is new need to add it
			a.Values = append(a.Values, value)
			a.Counts = append(a.Counts, metric.Counts[index])
			a.Min = min(a.Min, value)
			a.Max = max(a.Max, value)
		}
		a.Count++
		a.Sum += value
	}
}

// this is a helper function of sets to ensure we are locking appropriately
func (a *EMFAggregator) Aggregate(data unsafe.Pointer, length int) {
	dec := output.NewDecoder(data, length)

	a.mu.Lock()
	defer a.mu.Unlock()

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Create EMF metric directly from record
		emf, err := EmfFromRecord(record)

		if err != nil {
			log.Error().Printf("[ error] [emf-aggregator] failed to process EMF record: %v\n", err)
			continue
		}

		// Aggregate the metric
		a.AggregateMetric(emf)
		a.stats.InputRecords++
	}

	a.stats.InputLength += int64(length)
}

func (a *EMFAggregator) AggregateMetric(emf *EMFMetric) {

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

		metric.merge(value)
	}
}

func (a *EMFAggregator) flush() error {
	log.Info().Println("Flushing")
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.metrics) == 0 {
		log.Info().Println("No metrics to flush, skipping")
		return nil
	}

	outputEvents := make([]map[string]interface{}, 0)

	for dimHash, metricMap := range a.metrics {
		// Get the metadata for this dimension set
		metadata, exists := a.metadataStore[dimHash]
		if !exists {
			log.Warn().Printf("No metadata found for dimension hash %s\n", dimHash)
			continue
		}

		// Convert AWS metadata to proper types
		awsMetadata, hasAWS := metadata["_aws"].(*AWSMetadata)
		// Skip if no AWS metadata is available
		if !hasAWS {
			log.Warn().Printf("No AWS metadata found for dimension hash %s\n", dimHash)
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

	if len(outputEvents) == 0 {
		log.Warn().Println("No events to flush, skipping")
		return nil
	}

	size, count, err := a.flusher(outputEvents)

	if err != nil {
		return fmt.Errorf("error flushing: %w", err)
	}

	size_percentage := int(float64(a.stats.InputLength-size) / float64(a.stats.InputLength) * 100)
	count_percentage := int(float64(a.stats.InputRecords-count) / float64(a.stats.InputRecords) * 100)

	log.Info().Printf("Compressed %d bytes into %d bytes or %d%%; and %d Records into %d or %d%%\n", a.stats.InputLength, size, size_percentage, a.stats.InputRecords, count, count_percentage)

	// Reset metrics after successful flush
	a.metrics = make(map[string]map[string]*AggregatedValue)
	a.metadataStore = make(map[string]map[string]interface{})
	a.stats.InputLength = 0
	a.stats.InputRecords = 0

	log.Info().Println("Completed Flushing")
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
