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

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/histogram"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/options"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/fluent/fluent-bit-go/output"
)

type InputStats struct {
	InputLength  int
	InputRecords int
}

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	AggregationPeriod time.Duration
	// Map of dimension hash -> metric name -> aggregated values
	metrics map[string]map[string]*histogram.Histogram
	// Store metadata and metric definitions
	metadataStore map[string]Metadata
	stats         InputStats

	// flushing helpers
	flusher func([]EMFEvent) (int, int, error)
	// file flushing
	file_encoder *json.Encoder
	file         *os.File
	// cloudwatch flushing
	cloudwatch_client          *cloudwatchlogs.Client
	cloudwatch_log_group_name  string
	cloudwatch_log_stream_name string
	Task                       *ScheduledTask
}

type Metadata struct {
	AWS        *AWSMetadata
	Dimensions map[string]string
}

type EMFEvent struct {
	AWS         *AWSMetadata           `json:"_aws"`
	OtherFields map[string]interface{} `json:",inline"`
}

func NewEMFAggregator(options options.PluginOptions) (*EMFAggregator, error) {
	aggregator := &EMFAggregator{
		AggregationPeriod: options.AggregationPeriod,
		metrics:           make(map[string]map[string]*histogram.Histogram),
		metadataStore:     make(map[string]Metadata),
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
			log.Error().Printf("failed to process EMF record: %v\n", err)
			continue
		}

		// Aggregate the metric
		a.AggregateMetric(emf)
		a.stats.InputRecords++
	}

	a.stats.InputLength += length
}

func merge(old []MetricDefinition, new []MetricDefinition) {
	for _, attempt := range new {
		exists := false
		for _, v := range old {
			if v.Name == attempt.Name && v.Unit == attempt.Unit {
				exists = true
				break
			}
		}
		if !exists {
			old = append(old, attempt)
		}
	}

}

// we can only merge if the namespaces match and the dimension sets match
// even if the namespaces match, if the dimensions aren't the same we risk
// emitting metrics under dimensions they weren't intended to be emitted under
func (def *ProjectionDefinition) attemptMerge(new ProjectionDefinition) bool {
	if def.Namespace != new.Namespace {
		return false
	}
	if utils.Every(def.Dimensions, func(val []string) bool {
		return utils.Find(new.Dimensions, func(test []string) bool {
			return strings.Join(val, ", ") == strings.Join(test, ", ")
		}) != -1
	}) {
		merge(def.Metrics, new.Metrics)
		return true
	} else {
		return false
	}
}

func (m *AWSMetadata) merge(new *AWSMetadata) {
	m.Timestamp = new.Timestamp
	for _, attempt := range new.CloudWatchMetrics {
		merged := false
		for _, v := range m.CloudWatchMetrics {
			merged = v.attemptMerge(attempt)
			if merged {
				break
			}
		}
		if !merged {
			m.CloudWatchMetrics = append(m.CloudWatchMetrics, attempt)
		}
	}
}

func (a *EMFAggregator) AggregateMetric(emf *EMFMetric) {
	// Create dimension hash for grouping
	dimHash := createDimensionHash(emf.Dimensions)

	// Initialize or update metadata store
	if metadata, exists := a.metadataStore[dimHash]; !exists {
		a.metadataStore[dimHash] = Metadata{
			AWS:        emf.AWS,
			Dimensions: emf.Dimensions,
		}
	} else {
		// Store AWS metadata
		if metadata.AWS == nil {
			metadata.AWS = emf.AWS
		} else {
			metadata.AWS.merge(emf.AWS)
		}

		// Store extra fields
		for key, value := range emf.Dimensions {
			// Only update if the field doesn't exist or is empty
			if _, exists := metadata.Dimensions[key]; !exists {
				metadata.Dimensions[key] = value
			}
		}
	}

	// Initialize metric map for this dimension set if not exists
	if _, exists := a.metrics[dimHash]; !exists {
		a.metrics[dimHash] = make(map[string]*histogram.Histogram)
	}

	// Aggregate each metric
	for name, value := range emf.MetricData {
		if _, exists := a.metrics[dimHash][name]; !exists {
			a.metrics[dimHash][name] = histogram.NewHistogram()
		}

		metric := a.metrics[dimHash][name]

		if value.Value != nil {
			metric.Add(*value.Value, 1)
		} else {
			for index, v := range value.Values {
				metric.Add(v, uint(value.Counts[index]))
			}
		}
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

	outputEvents := make([]EMFEvent, 0, len(a.metrics))

	for dimHash, metricMap := range a.metrics {
		// Get the metadata for this dimension set
		metadata, exists := a.metadataStore[dimHash]
		if !exists {
			log.Warn().Printf("No metadata found for dimension hash %s\n", dimHash)
			continue
		}

		// Skip if no AWS metadata is available
		if metadata.AWS == nil {
			log.Warn().Printf("No AWS metadata found for dimension hash %s\n", dimHash)
			continue
		}

		// Create output map with string keys
		outputMap := EMFEvent{
			AWS:         metadata.AWS,
			OtherFields: make(map[string]interface{}),
		}

		// Add all metric values
		for name, value := range metricMap {
			stats := value.Reduce()
			if len(stats.Values) == 1 {
				// Single value
				outputMap.OtherFields[name] = stats.Max
			} else {
				outputMap.OtherFields[name] = stats
			}
		}

		// Add all extra fields from metadata
		for key, value := range metadata.Dimensions {
			// Skip special fields we've already handled
			if key != "_aws" {
				// Convert any map[interface{}]interface{} to map[string]interface{}
				outputMap.OtherFields[key] = convertToStringKeyMap(value)
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
	a.metrics = make(map[string]map[string]*histogram.Histogram)
	a.metadataStore = make(map[string]Metadata)
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
