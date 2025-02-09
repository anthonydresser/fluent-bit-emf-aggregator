package emf

/*
#include <stdlib.h>
#include <stdint.h>
#include "fluent-bit/flb_plugin.h"
*/
import (
	"C"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/histogram"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/fluent/fluent-bit-go/output"
)
import "github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/flush"

type InputStats struct {
	InputLength  int
	InputRecords int
}

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	aggregationPeriod time.Duration
	// Map of dimension hash -> metric name -> aggregated values
	metrics map[string]map[string]*histogram.Histogram
	// Store metadata and metric definitions
	metadataStore map[string]Metadata
	stats         InputStats

	// flushing helpers
	flusher flush.Flusher
	Task    *ScheduledTask
}

type Metadata struct {
	AWS        *common.AWSMetadata
	Dimensions map[string]string
}

func NewEMFAggregator(options *common.PluginOptions) (*EMFAggregator, error) {
	aggregator := &EMFAggregator{
		aggregationPeriod: options.AggregationPeriod,
		metrics:           make(map[string]map[string]*histogram.Histogram),
		metadataStore:     make(map[string]Metadata),
	}

	var err error

	if aggregator.flusher, err = flush.InitFlusher(options); err != nil {
		return nil, err
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
			metadata.AWS.Merge(emf.AWS)
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
		} else if value.Values == nil {
			if value.Max != nil && value.Min == value.Max {
				metric.Add(*value.Max, *value.Count)
			} else {
				log.Warn().Printf("Invalid metric value found for metric %s: %v\n", name, value)
				continue
			}
		} else {
			for index, v := range value.Values {
				metric.Add(v, value.Counts[index])
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

	outputEvents := make([]common.EMFEvent, 0, len(a.metrics))

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
		outputMap := common.EMFEvent{
			AWS:         metadata.AWS,
			OtherFields: make(map[string]interface{}),
		}

		// Add all metric values
		for name, value := range metricMap {
			stats := value.Reduce()
			if stats == nil {
				log.Warn().Printf("No stats found for metric %s\n", name)
				continue
			}
			if len(stats.Values) == 1 {
				// Single value
				outputMap.OtherFields[name] = stats.Max
			} else {
				outputMap.OtherFields[name] = stats
			}
		}

		for key, value := range metadata.Dimensions {
			outputMap.OtherFields[key] = value
		}

		outputEvents = append(outputEvents, outputMap)
	}

	if len(outputEvents) == 0 {
		log.Warn().Println("No events to flush, skipping")
		return nil
	}

	size, count, err := a.flusher.Flush(outputEvents)

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
