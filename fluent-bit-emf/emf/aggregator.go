package emf

/*
#include <stdlib.h>
#include <stdint.h>
#include "fluent-bit/flb_plugin.h"
*/
import (
	"C"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
	"unsafe"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/flush"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/metricaggregator"
	"github.com/fluent/fluent-bit-go/output"
	"golang.org/x/exp/maps"
)

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	aggregationPeriod time.Duration
	// Map of dimension hash -> metric name -> aggregated values
	metrics map[uint64]map[string]metricaggregator.MetricAggregator
	// Store metadata and metric definitions
	metadataStore map[uint64]*Metadata

	// flushing helpers
	flusher flush.Flusher
	Task    *ScheduledTask
}

var (
	metadataPool = sync.Pool{
		New: func() any {
			return &Metadata{
				AWS:        &common.AWSMetadata{},
				Dimensions: make(map[string]string),
			}
		},
	}
)

func (m *Metadata) Close() {
	m.AWS.Close()
	maps.Clear(m.Dimensions)
	metadataPool.Put(m)
}

func getMetadataStruct() *Metadata {
	return metadataPool.Get().(*Metadata)
}

type Metadata struct {
	AWS        *common.AWSMetadata
	Dimensions map[string]string
}

func NewEMFAggregator(options *common.PluginOptions, flusher flush.Flusher) (*EMFAggregator, error) {
	aggregator := &EMFAggregator{
		aggregationPeriod: options.AggregationPeriod,
		metrics:           make(map[uint64]map[string]metricaggregator.MetricAggregator),
		metadataStore:     make(map[uint64]*Metadata),
		flusher:           flusher,
	}

	aggregator.Task = NewScheduledTask(options.AggregationPeriod, aggregator.flush)

	return aggregator, nil
}

// this is a helper function of sets to ensure we are locking appropriately
func (a *EMFAggregator) Aggregate(data unsafe.Pointer, length int) {
	dec := output.NewDecoder(data, length)

	// Pre-allocate a slice if you know approximate size
	records := make([]*common.EMFReport, 0, 60) // adjust capacity based on typical usage

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		emf := common.GetEmfStruct()

		// Create EMF metric directly from record
		err := FillEmfFromRecord(record, emf)

		records = append(records, emf)

		if err != nil {
			log.Error().Printf("failed to process EMF record: %v\n", err)
			emf.Close()
			continue
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, record := range records {
		// Aggregate the metric
		a.AggregateMetric(record)
		record.Close()
	}
}

func (a *EMFAggregator) AggregateMetric(emf *common.EMFReport) {
	// Create dimension hash for grouping
	hash := generateHash(emf.Dimensions)

	// Initialize or update metadata store
	if metadata, exists := a.metadataStore[hash]; !exists {
		metadata = getMetadataStruct()
		metadata.AWS.FillFrom(emf.AWS)
		for key, value := range emf.Dimensions {
			metadata.Dimensions[key] = value
		}
		a.metadataStore[hash] = metadata
	} else {
		metadata.AWS.Merge(emf.AWS)
	}

	// Initialize metric map for this dimension set if not exists
	if _, exists := a.metrics[hash]; !exists {
		a.metrics[hash] = make(map[string]metricaggregator.MetricAggregator)
	}

	// Aggregate each metric
	for name, value := range emf.MetricData {
		if _, exists := a.metrics[hash][name]; !exists {
			aggregator, err := metricaggregator.InitMetricAggregator(value)
			if err != nil {
				log.Error().Printf("failed to initialize metric aggregator: %v\n", err)
				continue
			}
			a.metrics[hash][name] = aggregator
		}

		err := a.metrics[hash][name].Add(value)
		if err != nil {
			log.Error().Printf("failed to add metric %s sample: %v\n", name, err)
		}
	}
}

func (a *EMFAggregator) flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	log.Info().Println("Flushing")

	if len(a.metrics) == 0 {
		log.Info().Println("No metrics to flush, skipping")
		return nil
	}

	outputEvents := make([]*common.EMFEvent, 0, len(a.metrics))

	for dimHash, metricMap := range a.metrics {
		// Get the metadata for this dimension set
		metadata, exists := a.metadataStore[dimHash]
		if !exists {
			log.Warn().Println("No metadata found for hash")
			continue
		}

		outputMap := common.GetEmfEventStruct()
		outputMap.AWS.FillFrom(metadata.AWS)

		// Add all metric values
		for name, value := range metricMap {
			stats := value.Reduce()
			if stats == nil {
				log.Warn().Printf("No stats found for metric %s\n", name)
				continue
			}
			if stats.Count == 1 {
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

	if err := a.flusher.Flush(outputEvents); err != nil {
		return fmt.Errorf("error flushing: %w", err)
	}

	for _, val := range outputEvents {
		val.Close()
	}

	maps.Clear(a.metrics)
	for _, val := range a.metadataStore {
		val.Close()
	}
	maps.Clear(a.metadataStore)

	log.Info().Println("Completed Flushing")
	return nil
}

// Using FNV hash - fastest approach
func generateHash(dimensions map[string]string) uint64 {
	// Create hash
	h := fnv.New64a()

	// Write sorted keys and values
	for key, value := range dimensions {
		h.Write([]byte(key))
		h.Write([]byte(value))
	}

	return h.Sum64()
}
