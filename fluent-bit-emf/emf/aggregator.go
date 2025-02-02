package emf

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fluent/fluent-bit-go/output"
)

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	outputPath        string
	AggregationPeriod time.Duration
	LastFlush         time.Time
	// Map of dimension hash -> metric name -> aggregated values
	metrics map[string]map[string]*AggregatedValue
	// Store metadata and metric definitions
	metadataStore   map[string]map[string]interface{}
	definitionStore map[string]MetricDefinition
}

type AggregatedValue struct {
	Sum         float64
	Count       int64
	Min         float64
	Max         float64
	Unit        string
	Dimensions  map[string]string
	LastUpdated output.FLBTime
}

func NewEMFAggregator(outputPath string, aggregationPeriod time.Duration) *EMFAggregator {
	return &EMFAggregator{
		outputPath:        outputPath,
		AggregationPeriod: aggregationPeriod,
		metrics:           make(map[string]map[string]*AggregatedValue),
		metadataStore:     make(map[string]map[string]interface{}),
		definitionStore:   make(map[string]MetricDefinition),
	}
}

func (a *EMFAggregator) AggregateMetric(emf *EMFMetric, ts output.FLBTime) {
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

	// Store dimensions
	if len(emf.Dimensions) > 0 {
		a.metadataStore[dimHash]["dimensions"] = emf.Dimensions
	}

	// Store timestamp if present
	if emf.Timestamp != nil {
		a.metadataStore[dimHash]["timestamp"] = emf.Timestamp
	}

	// Store extra fields
	for key, value := range emf.ExtraFields {
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
			a.metrics[dimHash][name] = &AggregatedValue{
				Min:        math.MaxFloat64,
				Max:        -math.MaxFloat64,
				Dimensions: emf.Dimensions,
			}
		}

		metric := a.metrics[dimHash][name]

		// Handle different value types
		if value.Values != nil {
			// Handle array of values
			for i, v := range value.Values {
				count := int64(1)
				if value.Counts != nil && i < len(value.Counts) {
					count = value.Counts[i]
				}
				metric.Sum += v * float64(count)
				metric.Count += int64(count)
				metric.Min = min(metric.Min, v)
				metric.Max = max(metric.Max, v)
			}
		} else if value.Value != nil {
			// Handle single value
			v := convertToFloat64(value.Value)
			metric.Sum += v
			metric.Count++
			metric.Min = min(metric.Min, v)
			metric.Max = max(metric.Max, v)
		}

		metric.LastUpdated = ts
		metric.Unit = value.Unit
	}
}

func (a *EMFAggregator) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(a.outputPath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	for dimHash, metricMap := range a.metrics {
		// Get the metadata for this dimension set
		metadata, exists := a.metadataStore[dimHash]
		if !exists {
			continue
		}

		// Convert AWS metadata to proper types
		awsMetadata, hasAWS := metadata["_aws"].(*AWSMetadata)
		// Skip if no AWS metadata is available
		if !hasAWS {
			continue
		}

		// Get any metric to access its dimensions
		anyMetric := metricMap[getAnyKey(metricMap)]
		if anyMetric == nil {
			continue
		}

		// Create filename with timestamp and dimension hash
		filename := filepath.Join(a.outputPath,
			fmt.Sprintf("emf_aggregate_%d_%s.json",
				time.Now().Unix(),
				createSafeHash(dimHash)))

		// Create output map with string keys
		outputMap := make(map[string]interface{})

		// Add AWS metadata
		outputMap["_aws"] = awsMetadata

		// Add dimensions if present
		if len(anyMetric.Dimensions) > 0 {
			outputMap["dimensions"] = anyMetric.Dimensions
		}

		// Add timestamp if present in metadata
		if ts, ok := metadata["timestamp"]; ok {
			outputMap["timestamp"] = ts
		}

		// Add all metric values
		for name, value := range metricMap {
			mv := MetricValue{
				Values: []float64{value.Sum / float64(value.Count)}, // average
				Counts: []int64{value.Count},
				Min:    value.Min,
				Max:    value.Max,
				Sum:    value.Sum,
				Count:  value.Count,
				Unit:   value.Unit,
			}
			outputMap[name] = mv
		}

		// Add all extra fields from metadata
		for key, value := range metadata {
			// Skip special fields we've already handled
			if key != "_aws" && key != "dimensions" && key != "timestamp" {
				// Convert any map[interface{}]interface{} to map[string]interface{}
				outputMap[key] = convertToStringKeyMap(value)
			}
		}

		// Create file
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %v", filename, err)
		}

		// Create encoder with indentation for readability
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")

		// Encode the map
		if err := encoder.Encode(outputMap); err != nil {
			file.Close()
			os.Remove(filename)
			return fmt.Errorf("failed to write to file %s: %v", filename, err)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file %s: %v", filename, err)
		}
	}

	// Reset metrics after successful flush
	a.metrics = make(map[string]map[string]*AggregatedValue)
	a.metadataStore = make(map[string]map[string]interface{})
	a.LastFlush = time.Now()

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

// Helper function to create a safe filename hash
func createSafeHash(s string) string {
	// Create a SHA-256 hash of the dimension string
	h := sha256.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))[:8] // Use first 8 characters of hash
}

// Helper functions
func createDimensionHash(dimensions map[string]string) string {
	// Create a stable hash for dimensions
	var hash string
	for k, v := range dimensions {
		hash += fmt.Sprintf("%s=%s;", k, v)
	}
	return hash
}

func getAnyKey(m map[string]*AggregatedValue) string {
	for k := range m {
		return k
	}
	return ""
}
