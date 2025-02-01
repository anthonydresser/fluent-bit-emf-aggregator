package main

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
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)
import "strconv"

// EMF JSON structures
type EMFMetric struct {
	Version    string                 `json:"_aws"`
	Timestamp  time.Time              `json:"timestamp"`
	Metadata   map[string]interface{} `json:"metadata"`
	Dimensions map[string]string      `json:"dimensions"`
	Metrics    []MetricDefinition     `json:"metrics"`
	Values     map[string]float64     `json:"values"`
}

type MetricDefinition struct {
	Name string `json:"name"`
	Unit string `json:"unit"`
}

// Plugin context
type EMFAggregator struct {
	mu                sync.RWMutex
	outputPath        string
	aggregationPeriod time.Duration
	lastFlush         time.Time
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

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	fmt.Printf("[debug] Enter FLBPluginRegister")
	return output.FLBPluginRegister(def, "emf_aggregator", "EMF File Aggregator")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	fmt.Printf("[debug] Enter FLBPluginInit")
	outputPath := output.FLBPluginConfigKey(plugin, "OutputPath")
	if outputPath == "" {
		outputPath = "/tmp/emf_aggregated"
	}

	period := output.FLBPluginConfigKey(plugin, "AggregationPeriod")
	if period == "" {
		period = "1m"
	}

	aggregationPeriod, err := time.ParseDuration(period)
	if err != nil {
		fmt.Printf("[error] invalid aggregation period: %v\n", err)
		return output.FLB_ERROR
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		fmt.Printf("[error] failed to create output directory: %v\n", err)
		return output.FLB_ERROR
	}

	aggregator := &EMFAggregator{
		outputPath:        outputPath,
		aggregationPeriod: aggregationPeriod,
		lastFlush:         time.Now(),
		metrics:           make(map[string]map[string]*AggregatedValue),
		metadataStore:     make(map[string]map[string]interface{}),
		definitionStore:   make(map[string]MetricDefinition),
	}

	output.FLBPluginSetContext(plugin, aggregator)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	fmt.Printf("[debug] Enter FLBPluginFlush with length: %d\n", length)
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered in FLBPluginFlush: %v\n", r)
		}
	}()
	dec := output.NewDecoder(data, int(length))
	aggregator := output.FLBPluginGetContext(ctx).(*EMFAggregator)

	count := 0
	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}
		count++

		// fmt.Printf("[debug] Processing record #%d at timestamp: %v\n", count, ts)
		// printRecord(record)

		// Create EMF metric directly from record
		emf := EMFMetric{
			Version:    toString(record["_aws"]),
			Timestamp:  parseTime(toString(record["timestamp"])),
			Metadata:   convertToStringMap(record["metadata"]).(map[string]interface{}),
			Dimensions: convertMapToStringString(record["dimensions"].(map[interface{}]interface{})),
			Metrics:    convertMetrics(record["metrics"].([]interface{})),
			Values:     convertMapToFloat64(record["values"].(map[interface{}]interface{})),
		}

		// Aggregate the metric
		aggregator.aggregateMetric(emf, ts.(output.FLBTime))

		// Check if it's time to flush
		if time.Since(aggregator.lastFlush) >= aggregator.aggregationPeriod {
			if err := aggregator.flushToFile(); err != nil {
				fmt.Printf("[error] failed to flush metrics: %v\n", err)
			}
		}
	}

	return output.FLB_OK
}

// Helper function to convert interface{} to string
func toString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func convertMapToStringString(m map[interface{}]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		key := toString(k)
		value := toString(v)
		result[key] = value
	}
	return result
}

func convertMetrics(metrics []interface{}) []MetricDefinition {
	result := make([]MetricDefinition, len(metrics))
	for i, metric := range metrics {
		m := metric.(map[interface{}]interface{})
		result[i] = MetricDefinition{
			Name: toString(m["name"]),
			Unit: toString(m["unit"]),
		}
	}
	return result
}

func convertMapToFloat64(m map[interface{}]interface{}) map[string]float64 {
	result := make(map[string]float64)
	for k, v := range m {
		key := toString(k)
		switch value := v.(type) {
		case float64:
			result[key] = value
		case int:
			result[key] = float64(value)
		case int64:
			result[key] = float64(value)
		case []byte:
			if f, err := strconv.ParseFloat(string(value), 64); err == nil {
				result[key] = f
			}
		default:
			// Try to convert string to float if necessary
			if f, err := strconv.ParseFloat(toString(v), 64); err == nil {
				result[key] = f
			}
		}
	}
	return result
}

func parseTime(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		fmt.Printf("[error] Failed to parse time %s: %v\n", timeStr, err)
		return time.Now()
	}
	return t
}

func convertToStringMap(v interface{}) interface{} {
	switch v := v.(type) {
	case map[interface{}]interface{}:
		newMap := make(map[string]interface{})
		for k, v := range v {
			newMap[fmt.Sprintf("%v", k)] = convertToStringMap(v)
		}
		return newMap
	case []interface{}:
		newSlice := make([]interface{}, len(v))
		for i, v := range v {
			newSlice[i] = convertToStringMap(v)
		}
		return newSlice
	case []byte:
		return string(v)
	default:
		return v
	}
}

func printRecord(record map[interface{}]interface{}) {
	// Convert the entire record to a string-keyed map
	stringMap := convertToStringMap(record).(map[string]interface{})

	// Pretty print the converted map
	jsonBytes, err := json.MarshalIndent(stringMap, "", "    ")
	if err != nil {
		fmt.Printf("[debug] Error marshaling record: %v\n", err)
		return
	}

	fmt.Printf("[debug] Record contents:\n%s\n", string(jsonBytes))
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func (a *EMFAggregator) aggregateMetric(emf EMFMetric, ts output.FLBTime) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create dimension hash for grouping
	dimHash := createDimensionHash(emf.Dimensions)

	// Store metadata if not exists
	if _, exists := a.metadataStore[dimHash]; !exists {
		a.metadataStore[dimHash] = emf.Metadata
	}

	// Store metric definitions
	for _, def := range emf.Metrics {
		a.definitionStore[def.Name] = def
	}

	// Initialize metric map for this dimension set if not exists
	if _, exists := a.metrics[dimHash]; !exists {
		a.metrics[dimHash] = make(map[string]*AggregatedValue)
	}

	// Aggregate each value
	for name, value := range emf.Values {
		if _, exists := a.metrics[dimHash][name]; !exists {
			a.metrics[dimHash][name] = &AggregatedValue{
				Min:        value,
				Max:        value,
				Dimensions: emf.Dimensions,
				Unit:       a.definitionStore[name].Unit,
			}
		}

		metric := a.metrics[dimHash][name]
		metric.Sum += value
		metric.Count++
		metric.Min = min(metric.Min, value)
		metric.Max = max(metric.Max, value)
		metric.LastUpdated = ts
	}
}

func (a *EMFAggregator) flushToFile() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create aggregated EMF records
	var emfRecords []EMFMetric

	fmt.Printf("[debug] Flushing Records of length: %d\n", len(emfRecords))

	for dimHash, metricMap := range a.metrics {
		// Create a new EMF record for each dimension set
		emf := EMFMetric{
			Version:    "0",
			Timestamp:  time.Now(),
			Metadata:   a.metadataStore[dimHash],
			Dimensions: metricMap[getAnyKey(metricMap)].Dimensions,
			Metrics:    make([]MetricDefinition, 0),
			Values:     make(map[string]float64),
		}

		// Add metrics and values
		for name, value := range metricMap {
			emf.Metrics = append(emf.Metrics, MetricDefinition{
				Name: name,
				Unit: value.Unit,
			})
			emf.Values[name] = value.Sum / float64(value.Count) // Using average for aggregation
		}

		emfRecords = append(emfRecords, emf)
	}

	// Write to file
	filename := filepath.Join(a.outputPath, fmt.Sprintf("emf_aggregate_%d.json", time.Now().Unix()))
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(emfRecords); err != nil {
		return fmt.Errorf("failed to write to file: %v", err)
	}

	// Reset metrics after successful flush
	a.metrics = make(map[string]map[string]*AggregatedValue)
	a.lastFlush = time.Now()

	return nil
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

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func main() {
}
