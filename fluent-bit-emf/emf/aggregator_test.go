package emf

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
)

func TestAggregator(t *testing.T) {
	aggregator, _ := createTestAggregator(time.Second * 60)
	data := createTestRecord(5, 5)
	// Create EMF metric directly from record
	emf, err := EmfFromRecord(data)

	if err != nil {
		log.Error().Printf("failed to process EMF record: %v\n", err)
	}

	// Aggregate the metric
	aggregator.AggregateMetric(emf)
}

// Mock flusher for testing
type mockFlusher struct {
	flushCount int
	events     []common.EMFEvent
}

func (m *mockFlusher) Flush(events []common.EMFEvent) error {
	m.flushCount++
	m.events = events
	return nil
}

// Helper to create test records
func createTestRecord(metricCount int, dimensionCount int) map[interface{}]interface{} {
	record := make(map[interface{}]interface{})

	dimensions := make([]interface{}, 0, dimensionCount)
	for i := 0; i < dimensionCount; i++ {
		dimensions = append(dimensions, []interface{}{fmt.Sprintf("dimension_%d", i)})
		record[fmt.Sprintf("dimension_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	metricDefs := make([]interface{}, 0, metricCount)
	for i := 0; i < metricCount; i++ {
		metricDefs = append(metricDefs, map[interface{}]interface{}{
			"Name": fmt.Sprintf("metric_%d", i),
			"Unit": "Milliseconds",
		})
		record[fmt.Sprintf("metric_%d", i)] = rand.Float64() * 100
	}

	// Add AWS metadata
	record["_aws"] = map[interface{}]interface{}{
		"Timestamp": time.Now().Unix(),
		"CloudWatchMetrics": []interface{}{
			map[interface{}]interface{}{
				"Namespace":  "TestNamespace",
				"Dimensions": dimensions,
				"Metrics":    metricDefs,
			},
		},
	}

	return record
}

// Helper to create multiple records
func createTestRecords(recordCount, metricCount, dimensionCount int) []map[interface{}]interface{} {
	records := make([]map[interface{}]interface{}, recordCount)
	for i := 0; i < recordCount; i++ {
		records[i] = createTestRecord(metricCount, dimensionCount)
	}

	// Convert to unsafe.Pointer (you'll need to implement this based on your actual serialization)
	// This is a placeholder - implement actual conversion logic
	return records
}

func createTestAggregator(period time.Duration) (*EMFAggregator, *mockFlusher) {
	flusher := &mockFlusher{}
	options := &common.PluginOptions{
		AggregationPeriod: period,
	}

	aggregator, _ := NewEMFAggregator(options, flusher)
	return aggregator, flusher
}

func BenchmarkEMFAggregator(b *testing.B) {
	log.SetLevel(log.WarnLevel)
	testCases := []struct {
		name           string
		recordCount    int
		metricCount    int
		dimensionCount int
		period         time.Duration
	}{
		{"Small_Few_Dimensions", 100, 2, 2, time.Second * 60},
		{"Small_Many_Dimensions", 100, 2, 10, time.Second * 60},
		{"Medium_Few_Dimensions", 1000, 5, 2, time.Second * 60},
		{"Medium_Many_Dimensions", 1000, 5, 10, time.Second * 60},
		{"Large_Few_Dimensions", 10000, 10, 2, time.Second * 60},
		{"Large_Many_Dimensions", 10000, 10, 10, time.Second * 60},
		{"Massive_Few_Dimensions", 100000, 20, 2, time.Second * 60},
		{"Massive_Many_Dimensions", 100000, 20, 10, time.Second * 60},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			aggregator, _ := createTestAggregator(tc.period)

			// Create test data once before the benchmark loop
			data := createTestRecords(tc.recordCount, tc.metricCount, tc.dimensionCount)

			// Reset timer before the actual benchmark
			b.ResetTimer()

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				records := make([]*EMFMetric, 0, 60)
				for _, datum := range data {

					// Create EMF metric directly from record
					emf, err := EmfFromRecord(datum)

					if err != nil {
						log.Error().Printf("failed to process EMF record: %v\n", err)
						continue
					}

					records = append(records, emf)
				}
				for _, record := range records {
					// Aggregate the metric
					aggregator.AggregateMetric(record)
				}

				// Force flush
				aggregator.flush()
			}
		})
	}
}

// Memory allocation benchmark
func BenchmarkEMFAggregatorMemory(b *testing.B) {
	b.Run("Memory_Usage", func(b *testing.B) {
		aggregator, _ := createTestAggregator(time.Second * 60)

		data := createTestRecords(10000, 10, 5)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			records := make([]*EMFMetric, 0, 60)
			for _, datum := range data {

				// Create EMF metric directly from record
				emf, err := EmfFromRecord(datum)

				if err != nil {
					log.Error().Printf("failed to process EMF record: %v\n", err)
					continue
				}

				records = append(records, emf)
			}

			for _, record := range records {
				// Aggregate the metric
				aggregator.AggregateMetric(record)
			}
		}
	})
}
