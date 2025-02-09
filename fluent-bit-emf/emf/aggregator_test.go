package emf

import (
	"testing"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
)

type dumbFlusher struct {
}

func (f *dumbFlusher) Flush(events []common.EMFEvent) (int, int, error) {
	return 0, 0, nil
}

// a very dumb benchmark to just clal aggregate with the same value over and over
func BenchmarkAggregator(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	options := common.PluginOptions{
		AggregationPeriod: time.Second * 5,
	}

	agg, err := NewEMFAggregator(&options, &dumbFlusher{})

	if err != nil {
		b.Fatal(err)
	}

	metricvalue := 1.0

	metric := EMFMetric{
		AWS: &common.AWSMetadata{
			CloudWatchMetrics: []common.ProjectionDefinition{
				{
					Dimensions: [][]string{
						{
							"test-dimension",
						},
					},
					Namespace: "test/namespace",
					Metrics: []common.MetricDefinition{
						{
							Name: "test-metric",
							Unit: "Count",
						},
					},
				},
			},
		},
		Dimensions: map[string]string{
			"test-dimension": "test-value",
		},
		MetricData: map[string]MetricValue{
			"test-metric": {
				Value: &metricvalue,
			},
		},
	}

	for i := 0; i < b.N; i++ {
		agg.AggregateMetric(&metric)
	}
}
