package emf

import (
	"testing"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
)

type dumbFlusher struct {
}

func (f *dumbFlusher) Flush(events []common.EMFEvent) error {
	return nil
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

	// metricvalue := 1.0

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
		MetricData: map[string]common.MetricValue{
			"test-metric": {},
		},
	}

	log.Init("test", log.WarnLevel)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			agg.AggregateMetric(&metric)
		}
		agg.flush()
	}
}
