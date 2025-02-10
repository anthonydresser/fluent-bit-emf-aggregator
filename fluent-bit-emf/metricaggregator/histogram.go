package metricaggregator

import (
	"fmt"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

type histogram struct {
	// internal
	values []float64
	counts []uint
}

func newHistogram() *histogram {
	return &histogram{
		values: make([]float64, 0),
		counts: make([]uint, 0),
	}
}

func (va *histogram) Add(metric common.MetricValue) error {
	if metric.Value != nil {
		va.add(*metric.Value, 1)
	} else if len(metric.Counts) > 0 && len(metric.Values) > 0 {
		for i, value := range metric.Values {
			va.add(value, metric.Counts[i])
		}
	} else if metric.Count != nil && metric.Max != nil && metric.Min != nil && *metric.Min == *metric.Max {
		va.add(*metric.Min, *metric.Count)
	} else {
		return fmt.Errorf("invalid metric: %v", metric)
	}
	return nil
}

func (va *histogram) add(value float64, count uint) {
	for i := 0; i < len(va.values); i++ {
		if va.values[i] == value {
			va.counts[i] += count
			return
		}
	}
	va.values = append(va.values, value)
	va.counts = append(va.counts, count)
}

func (va *histogram) Reduce() *MetricStats {
	switch len(va.values) {
	case 0:
		return nil
	case 1:
		return &MetricStats{
			Values: []float64{va.values[0]},
			Counts: []uint{va.counts[0]},
			Min:    va.values[0],
			Max:    va.values[0],
			Sum:    float64(va.values[0]) * float64(va.counts[0]),
			Count:  va.counts[0],
		}
	case 2:
		return &MetricStats{
			Values: []float64{va.values[0], va.values[1]},
			Counts: []uint{va.counts[0], va.counts[1]},
			Min:    utils.Min(va.values[0], va.values[1]),
			Max:    utils.Max(va.values[0], va.values[1]),
			Sum:    float64(va.values[0])*float64(va.counts[0]) + float64(va.values[1])*float64(va.counts[1]),
			Count:  va.counts[0] + va.counts[1],
		}
	default:
		histogram := NewExponentialHistogram()
		for i := range va.values {
			histogram.Add(va.values[i], va.counts[i])
		}
		if histogram.max == histogram.min {
			return &MetricStats{
				Values: []float64{histogram.min},
				Counts: []uint{histogram.count},
				Min:    histogram.min,
				Max:    histogram.max,
				Sum:    histogram.sum,
				Count:  histogram.count,
			}
		}
		buckets := histogram.GetNonEmptyBuckets()
		values := make([]float64, len(buckets))
		counts := make([]uint, len(buckets))
		count := uint(0)
		for i := range buckets {
			values[i] = buckets[i].Value
			counts[i] = buckets[i].Count
			count += buckets[i].Count
		}
		return &MetricStats{
			Values: values,
			Counts: counts,
			Min:    histogram.min,
			Max:    histogram.max,
			Sum:    histogram.sum,
			Count:  count,
		}
	}
}
