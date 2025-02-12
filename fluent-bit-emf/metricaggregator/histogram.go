package metricaggregator

import (
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

type histogram struct {
	// internal
	counts map[float64]uint
}

func newHistogram() *histogram {
	return &histogram{
		counts: make(map[float64]uint, 0),
	}
}

func (va *histogram) Add(metric *common.MetricValue) error {
	for i, value := range metric.Values {
		va.counts[value] += metric.Counts[i]
	}
	return nil
}

func (va *histogram) Reduce() *MetricStats {
	counts := make([]uint, 0, len(va.counts))
	values := make([]float64, 0, len(va.counts))
	for key, count := range va.counts {
		counts = append(counts, count)
		values = append(values, key)
	}
	switch len(va.counts) {
	case 0:
		return nil
	case 1:
		return &MetricStats{
			Values: []float64{values[0]},
			Counts: []uint{counts[0]},
			Min:    values[0],
			Max:    values[0],
			Sum:    float64(values[0]) * float64(va.counts[0]),
			Count:  va.counts[0],
		}
	case 2:
		return &MetricStats{
			Values: []float64{values[0], values[1]},
			Counts: []uint{va.counts[0], va.counts[1]},
			Min:    utils.Min(values[0], values[1]),
			Max:    utils.Max(values[0], values[1]),
			Sum:    float64(values[0])*float64(va.counts[0]) + float64(values[1])*float64(va.counts[1]),
			Count:  va.counts[0] + va.counts[1],
		}
	default:
		histogram := NewExponentialHistogram()
		for i := range values {
			histogram.Add(values[i], counts[i])
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
