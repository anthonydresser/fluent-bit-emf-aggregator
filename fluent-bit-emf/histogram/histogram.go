package histogram

import (
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

type Histogram struct {
	// internal
	values []float64
	counts []uint
}

type HistogramStats struct {
	Values []float64 `json:"Values"`
	Counts []uint    `json:"Counts"`
	Min    float64   `json:"Min"`
	Max    float64   `json:"Max"`
	Sum    float64   `json:"Sum"`
}

func NewHistogram() *Histogram {
	return &Histogram{
		values: make([]float64, 0),
		counts: make([]uint, 0),
	}
}

func (va *Histogram) Add(value float64, count uint) {
	for i := 0; i < len(va.values); i++ {
		if va.values[i] == value {
			va.counts[i] += count
			return
		}
	}
	va.values = append(va.values, value)
	va.counts = append(va.counts, count)
}

func (va *Histogram) Reduce() *HistogramStats {
	switch len(va.values) {
	case 0:
		return nil
	case 1:
		return &HistogramStats{
			Values: []float64{va.values[0]},
			Counts: []uint{va.counts[0]},
			Min:    va.values[0],
			Max:    va.values[0],
			Sum:    float64(va.values[0]) * float64(va.counts[0]),
		}
	case 2:
		return &HistogramStats{
			Values: []float64{va.values[0], va.values[1]},
			Counts: []uint{va.counts[0], va.counts[1]},
			Min:    utils.Min(va.values[0], va.values[1]),
			Max:    utils.Max(va.values[0], va.values[1]),
			Sum:    float64(va.values[0])*float64(va.counts[0]) + float64(va.values[1])*float64(va.counts[1]),
		}
	default:
		histogram := NewExponentialHistogram()
		for i := range va.values {
			histogram.Add(va.values[i], va.counts[i])
		}
		if histogram.max == histogram.min {
			return &HistogramStats{
				Values: []float64{histogram.min},
				Counts: []uint{histogram.count},
				Min:    histogram.min,
				Max:    histogram.max,
				Sum:    histogram.sum,
			}
		}
		buckets := histogram.GetNonEmptyBuckets()
		values := make([]float64, len(buckets))
		counts := make([]uint, len(buckets))
		for i := range buckets {
			values[i] = buckets[i].Value
			counts[i] = buckets[i].Count
		}
		return &HistogramStats{
			Values: values,
			Counts: counts,
			Min:    histogram.min,
			Max:    histogram.max,
			Sum:    histogram.sum,
		}
	}
}
