package metricaggregator

import (
	"math"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
)

type rateAggregator struct {
	sum   float64
	max   float64
	min   float64
	count uint
}

func newRateAggregator() *rateAggregator {
	return &rateAggregator{
		sum:   0,
		max:   math.MinInt64,
		min:   math.MaxFloat64,
		count: 0,
	}
}

func (r *rateAggregator) Add(metric *common.MetricValue) error {
	r.sum += metric.Sum
	r.count += metric.Count
	if metric.Max > r.max {
		r.max = metric.Max
	}
	if metric.Min < r.min {
		r.min = metric.Min
	}
	return nil
}

func (r *rateAggregator) Reduce() *MetricStats {
	if r.count == 0 {
		return &MetricStats{}
	}
	return &MetricStats{
		Sum:   r.sum,
		Min:   r.min,
		Max:   r.max,
		Count: r.count,
	}
}
