package metricaggregator

import (
	"fmt"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
)

type MetricAggregator interface {
	Add(val common.MetricValue) error
	Reduce() *MetricStats
}

type MetricStats struct {
	Values []float64 `json:"Values,omitempty"`
	Counts []uint    `json:"Counts,omitempty"`
	Min    float64   `json:"Min"`
	Max    float64   `json:"Max"`
	Sum    float64   `json:"Sum"`
	Count  uint      `json:"Count"`
}

func InitMetricAggregator(sample common.MetricValue) (MetricAggregator, error) {
	// based on the sample we can predetermine what kind of aggregator we need
	if len(sample.Counts) > 0 && len(sample.Values) > 0 {
		return newHistogram(), nil
	} else if sample.Sum != nil && sample.Count != nil && sample.Max != nil && sample.Min != nil {
		return newRateAggregator(), nil
	}
	return nil, fmt.Errorf("could not determine the right type of aggregator to use for sample %v", sample)
}
