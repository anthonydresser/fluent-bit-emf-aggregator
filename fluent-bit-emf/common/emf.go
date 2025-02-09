package common

import (
	"strings"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

type EMFEvent struct {
	AWS         *AWSMetadata           `json:"_aws"`
	OtherFields map[string]interface{} `json:"inline"`
}

type AWSMetadata struct {
	Timestamp         int64                  `json:"Timestamp,omitempty"`
	CloudWatchMetrics []ProjectionDefinition `json:"CloudWatchMetrics"`
}

type MetricDefinition struct {
	Name string `json:"Name"`
	Unit string `json:"Unit,omitempty"`
}

type ProjectionDefinition struct {
	Namespace  string             `json:"Namespace"`
	Dimensions [][]string         `json:"Dimensions"`
	Metrics    []MetricDefinition `json:"Metrics"`
}

func merge(old []MetricDefinition, new []MetricDefinition) {
	for _, attempt := range new {
		exists := false
		for _, v := range old {
			if v.Name == attempt.Name && v.Unit == attempt.Unit {
				exists = true
				break
			}
		}
		if !exists {
			old = append(old, attempt)
		}
	}

}

// we can only merge if the namespaces match and the dimension sets match
// even if the namespaces match, if the dimensions aren't the same we risk
// emitting metrics under dimensions they weren't intended to be emitted under
func (def *ProjectionDefinition) attemptMerge(new *ProjectionDefinition) bool {
	if def.Namespace != new.Namespace {
		return false
	}
	if utils.Every(def.Dimensions, func(val []string) bool {
		return utils.Find(new.Dimensions, func(test []string) bool {
			return strings.Join(val, ", ") == strings.Join(test, ", ")
		}) != -1
	}) {
		merge(def.Metrics, new.Metrics)
		return true
	} else {
		return false
	}
}

func (m *AWSMetadata) Merge(new *AWSMetadata) {
	m.Timestamp = new.Timestamp
	for _, attempt := range new.CloudWatchMetrics {
		merged := false
		for _, v := range m.CloudWatchMetrics {
			merged = v.attemptMerge(&attempt)
			if merged {
				break
			}
		}
		if !merged {
			m.CloudWatchMetrics = append(m.CloudWatchMetrics, attempt)
		}
	}
}
