package common

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

type EMFEvent struct {
	AWS         *AWSMetadata
	OtherFields map[string]interface{}
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

func (e EMFEvent) MarshalJSON() ([]byte, error) {
	output := make(map[string]interface{})
	output["_aws"] = e.AWS

	for k, v := range e.OtherFields {
		output[k] = v
	}
	return json.Marshal(output)
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

type MetricValue struct {
	Value  *float64  `json:"Value,omitempty"`
	Values []float64 `json:"Values,omitempty"`
	Counts []uint    `json:"Counts,omitempty"`
	Min    *float64  `json:"Min,omitempty"`
	Max    *float64  `json:"Max,omitempty"`
	Sum    *float64  `json:"Sum,omitempty"`
	Count  *uint     `json:"Count,omitempty"`
}

func (m MetricValue) String() string {
	var b strings.Builder
	b.Grow(256) // Pre-allocate space
	b.WriteString("{ ")

	// Helper function to handle nil checks and formatting
	addField := func(name string, value interface{}, isPointer bool) {
		if b.Len() > 2 { // Add comma if not the first field
			b.WriteString(", ")
		}
		b.WriteString(name + ": ")

		if value == nil {
			b.WriteString("nil")
			return
		}

		if !isPointer {
			// For non-pointer types (slices)
			b.WriteString(fmt.Sprintf("%v", value))
			return
		}

		// For pointer types, we need to check the concrete type
		switch v := value.(type) {
		case *float64:
			if v == nil {
				b.WriteString("nil")
			} else {
				b.WriteString(fmt.Sprintf("%v", *v))
			}
		case *int64:
			if v == nil {
				b.WriteString("nil")
			} else {
				b.WriteString(fmt.Sprintf("%v", *v))
			}
		default:
			b.WriteString("nil")
		}
	}

	// Add all fields using the helper function
	addField("Value", m.Value, true)
	addField("Values", m.Values, false)
	addField("Counts", m.Counts, false)
	addField("Min", m.Min, true)
	addField("Max", m.Max, true)
	addField("Sum", m.Sum, true)
	addField("Count", m.Count, true)

	b.WriteString(" }")
	return b.String()
}
