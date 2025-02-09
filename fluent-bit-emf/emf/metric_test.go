package emf

import (
	"reflect"
	"testing"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

func TestEmfFromRecord_ValidInput(t *testing.T) {
	// Test case with valid input
	input := map[interface{}]interface{}{
		"_aws": map[interface{}]interface{}{
			"Timestamp": int64(1234567890),
			"CloudWatchMetrics": []interface{}{
				map[interface{}]interface{}{
					"Namespace": "TestNamespace",
					"Dimensions": []interface{}{
						[]interface{}{"DimensionName"},
					},
					"Metrics": []interface{}{
						map[interface{}]interface{}{
							"Name": "TestMetric",
							"Unit": "Count",
						},
					},
				},
			},
		},
		"TestMetric":    float64(100),
		"DimensionName": "DimensionValue",
	}

	emf, err := EmfFromRecord(input)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Verify AWS metadata
	if emf.AWS.Timestamp != 1234567890 {
		t.Errorf("Expected timestamp 1234567890, got %d", emf.AWS.Timestamp)
	}

	// Verify CloudWatchMetrics
	if len(emf.AWS.CloudWatchMetrics) != 1 {
		t.Errorf("Expected 1 CloudWatchMetrics, got %d", len(emf.AWS.CloudWatchMetrics))
	}

	// Verify metric data
	if value := *emf.MetricData["TestMetric"].Value; value != 100 {
		t.Errorf("Expected metric value 100, got %f", value)
	}

	// Verify dimensions
	if value := emf.Dimensions["DimensionName"]; value != "DimensionValue" {
		t.Errorf("Expected dimension value 'DimensionValue', got %s", value)
	}
}

func TestEmfFromRecord_InvalidInput(t *testing.T) {
	testCases := []struct {
		name  string
		input map[interface{}]interface{}
	}{
		{
			name:  "Missing AWS metadata",
			input: map[interface{}]interface{}{},
		},
		{
			name: "Missing Timestamp",
			input: map[interface{}]interface{}{
				"_aws": map[interface{}]interface{}{
					"CloudWatchMetrics": []interface{}{},
				},
			},
		},
		{
			name: "Missing CloudWatchMetrics",
			input: map[interface{}]interface{}{
				"_aws": map[interface{}]interface{}{
					"Timestamp": int64(1234567890),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EmfFromRecord(tc.input)
			if err == nil {
				t.Error("Expected error, got nil")
			}
		})
	}
}

func TestParseMetricValue(t *testing.T) {
	expectedMin := 1.0
	expectedMax := 2.0
	expectedSum := 3.0
	expectedCount := uint(2)
	testCases := []struct {
		name     string
		input    interface{}
		expected MetricValue
	}{
		{
			name:  "Simple float value",
			input: float64(123.45),
			expected: MetricValue{
				Value: float64Ptr(123.45),
			},
		},
		{
			name: "Structured metric value",
			input: map[interface{}]interface{}{
				"Values": []interface{}{float64(1.0), float64(2.0)},
				"Counts": []interface{}{float64(1), float64(1)},
				"Min":    float64(1.0),
				"Max":    float64(2.0),
				"Sum":    float64(3.0),
				"Count":  float64(2),
			},
			expected: MetricValue{
				Values: []float64{1.0, 2.0},
				Counts: []uint{1, 1},
				Min:    &expectedMin,
				Max:    &expectedMax,
				Sum:    &expectedSum,
				Count:  &expectedCount,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseMetricValue(tc.input)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("Expected %+v, got %+v", tc.expected, result)
			}
		})
	}
}

func TestToString(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "String input",
			input:    "test",
			expected: "test",
		},
		{
			name:     "Byte slice input",
			input:    []byte("test"),
			expected: "test",
		},
		{
			name:     "Integer input",
			input:    123,
			expected: "123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := utils.ToString(tc.input)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

// Helper function to create float64 pointer
func float64Ptr(v float64) *float64 {
	return &v
}
