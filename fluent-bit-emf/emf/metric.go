package emf

import (
	"fmt"
)

type AWSMetadata struct {
	Timestamp         int64              `json:"Timestamp,omitempty"`
	CloudWatchMetrics []MetricDefinition `json:"CloudWatchMetrics"`
}

type MetricDefinition struct {
	Namespace  string     `json:"Namespace"`
	Dimensions [][]string `json:"Dimensions"`
	Metrics    []struct {
		Name string `json:"Name"`
		Unit string `json:"Unit,omitempty"`
	} `json:"Metrics"`
}

type MetricValue struct {
	Value  *float64  `json:"Value,omitempty"`
	Values []float64 `json:"Values,omitempty"`
	Counts []int64   `json:"Counts,omitempty"`
	Min    float64   `json:"Min,omitempty"`
	Max    float64   `json:"Max,omitempty"`
	Sum    float64   `json:"Sum,omitempty"`
	Count  int64     `json:"Count,omitempty"`
}

type EMFMetric struct {
	AWS          *AWSMetadata           `json:"_aws"`
	DimensionSet map[string]bool        `json:"-"`
	Dimensions   map[string]string      `json:"-"`
	MetricData   map[string]MetricValue `json:"-"`
}

// EMF structures remain the same, but we'll add a new constructor
func EmfFromRecord(record map[interface{}]interface{}) (*EMFMetric, error) {
	emf := &EMFMetric{
		MetricData:   make(map[string]MetricValue),
		DimensionSet: make(map[string]bool),
		Dimensions:   make(map[string]string),
	}

	if awsData, ok := record["_aws"].(map[interface{}]interface{}); ok {
		aws := &AWSMetadata{}

		// Handle CloudWatch Metrics
		if cwMetrics, exists := awsData["CloudWatchMetrics"]; exists {
			if metricsArray, ok := cwMetrics.([]interface{}); ok {
				aws.CloudWatchMetrics = make([]MetricDefinition, len(metricsArray))
				for i, metricDef := range metricsArray {
					if md, ok := metricDef.(map[interface{}]interface{}); ok {
						// Parse Namespace
						if ns, exists := md["Namespace"]; exists {
							aws.CloudWatchMetrics[i].Namespace = toString(ns)
						}

						// Parse Dimensions
						if dims, exists := md["Dimensions"]; exists {
							if dimArray, ok := dims.([]interface{}); ok {
								aws.CloudWatchMetrics[i].Dimensions = make([][]string, len(dimArray))
								for j, dim := range dimArray {
									if dimSet, ok := dim.([]interface{}); ok {
										dimStrings := make([]string, len(dimSet))
										for k, d := range dimSet {
											dimStrings[k] = toString(d)
											emf.DimensionSet[toString(d)] = true
										}
										aws.CloudWatchMetrics[i].Dimensions[j] = dimStrings
									}
								}
							}
						}

						// Parse Metrics
						if metrics, exists := md["Metrics"]; exists {
							if metricsArray, ok := metrics.([]interface{}); ok {
								aws.CloudWatchMetrics[i].Metrics = make([]struct {
									Name string `json:"Name"`
									Unit string `json:"Unit,omitempty"`
								}, len(metricsArray))

								for j, metric := range metricsArray {
									if m, ok := metric.(map[interface{}]interface{}); ok {
										aws.CloudWatchMetrics[i].Metrics[j].Name = toString(m["Name"])
										aws.CloudWatchMetrics[i].Metrics[j].Unit = toString(m["Unit"])
									}
								}
							}
						}
					}
				}
			}
		}

		// Handle Timestamp
		if ts, exists := awsData["Timestamp"]; exists {
			switch v := ts.(type) {
			case int64:
				aws.Timestamp = v
			case float64:
				aws.Timestamp = int64(v)
			}
		}

		emf.AWS = aws
	}

	for key, value := range record {
		strKey := toString(key)
		switch strKey {
		case "_aws":
			continue
		default:
			// Check if this is a metric value
			isMetric := false
			if emf.AWS != nil {
				for _, metricDef := range emf.AWS.CloudWatchMetrics {
					for _, metric := range metricDef.Metrics {
						if metric.Name == strKey {
							isMetric = true
							metricValue := parseMetricValue(value)
							emf.MetricData[strKey] = metricValue
							break
						}
					}
					if isMetric {
						break
					}
				}
			}

			if !isMetric {
				_, present := emf.DimensionSet[strKey]
				if present {
					emf.Dimensions[strKey] = toString(value)
				}
			}
		}
	}

	return emf, nil
}

func parseMetricValue(value interface{}) MetricValue {
	mv := MetricValue{}

	switch v := value.(type) {
	case map[interface{}]interface{}:
		// Handle structured metric value
		if values, ok := v["Values"].([]interface{}); ok {
			mv.Values = make([]float64, len(values))
			for i, val := range values {
				mv.Values[i] = convertToFloat64(val)
			}
		}
		if counts, ok := v["Counts"].([]interface{}); ok {
			mv.Counts = make([]int64, len(counts))
			for i, count := range counts {
				mv.Counts[i] = int64(convertToFloat64(count))
			}
		}
		if min, ok := v["Min"]; ok {
			mv.Min = convertToFloat64(min)
		}
		if max, ok := v["Max"]; ok {
			mv.Max = convertToFloat64(max)
		}
		if sum, ok := v["Sum"]; ok {
			mv.Sum = convertToFloat64(sum)
		}
		if count, ok := v["Count"]; ok {
			mv.Count = int64(convertToFloat64(count))
		}
	default:
		// Handle simple value
		value := convertToFloat64(v)
		mv.Value = &value
	}

	return mv
}

// Helper function to convert interface{} to string
func toString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
