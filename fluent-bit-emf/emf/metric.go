package emf

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

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

type EMFMetric struct {
	AWS          *common.AWSMetadata    `json:"_aws"`
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

	if rawAwsData, exists := record["_aws"]; !exists {
		return nil, fmt.Errorf("no aws metadata from found in record; likely means malformed record")
	} else {
		if awsData, ok := rawAwsData.(map[interface{}]interface{}); !ok {
			return nil, fmt.Errorf("aws metadata was not expected type; expect map, was %v", rawAwsData)
		} else {
			aws := &common.AWSMetadata{}

			// Handle Timestamp
			if ts, exists := awsData["Timestamp"]; !exists {
				return nil, fmt.Errorf("no timestamp was found in aws data; likely means malformed record")
			} else {
				switch v := ts.(type) {
				case int64:
					aws.Timestamp = v
				case int:
					aws.Timestamp = int64(v)
				case uint:
					aws.Timestamp = int64(v)
				case uint32:
					aws.Timestamp = int64(v)
				case uint64:
					aws.Timestamp = int64(v)
				default:
					return nil, fmt.Errorf("timestamp was not int, int64, uint, uint32, or uint64; was %v", reflect.TypeOf(v))
				}
			}

			// Handle CloudWatch Metrics
			if cwMetrics, exists := awsData["CloudWatchMetrics"]; !exists {
				return nil, fmt.Errorf("no CloudWatchMetrics key was found; likely means malformed record")
			} else {
				if metricsArray, ok := cwMetrics.([]interface{}); !ok {
					return nil, fmt.Errorf("CloudWatchMetrics is not an array; likely means malformed record")
				} else {
					aws.CloudWatchMetrics = make([]common.ProjectionDefinition, len(metricsArray))
					for i, metricDef := range metricsArray {
						if md, ok := metricDef.(map[interface{}]interface{}); !ok {
							log.Warn().Printf("Skipping metric: Metric definition was not a map, was %v\n", metricDef)
							continue
						} else {
							// Parse Namespace
							if ns, exists := md["Namespace"]; !exists {
								log.Warn().Println("Skipping metric: Metric definition had no Namespace field")
								continue
							} else {
								aws.CloudWatchMetrics[i].Namespace = utils.ToString(ns)
							}

							// Parse Dimensions
							if dims, exists := md["Dimensions"]; !exists {
								log.Warn().Println("Skipping metric: Metric definition had no Dimensions field")
								continue
							} else {
								if dimArray, ok := dims.([]interface{}); !ok {
									log.Warn().Printf("Skipping metric: Dimensions was not an array, was %v\n", dims)
									continue
								} else {
									aws.CloudWatchMetrics[i].Dimensions = make([][]string, len(dimArray))
									for j, dim := range dimArray {
										if dimSet, ok := dim.([]interface{}); !ok {
											log.Warn().Printf("Skipping dimension set: was not an array, was %v\n", dim)
											continue
										} else {
											dimStrings := make([]string, len(dimSet))
											for k, d := range dimSet {
												dimStrings[k] = utils.ToString(d)
												emf.DimensionSet[utils.ToString(d)] = true
											}
											// we sort here so we can do easy comparisons later
											sort.Strings(dimStrings)
											aws.CloudWatchMetrics[i].Dimensions[j] = dimStrings
										}
									}
								}
							}

							// Parse Metrics
							if metrics, exists := md["Metrics"]; !exists {
								log.Warn().Println("Skipping metric: Metric definition had no Metrics field")
								continue
							} else {
								if metricsArray, ok := metrics.([]interface{}); !ok {
									log.Warn().Printf("Skipping metric: Metrics was not an array, was %v\n", metrics)
									continue
								} else {
									aws.CloudWatchMetrics[i].Metrics = make([]common.MetricDefinition, len(metricsArray))

									for j, metric := range metricsArray {
										if m, ok := metric.(map[interface{}]interface{}); !ok {
											log.Warn().Printf("Skipping metric: Metric was not a map, was %v\n", metric)
											continue
										} else {
											aws.CloudWatchMetrics[i].Metrics[j].Name = utils.ToString(m["Name"])
											aws.CloudWatchMetrics[i].Metrics[j].Unit = utils.ToString(m["Unit"])
										}
									}
								}
							}
						}
					}
				}
			}
			emf.AWS = aws
		}
	}

	for key, value := range record {
		strKey := utils.ToString(key)
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
							if !isValidMetric(&metricValue) {
								log.Warn().Printf("Found invalid metric %s with value %v\n; original: %v", strKey, metricValue, value)
								continue
							}
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
				if _, present := emf.DimensionSet[strKey]; present {
					emf.Dimensions[strKey] = utils.ToString(value)
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
				mv.Values[i] = utils.ConvertToFloat64(val)
			}
		}
		if counts, ok := v["Counts"].([]interface{}); ok {
			mv.Counts = make([]uint, len(counts))
			for i, count := range counts {
				mv.Counts[i] = uint(utils.ConvertToFloat64(count))
			}
		}
		if min, ok := v["Min"]; ok {
			value := utils.ConvertToFloat64(min)
			mv.Min = &value
		}
		if max, ok := v["Max"]; ok {
			value := utils.ConvertToFloat64(max)
			mv.Max = &value
		}
		if sum, ok := v["Sum"]; ok {
			value := utils.ConvertToFloat64(sum)
			mv.Sum = &value
		}
		if count, ok := v["Count"]; ok {
			value := uint(utils.ConvertToFloat64(count))
			mv.Count = &value
		}
	default:
		// Handle simple value
		value := utils.ConvertToFloat64(v)
		mv.Value = &value
	}

	return mv
}

func isValidMetric(value *MetricValue) bool {
	if value.Value != nil {
		return true
	} else if len(value.Values) != 0 && len(value.Counts) != 0 {
		return true
	} else if value.Max != nil && value.Count != nil && *value.Max == *value.Min {
		return true
	} else {
		return false
	}
}
