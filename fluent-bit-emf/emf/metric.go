package emf

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

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
	Tag          string                 `json:"-"`
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
			aws := &AWSMetadata{}

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
					aws.CloudWatchMetrics = make([]ProjectionDefinition, len(metricsArray))
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
									aws.CloudWatchMetrics[i].Metrics = make([]MetricDefinition, len(metricsArray))

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
			mv.Counts = make([]int64, len(counts))
			for i, count := range counts {
				mv.Counts[i] = int64(utils.ConvertToFloat64(count))
			}
		}
		if min, ok := v["Min"]; ok {
			mv.Min = utils.ConvertToFloat64(min)
		}
		if max, ok := v["Max"]; ok {
			mv.Max = utils.ConvertToFloat64(max)
		}
		if sum, ok := v["Sum"]; ok {
			mv.Sum = utils.ConvertToFloat64(sum)
		}
		if count, ok := v["Count"]; ok {
			mv.Count = int64(utils.ConvertToFloat64(count))
		}
	default:
		// Handle simple value
		value := utils.ConvertToFloat64(v)
		mv.Value = &value
	}

	return mv
}
