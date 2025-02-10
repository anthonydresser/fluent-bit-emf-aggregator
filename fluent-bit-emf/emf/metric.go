package emf

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

type EMFMetric struct {
	AWS        *common.AWSMetadata           `json:"_aws"`
	Dimensions map[string]string             `json:"-"`
	MetricData map[string]common.MetricValue `json:"-"`
}

// EMF structures remain the same, but we'll add a new constructor
func EmfFromRecord(record map[interface{}]interface{}) (*EMFMetric, error) {
	emf := &EMFMetric{
		MetricData: make(map[string]common.MetricValue),
		Dimensions: make(map[string]string),
	}

	dimensionSet := make(map[string]struct{})
	metricSet := make(map[string]struct{})

	var awsData map[interface{}]interface{}

	if rawAwsData, exists := record["_aws"]; !exists {
		return nil, fmt.Errorf("no aws metadata from found in record; likely means malformed record")
	} else {
		var ok bool
		if awsData, ok = rawAwsData.(map[interface{}]interface{}); !ok {
			return nil, fmt.Errorf("aws metadata was not expected type; expect map, was %v", rawAwsData)
		}
	}

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
										dimensionSet[utils.ToString(d)] = struct{}{}
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
									name := utils.ToString(m["Name"])
									unit := utils.ToString(m["Unit"])
									aws.CloudWatchMetrics[i].Metrics[j].Name = name
									aws.CloudWatchMetrics[i].Metrics[j].Unit = unit
									metricSet[name] = struct{}{}
								}
							}
						}
					}
				}
			}
		}
	}
	emf.AWS = aws

	for key, value := range record {
		strKey := utils.ToString(key)
		switch strKey {
		case "_aws":
			continue
		default:
			// Check if this is a metric value
			if _, exists := metricSet[strKey]; exists {
				metricValue := parseMetricValue(value)
				if !isValidMetric(&metricValue) {
					log.Warn().Printf("Found invalid metric %s with value %v\n; original: %v", strKey, metricValue, value)
					continue
				}
				emf.MetricData[strKey] = metricValue
			} else if _, exists := dimensionSet[strKey]; exists {
				emf.Dimensions[strKey] = utils.ToString(value)
			}
		}
	}

	return emf, nil
}

func parseMetricValue(value interface{}) common.MetricValue {
	mv := common.MetricValue{}

	switch v := value.(type) {
	case map[interface{}]interface{}:
		// Handle structured metric value
		if values, exists := v["Values"]; exists {
			if actual, ok := values.([]interface{}); ok {
				mv.Values = make([]float64, len(actual))
				for i, val := range actual {
					mv.Values[i] = utils.ConvertToFloat64(val)
				}
			}
		}
		if counts, exists := v["Counts"]; exists {
			if actual, ok := counts.([]interface{}); ok {
				mv.Counts = make([]uint, len(actual))
				for i, val := range actual {
					mv.Counts[i] = uint(utils.ConvertToUint(val))
				}
			}
		}
		if min, ok := v["Min"]; ok {
			mv.Min = utils.ToPtr(utils.ConvertToFloat64(min))
		}
		if max, ok := v["Max"]; ok {
			mv.Max = utils.ToPtr(utils.ConvertToFloat64(max))
		}
		if sum, ok := v["Sum"]; ok {
			mv.Sum = utils.ToPtr(utils.ConvertToFloat64(sum))
		}
		if count, ok := v["Count"]; ok {
			mv.Count = utils.ToPtr(uint(utils.ConvertToUint(count)))
		}
	default:
		// Handle simple value
		value := utils.ConvertToFloat64(v)
		mv.Values = []float64{value}
		mv.Counts = []uint{1}
		mv.Count = utils.ToPtr(uint(1))
		mv.Max = utils.ToPtr(value)
		mv.Min = utils.ToPtr(value)
		mv.Sum = utils.ToPtr(value)
	}

	return mv
}

func isValidMetric(value *common.MetricValue) bool {
	if len(value.Values) != 0 && len(value.Counts) != 0 {
		return true
	} else if value.Sum != nil && value.Count != nil && value.Max != nil && value.Min != nil {
		return true
	} else {
		return false
	}
}
