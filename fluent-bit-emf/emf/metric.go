package emf

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/utils"
)

// EMF structures remain the same, but we'll add a new constructor
func FillEmfFromRecord(record map[interface{}]interface{}, emf *common.EMFReport) error {
	dimensionSet := make(map[string]struct{})
	metricSet := make(map[string]struct{})

	var awsData map[interface{}]interface{}
	var ok bool

	if awsData, ok = record["_aws"].(map[interface{}]interface{}); !ok {
		return fmt.Errorf("aws metadata did not exist or not expected form")
	}

	// Handle Timestamp
	if ts, exists := awsData["Timestamp"]; !exists {
		return fmt.Errorf("no timestamp was found in aws data; likely means malformed record")
	} else {
		switch v := ts.(type) {
		case int64:
			emf.AWS.Timestamp = v
		case int:
			emf.AWS.Timestamp = int64(v)
		case uint:
			emf.AWS.Timestamp = int64(v)
		case uint32:
			emf.AWS.Timestamp = int64(v)
		case uint64:
			emf.AWS.Timestamp = int64(v)
		default:
			return fmt.Errorf("timestamp was not int, int64, uint, uint32, or uint64; was %v", reflect.TypeOf(v))
		}
	}

	var i int
	var metricDef interface{}
	var md map[interface{}]interface{}
	var dimArray []interface{}
	var j int
	var dim interface{}
	var dimSet []interface{}
	var k int
	var d interface{}
	var metricsArray []interface{}
	var metric interface{}
	var m map[interface{}]interface{}

	cwArray := awsData["CloudWatchMetrics"].([]interface{})

	emf.AWS.Resize(len(cwArray))
	for i, metricDef = range cwArray {
		def := common.GetProjectionStruct()
		md = metricDef.(map[interface{}]interface{})
		def.Namespace = utils.ToString(md["Namespace"])
		dimArray = md["Dimensions"].([]interface{})

		def.ResizeDimensions(len(dimArray))
		for j, dim = range dimArray {
			dimSet = dim.([]interface{})
			dimStrings := make([]string, len(dimSet))
			for k, d = range dimSet {
				dimStrings[k] = utils.ToString(d)
				dimensionSet[utils.ToString(d)] = struct{}{}
			}
			// we sort here so we can do easy comparisons later
			sort.Strings(dimStrings)
			def.Dimensions[j] = dimStrings
		}

		metricsArray = md["Metrics"].([]interface{})
		def.ResizeMetrics(len(metricsArray))

		for j, metric = range metricsArray {
			metricDef := common.MetricDefinition{}
			m = metric.(map[interface{}]interface{})
			name := utils.ToString(m["Name"])
			unit := utils.ToString(m["Unit"])
			metricDef.Name = name
			metricDef.Unit = unit
			def.Metrics[j] = &metricDef
			metricSet[name] = struct{}{}
		}

		emf.AWS.CloudWatchMetrics[i] = def
	}

	for key, value := range record {
		strKey := utils.ToString(key)
		switch strKey {
		case "_aws":
			continue
		default:
			// Check if this is a metric value
			if _, exists := metricSet[strKey]; exists {
				emf.MetricData[strKey] = parseMetricValue(value)
			} else if _, exists := dimensionSet[strKey]; exists {
				emf.Dimensions[strKey] = utils.ToString(value)
			}
		}
	}

	return nil
}

func parseMetricValue(value interface{}) *common.MetricValue {
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
			mv.Min = utils.ConvertToFloat64(min)
		}
		if max, ok := v["Max"]; ok {
			mv.Max = utils.ConvertToFloat64(max)
		}
		if sum, ok := v["Sum"]; ok {
			mv.Sum = utils.ConvertToFloat64(sum)
		}
		if count, ok := v["Count"]; ok {
			mv.Count = uint(utils.ConvertToUint(count))
		}
	default:
		// Handle simple value
		value := utils.ConvertToFloat64(v)
		mv.Values = []float64{value}
		mv.Counts = []uint{1}
		mv.Count = uint(1)
		mv.Max = value
		mv.Min = value
		mv.Sum = value
	}

	return &mv
}
