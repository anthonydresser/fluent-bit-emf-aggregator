package emf

import (
	"strconv"
)

// Helper function to convert interface{} to float64
func convertToFloat64(v interface{}) float64 {
	switch v := v.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func find[T any](array []T, test func(T) bool) int {
	found := -1
	for i, v := range array {
		if test(v) {
			found = i
			break
		}
	}
	return found
}

func every[T any](array []T, test func(T) bool) bool {
	for _, v := range array {
		if !test(v) {
			return false
		}
	}
	return true
}
