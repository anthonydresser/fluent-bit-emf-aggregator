package utils

import (
	"fmt"
	"strconv"
)

// Helper function to convert interface{} to float64
func ConvertToFloat64(v interface{}) float64 {
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

func Find[T any](array []T, test func(T) bool) int {
	found := -1
	for i, v := range array {
		if test(v) {
			found = i
			break
		}
	}
	return found
}

func Every[T any](array []T, test func(T) bool) bool {
	for _, v := range array {
		if !test(v) {
			return false
		}
	}
	return true
}

func Min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// Helper function to convert interface{} to string
func ToString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
