package common

import (
	"fmt"
	"strings"
)

type MetricValue struct {
	Values []float64
	Counts []uint
	Min    float64
	Max    float64
	Sum    float64
	Count  uint
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
		case *uint:
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
	addField("Values", m.Values, false)
	addField("Counts", m.Counts, false)
	addField("Min", m.Min, true)
	addField("Max", m.Max, true)
	addField("Sum", m.Sum, true)
	addField("Count", m.Count, true)

	b.WriteString(" }")
	return b.String()
}
