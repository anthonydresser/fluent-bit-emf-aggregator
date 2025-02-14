package metricaggregator

import (
	"math"
	"testing"
)

func TestNewExponentialHistogram(t *testing.T) {
	h := NewExponentialHistogram()

	if h.binSize != math.Log(1+epsilon) {
		t.Errorf("Expected binSize %v, got %v", math.Log(1+epsilon), h.binSize)
	}

	if h.count != 0 {
		t.Errorf("Expected initial count 0, got %v", h.count)
	}

	if h.sum != 0 {
		t.Errorf("Expected initial sum 0, got %v", h.sum)
	}
}

func TestAdd(t *testing.T) {
	h := NewExponentialHistogram()

	testCases := []struct {
		value    float64
		count    uint
		expected uint
	}{
		{1.0, 1, 1},
		{2.0, 2, 2},
		{math.NaN(), 1, 0},  // Should not add NaN
		{math.Inf(1), 1, 0}, // Should not add Inf
	}

	for _, tc := range testCases {
		h.Add(tc.value, tc.count)
		if !math.IsNaN(tc.value) && !math.IsInf(tc.value, 0) {
			bucket := h.getBucketIndex(tc.value)
			if count := h.GetBucketCount(bucket); count != tc.expected {
				t.Errorf("For value %v, expected count %v, got %v", tc.value, tc.expected, count)
			}
		}
	}
}

func TestMinMax(t *testing.T) {
	h := NewExponentialHistogram()

	h.Add(1.0, 1)
	h.Add(2.0, 1)
	h.Add(0.5, 1)

	if h.min != 0.5 {
		t.Errorf("Expected min 0.5, got %v", h.min)
	}

	if h.max != 2.0 {
		t.Errorf("Expected max 2.0, got %v", h.max)
	}
}

func TestMean(t *testing.T) {
	h := NewExponentialHistogram()

	// Test empty histogram
	if mean := h.Mean(); mean != 0 {
		t.Errorf("Expected mean 0 for empty histogram, got %v", mean)
	}

	// Test with values
	h.Add(2.0, 1)
	h.Add(4.0, 1)

	expectedMean := 3.0
	if mean := h.Mean(); math.Abs(mean-expectedMean) > 1e-10 {
		t.Errorf("Expected mean %v, got %v", expectedMean, mean)
	}
}

func TestMerge(t *testing.T) {
	h1 := NewExponentialHistogram()
	h2 := NewExponentialHistogram()

	h1.Add(1.0, 1)
	h1.Add(2.0, 1)
	h2.Add(3.0, 1)
	h2.Add(4.0, 1)

	h1.Merge(h2)

	expectedCount := uint(4)
	if h1.count != expectedCount {
		t.Errorf("Expected merged count %v, got %v", expectedCount, h1.count)
	}

	expectedSum := 10.0 // 1 + 2 + 3 + 4
	if math.Abs(h1.sum-expectedSum) > 1e-10 {
		t.Errorf("Expected merged sum %v, got %v", expectedSum, h1.sum)
	}
}

func TestGetNonEmptyBuckets(t *testing.T) {
	h := NewExponentialHistogram()

	h.Add(1.0, 1)
	h.Add(2.0, 2)

	buckets := h.GetNonEmptyBuckets()

	if len(buckets) == 0 {
		t.Error("Expected non-empty buckets, got empty result")
	}

	// Verify bucket counts
	for _, bucket := range buckets {
		if bucket.Count <= 0 {
			t.Errorf("Expected positive count, got %v", bucket.Count)
		}
	}
}
