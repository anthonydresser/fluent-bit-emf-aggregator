package emf

import "math"

type ExponentialHistogram struct {
	buckets map[int]uint64
	binSize float64
	count   uint64
	sum     float64 // New sum field
	min     float64
	max     float64
}

const (
	epsilon = 0.1
)

type Bucket struct {
	Value float64
	Count int64
}

// NewExponentialHistogram creates a new histogram with exponential buckets
func NewExponentialHistogram(base float64) *ExponentialHistogram {
	return &ExponentialHistogram{
		buckets: make(map[int]uint64),
		binSize: math.Log(1 + epsilon),
		sum:     0,
		min:     math.MaxFloat64,
		max:     -math.MaxFloat64,
	}
}

// getBucketIndex returns the bucket index for a given value
func (h *ExponentialHistogram) getBucketIndex(value float64) int {
	if value <= 0 {
		return 0
	}
	return int(math.Floor(math.Log(value) / h.binSize))
}

// GetBucketBounds returns the lower and upper bounds for a bucket
func (h *ExponentialHistogram) ValueOf(bucket int) float64 {
	return math.Exp((float64(bucket) + 0.5) * h.binSize)
}

// GetBucketCount returns the count for a specific bucket
func (h *ExponentialHistogram) GetBucketCount(bucket int) uint64 {
	return h.buckets[bucket]
}

// GetNonEmptyBuckets returns a map of non-empty buckets and their counts
func (h *ExponentialHistogram) GetNonEmptyBuckets() []Bucket {
	result := make([]Bucket, 0)
	for bucket, count := range h.buckets {
		if count > 0 {
			result = append(result, Bucket{Value: h.ValueOf(bucket), Count: int64(count)})
		}
	}
	return result
}

// Add adds a value to the histogram
func (h *ExponentialHistogram) Add(value float64, count uint64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return
	}

	bucket := h.getBucketIndex(value)
	h.buckets[bucket] += count
	h.count += count
	h.sum += (value * float64(count)) // Add to sum

	if value < h.min {
		h.min = value
	}
	if value > h.max {
		h.max = value
	}
}

// Sum returns the sum of all values added
func (h *ExponentialHistogram) Sum() float64 {
	return h.sum
}

// Mean returns the arithmetic mean of all values
func (h *ExponentialHistogram) Mean() float64 {
	if h.count == 0 {
		return 0
	}
	return h.sum / float64(h.count)
}

// Merge combines another histogram into this one
func (h *ExponentialHistogram) Merge(other *ExponentialHistogram) {
	if other.binSize != h.binSize {
		return // Cannot merge histograms with different bases
	}

	for bucket, count := range other.buckets {
		h.buckets[bucket] += count
	}
	h.count += other.count
	h.sum += other.sum // Add the sums
	h.min = math.Min(h.min, other.min)
	h.max = math.Max(h.max, other.max)
}
