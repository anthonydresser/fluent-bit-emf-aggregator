package common

import (
	"encoding/json"
	"math"
	"sync"

	"golang.org/x/exp/maps"
)

var (
	emfEventPool = sync.Pool{
		New: func() any {
			return &EMFEvent{
				AWS:         &AWSMetadata{},
				OtherFields: make(map[string]interface{}),
			}
		},
	}
)

type EMFEvent struct {
	AWS         *AWSMetadata
	OtherFields map[string]interface{}
}

func GetEmfEventStruct() *EMFEvent {
	return emfEventPool.Get().(*EMFEvent)
}

func (e *EMFEvent) Close() {
	maps.Clear(e.OtherFields)
	e.AWS.Close()
	emfEventPool.Put(e)
}

type AWSMetadata struct {
	Timestamp         int64                   `json:"Timestamp"`
	CloudWatchMetrics []*ProjectionDefinition `json:"CloudWatchMetrics"`
}

func (e *AWSMetadata) Close() {
	for _, val := range e.CloudWatchMetrics {
		val.close()
	}
}

func (e *AWSMetadata) Resize(size int) {
	if cap(e.CloudWatchMetrics) < size {
		e.CloudWatchMetrics = make([]*ProjectionDefinition, size)
	} else {
		e.CloudWatchMetrics = e.CloudWatchMetrics[:size]
	}
}

func (m *AWSMetadata) FillFrom(from *AWSMetadata) {
	m.Timestamp = from.Timestamp
	m.Resize(len(from.CloudWatchMetrics))
	for i, val := range from.CloudWatchMetrics {
		holder := GetProjectionStruct()
		holder.FillFrom(val)
		m.CloudWatchMetrics[i] = holder
	}
}

func (e EMFEvent) MarshalJSON() ([]byte, error) {
	output := make(map[string]interface{})
	output["_aws"] = e.AWS

	for k, v := range e.OtherFields {
		output[k] = v
	}
	return json.Marshal(output)
}

func (m *AWSMetadata) Merge(new *AWSMetadata) {
	m.Timestamp = int64(math.Min(float64(m.Timestamp), float64(new.Timestamp)))
	for _, attempt := range new.CloudWatchMetrics {
		merged := false
		for _, v := range m.CloudWatchMetrics {
			if merged = v.attemptMerge(attempt); merged {
				break
			}
		}
		if !merged {
			m.CloudWatchMetrics = append(m.CloudWatchMetrics, attempt)
		}
	}
}
