package common

import (
	"sync"

	"golang.org/x/exp/maps"
)

var (
	emfReportPool = sync.Pool{
		New: func() any {
			return &EMFReport{
				MetricData: make(map[string]*MetricValue),
				Dimensions: make(map[string]string),
				AWS:        &AWSMetadata{},
			}
		},
	}
)

func GetEmfStruct() *EMFReport {
	return emfReportPool.Get().(*EMFReport)
}

type EMFReport struct {
	AWS        *AWSMetadata            `json:"_aws"`
	Dimensions map[string]string       `json:"-"`
	MetricData map[string]*MetricValue `json:"-"`
}

func (e *EMFReport) reset() {
	maps.Clear(e.Dimensions)
	maps.Clear(e.MetricData)
	e.AWS.Close()
}

func (e *EMFReport) Close() {
	e.reset()
	emfReportPool.Put(e)
}
