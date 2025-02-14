package common

import "sync"

var (
	projectionPool = sync.Pool{
		New: func() any {
			return &ProjectionDefinition{}
		},
	}
)

type MetricDefinition struct {
	Name string `json:"Name"`
	Unit string `json:"Unit,omitempty"`
}

type ProjectionDefinition struct {
	Namespace  string              `json:"Namespace"`
	Dimensions [][]string          `json:"Dimensions"`
	Metrics    []*MetricDefinition `json:"Metrics"`
}

func GetProjectionStruct() *ProjectionDefinition {
	return projectionPool.Get().(*ProjectionDefinition)
}

func (p *ProjectionDefinition) FillFrom(from *ProjectionDefinition) {
	p.Namespace = from.Namespace
	p.ResizeDimensions(len(from.Dimensions))
	p.ResizeMetrics(len(from.Metrics))
	for i, val := range from.Dimensions {
		p.Dimensions[i] = make([]string, len(val))
		copy(p.Dimensions[i], val)
	}
	for i, val := range from.Metrics {
		p.Metrics[i] = &MetricDefinition{
			Name: val.Name,
			Unit: val.Unit,
		}
	}
}

func (p *ProjectionDefinition) ResizeDimensions(size int) {
	if cap(p.Dimensions) < size {
		p.Dimensions = make([][]string, size)
	} else {
		p.Dimensions = p.Dimensions[:size]
	}
}

func (p *ProjectionDefinition) ResizeMetrics(size int) {
	if cap(p.Metrics) < size {
		p.Metrics = make([]*MetricDefinition, size)
	} else {
		p.Metrics = p.Metrics[:size]
	}
}

func (p *ProjectionDefinition) close() {
	projectionPool.Put(p)
}

func merge(old []*MetricDefinition, new []*MetricDefinition) {
	for _, attempt := range new {
		exists := false
		for _, v := range old {
			if v.Name == attempt.Name && v.Unit == attempt.Unit {
				exists = true
				break
			}
		}
		if !exists {
			old = append(old, attempt)
		}
	}
}

// we can only merge if the namespaces match and the dimension sets match
// even if the namespaces match, if the dimensions aren't the same we risk
// emitting metrics under dimensions they weren't intended to be emitted under
func (def *ProjectionDefinition) attemptMerge(new *ProjectionDefinition) bool {
	if def.Namespace != new.Namespace {
		return false
	}
	// for now because of how we generate the hash we can skip this check
	merge(def.Metrics, new.Metrics)
	return true
	// if utils.Every(def.Dimensions, func(val []string) bool {
	// 	return utils.Find(new.Dimensions, func(test []string) bool {
	// 		return strings.Join(val, ", ") == strings.Join(test, ", ")
	// 	}) != -1
	// }) {
	// 	merge(def.Metrics, new.Metrics)
	// 	return true
	// } else {
	// 	return false
	// }
}
