package emf

import (
	"context"
	"time"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/log"
)

type ScheduledTask struct {
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	errors   chan error
	work     func() error
}

func NewScheduledTask(interval time.Duration, target func() error) *ScheduledTask {
	ctx, cancel := context.WithCancel(context.Background())
	return &ScheduledTask{
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
		errors:   make(chan error, 1),
		work:     target,
	}
}

func (st *ScheduledTask) Start() {
	ticker := time.NewTicker(st.interval)
	go func() {
		defer ticker.Stop()
		defer close(st.errors)

		for {
			select {
			case <-st.ctx.Done():
				return
			case <-ticker.C:
				if err := st.work(); err != nil {
					log.Error().Printf("Encountered error during flush: %v", err)
					continue
				}
			}
		}
	}()
}

func (st *ScheduledTask) Stop() {
	st.cancel()
}

func (st *ScheduledTask) Errors() <-chan error {
	return st.errors
}
