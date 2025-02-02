package main

/*
#include <stdlib.h>
#include <stdint.h>
#include "fluent-bit/flb_plugin.h"
*/
import (
	"C"
	"fmt"
	"os"
	"time"
	"unsafe"

	"github.com/anthonydresser/fluent-bit-emf/emf"
	"github.com/fluent/fluent-bit-go/output"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	fmt.Printf("[debug] Enter FLBPluginRegister")
	return output.FLBPluginRegister(def, "emf_aggregator", "EMF File Aggregator")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	fmt.Printf("[debug] Enter FLBPluginInit")
	outputPath := output.FLBPluginConfigKey(plugin, "OutputPath")
	if outputPath == "" {
		outputPath = "/tmp/emf_aggregated"
	}

	period := output.FLBPluginConfigKey(plugin, "AggregationPeriod")
	if period == "" {
		period = "1m"
	}

	aggregationPeriod, err := time.ParseDuration(period)
	if err != nil {
		fmt.Printf("[error] invalid aggregation period: %v\n", err)
		return output.FLB_ERROR
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		fmt.Printf("[error] failed to create output directory: %v\n", err)
		return output.FLB_ERROR
	}

	aggregator := emf.NewEMFAggregator(outputPath, aggregationPeriod)

	output.FLBPluginSetContext(plugin, aggregator)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	fmt.Printf("[debug] Enter FLBPluginFlush with length: %d\n", length)
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered in FLBPluginFlush: %v\n", r)
		} else {
			fmt.Printf("[debug] FLBPluginFlush completed successfully\n")
		}
	}()
	dec := output.NewDecoder(data, int(length))
	aggregator := output.FLBPluginGetContext(ctx).(*emf.EMFAggregator)

	count := 0
	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}
		count++

		// fmt.Printf("[debug] Processing record #%d at timestamp: %v\n", count, ts)
		// printRecord(record)

		// Create EMF metric directly from record
		emf, err := emf.EmfFromRecord(record)

		if err != nil {
			fmt.Printf("[error] failed to process EMF record: %v\n", err)
			continue

		}

		// Aggregate the metric
		aggregator.AggregateMetric(emf, ts.(output.FLBTime))

		// Check if it's time to flush
		if time.Since(aggregator.LastFlush) >= aggregator.AggregationPeriod {
			if err := aggregator.Flush(); err != nil {
				fmt.Printf("[error] failed to flush metrics: %v\n", err)
			}
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
