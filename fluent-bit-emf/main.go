package main

/*
#include <stdlib.h>
#include <stdint.h>
#include "fluent-bit/flb_plugin.h"
*/
import (
	"C"
	"fmt"
	"time"
	"unsafe"

	"github.com/anthonydresser/fluent-bit-emf/emf"
	"github.com/fluent/fluent-bit-go/output"
)
import "github.com/anthonydresser/fluent-bit-emf/options"

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	fmt.Printf("[debug] [emf-aggregator] Enter FLBPluginRegister\n")
	return output.FLBPluginRegister(def, "emf_aggregator", "EMF File Aggregator")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	fmt.Printf("[debug] [emf-aggregator] Enter FLBPluginInit\n")

	options := options.PluginOptions{}

	options.OutputPath = output.FLBPluginConfigKey(plugin, "OutputPath")

	options.LogGroupName = output.FLBPluginConfigKey(plugin, "LogGroupName")
	options.LogStreamName = output.FLBPluginConfigKey(plugin, "LogStreamName")
	cloudwatchEndpoint := output.FLBPluginConfigKey(plugin, "CloudWatchEndpoint")

	if cloudwatchEndpoint != "" {
		options.CloudWatchEndpoint = &cloudwatchEndpoint
	}

	period := output.FLBPluginConfigKey(plugin, "AggregationPeriod")
	if period == "" {
		fmt.Printf("[warn] [emf-aggregator] AggregationPeriod not set, defaulting to 1m\n")
		period = "1m"
	}

	aggregationPeriod, err := time.ParseDuration(period)
	if err != nil {
		fmt.Printf("[error] [emf-aggregator] invalid aggregation period: %v\n", err)
		return output.FLB_ERROR
	}

	options.AggregationPeriod = aggregationPeriod

	aggregator, err := emf.NewEMFAggregator(options)
	if err != nil {
		fmt.Printf("[error] [emf-aggregator] failed to create EMFAggregator: %v\n", err)
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, aggregator)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[error] [emf-aggregator] Recovered in FLBPluginFlush: %v\n", r)
		}
	}()
	dec := output.NewDecoder(data, int(length))
	aggregator := output.FLBPluginGetContext(ctx).(*emf.EMFAggregator)

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Create EMF metric directly from record
		emf, err := emf.EmfFromRecord(record)

		if err != nil {
			fmt.Printf("[error] [emf-aggregator] failed to process EMF record: %v\n", err)
			continue
		}

		// Aggregate the metric
		aggregator.AggregateMetric(emf, ts.(output.FLBTime))
		aggregator.Stats.InputRecords++
	}

	aggregator.Stats.InputLength += int64(length)

	// Check if it's time to flush
	if time.Since(aggregator.LastFlush) >= aggregator.AggregationPeriod {
		if err := aggregator.Flush(); err != nil {
			fmt.Printf("[error] [emf-aggregator] failed to flush metrics: %v\n", err)
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
