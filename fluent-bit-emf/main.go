package main

/*
#include <stdlib.h>
#include <stdint.h>
#include "fluent-bit/flb_plugin.h"
*/
import (
	"C"
	"fmt"
	"log"
	"time"
	"unsafe"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/emf"
	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/options"
	"github.com/fluent/fluent-bit-go/output"
)

type CustomWriter struct{}

func (f CustomWriter) Write(bytes []byte) (int, error) {
	return fmt.Print("[" + time.Now().UTC().Format("2006/01/02 15:04:05") + "] " + string(bytes))
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.SetFlags(0)
	log.SetOutput(new(CustomWriter))
	log.Println("[ info] [emf-aggregator] Enter FLBPluginRegister")
	return output.FLBPluginRegister(def, "emf_aggregator", "EMF File Aggregator")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	log.Println("[ info] [emf-aggregator] Initializing")

	options := options.PluginOptions{}

	options.OutputPath = output.FLBPluginConfigKey(plugin, "output_path")
	options.LogGroupName = output.FLBPluginConfigKey(plugin, "log_group_name")
	options.LogStreamName = output.FLBPluginConfigKey(plugin, "log_stream_name")
	options.CloudWatchEndpoint = output.FLBPluginConfigKey(plugin, "endpoint")
	options.Protocol = output.FLBPluginConfigKey(plugin, "protocol")

	period := output.FLBPluginConfigKey(plugin, "aggregation_period")
	if period == "" {
		log.Println("[ warn] [emf-aggregator] AggregationPeriod not set, defaulting to 1m")
		period = "1m"
	}

	aggregationPeriod, err := time.ParseDuration(period)
	if err != nil {
		log.Printf("[ error] [emf-aggregator] invalid aggregation period: %v\n", err)
		return output.FLB_ERROR
	}

	options.AggregationPeriod = aggregationPeriod

	aggregator, err := emf.NewEMFAggregator(options)
	if err != nil {
		log.Printf("[ error] [emf-aggregator] failed to create EMFAggregator: %v\n", err)
		return output.FLB_ERROR
	}

	output.FLBPluginSetContext(plugin, aggregator)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[ error] [emf-aggregator] Recovered in FLBPluginFlush: %v\n", r)
		}
	}()
	dec := output.NewDecoder(data, int(length))
	aggregator := output.FLBPluginGetContext(ctx).(*emf.EMFAggregator)

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Create EMF metric directly from record
		emf, err := emf.EmfFromRecord(record)

		if err != nil {
			log.Printf("[ error] [emf-aggregator] failed to process EMF record: %v\n", err)
			continue
		}

		// Aggregate the metric
		aggregator.AggregateMetric(emf)
		aggregator.Stats.InputRecords++
	}

	aggregator.Stats.InputLength += int64(length)

	// Check if it's time to flush
	if time.Since(aggregator.LastFlush) >= aggregator.AggregationPeriod {
		if err := aggregator.Flush(); err != nil {
			log.Printf("[ error] [emf-aggregator] failed to flush metrics: %v\n", err)
		}
	}

	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	// perform a last flush before we are killed
	// I don't think this actually works
	aggregator := output.FLBPluginGetContext(ctx).(*emf.EMFAggregator)
	if err := aggregator.Flush(); err != nil {
		log.Printf("[ error] [emf-aggregator] failed to flush metrics: %v\n", err)
	}
	return output.FLB_OK
}

func main() {
}
