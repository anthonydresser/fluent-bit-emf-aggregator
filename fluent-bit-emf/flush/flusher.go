package flush

import (
	"fmt"

	"github.com/anthonydresser/fluent-bit-emf-aggregator/fluent-bit-emf/common"
)

type Flusher interface {
	Flush(events []common.EMFEvent) (int, int, error)
}

func InitFlusher(options *common.PluginOptions) (Flusher, error) {
	var flusher Flusher
	var err error
	if options.OutputPath != "" {
		flusher, err = init_file_flush(options.OutputPath)
	} else if options.LogGroupName != "" && options.LogStreamName != "" {
		flusher, err = init_cloudwatch_flush(options.LogGroupName, options.LogStreamName, options.CloudWatchEndpoint, options.Protocol)
	} else {
		err = fmt.Errorf("no output configured")
	}

	return flusher, err
}
