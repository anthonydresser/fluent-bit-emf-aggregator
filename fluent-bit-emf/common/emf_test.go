package common

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestEmfEvent(t *testing.T) {
	event := EMFEvent{
		AWS: &AWSMetadata{
			Timestamp: 1234567890,
		},
		OtherFields: map[string]interface{}{
			"foo": "bar",
			"baz": 123,
		},
	}

	// First try with json.Marshal to verify the struct tags
	data, _ := json.Marshal(event)
	fmt.Println("Marshal output:", string(data))

	// Then try with Encoder
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "")
	fmt.Print("Encoder output: ")
	encoder.Encode(event)
}
