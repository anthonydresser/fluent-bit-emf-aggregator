package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

type EMFMetric struct {
	AWS       AWSMetadata `json:"_aws"`
	Latency   MetricValue `json:"Latency"`
	Fault     int         `json:"Fault"`
	Size      int         `json:"Size"`
	Function  MetricValue `json:"Function:Called"`
	Service   string      `json:"ServiceName"`
	Operation string      `json:"Operation"`
	Client    string      `json:"Client"`
	RequestID string      `json:"RequestId"`
}

type AWSMetadata struct {
	Timestamp         int64              `json:"Timestamp"`
	CloudWatchMetrics []MetricDefinition `json:"CloudWatchMetrics"`
}

type MetricDefinition struct {
	Namespace  string     `json:"Namespace"`
	Dimensions [][]string `json:"Dimensions"`
	Metrics    []Metric   `json:"Metrics"`
}

type Metric struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type MetricValue struct {
	Values []float64 `json:"Values"`
	Counts []int     `json:"Counts"`
	Min    float64   `json:"Min"`
	Max    float64   `json:"Max"`
	Sum    float64   `json:"Sum"`
	Count  int       `json:"Count"`
}

type CustomWriter struct{}

func (f CustomWriter) Write(bytes []byte) (int, error) {
	return fmt.Print("[" + time.Now().UTC().Format("2006/01/02 15:04:05") + "] " + string(bytes))
}

func generateRequestID() string {
	return fmt.Sprintf("%x-%x-%x-%x",
		rand.Int31(), rand.Int31(),
		rand.Int31(), rand.Int31())
}

func createMetricValue(value float64) MetricValue {
	return MetricValue{
		Values: []float64{value},
		Counts: []int{1},
		Min:    value,
		Max:    value,
		Sum:    value,
		Count:  1,
	}
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(CustomWriter))
	port := os.Getenv("PORT")
	host := os.Getenv("HOST")
	var recordsPerSecond int
	if rawRrecordsPerSecond, exists := os.LookupEnv("RPS"); !exists {
		log.Fatalf("Invalid RPS value")
	} else {
		if parsedRecordsPerSecond, err := strconv.Atoi(rawRrecordsPerSecond); err != nil {
			log.Fatalf("Invalid RPS value: %v", err)
		} else {
			recordsPerSecond = parsedRecordsPerSecond
		}
	}

	maxRetries := 30
	if rawMaxRetries, exists := os.LookupEnv("RETRIES"); exists {
		if parsedRetries, err := strconv.Atoi(rawMaxRetries); err != nil {
			log.Fatalf("Invalid RETRIES value: %v", err)
		} else {
			maxRetries = parsedRetries
		}
	}
	retryInterval := 2 * time.Second
	if rawRetryInterval, exists := os.LookupEnv("RETRY_INTERVAL"); exists {
		if parseRetryInterval, err := strconv.Atoi(rawRetryInterval); err != nil {
			log.Fatalf("Invalid RETRY_INTERVAL value: %v", err)
		} else {
			retryInterval = time.Duration(parseRetryInterval) * time.Second
		}
	}

	var conn net.Conn
	var err error

	// Connection retry loop
	log.Printf("[ info] Attempting to connect to %s:%s", host, port)
	for i := 0; i < maxRetries; i++ {
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
		if err == nil {
			log.Printf("[ info] Successfully connected to %s:%s", host, port)
			break
		}
		log.Printf("Connection attempt %d failed: %v. Retrying in %v...", i+1, err, retryInterval)
		time.Sleep(retryInterval)
	}

	if err != nil {
		log.Fatalf("Failed to connect after %d attempts: %v", maxRetries, err)
	}
	defer conn.Close()

	// Prepare static test data
	services := []string{"ServiceA", "ServiceB", "ServiceC"}
	operations := []string{"GetItem", "PutItem", "Query", "Scan"}
	clients := []string{"ClientA", "ClientB", "ClientC"}

	// Define the metric definition template
	metricDef := MetricDefinition{
		Namespace: "MyService/namespace",
		Dimensions: [][]string{
			{},
			{"ServiceName"},
			{"Operation"},
			{"ServiceName", "Operation"},
			{"Client"},
			{"ServiceName", "Client"},
			{"Operation", "Client"},
			{"ServiceName", "Operation", "Client"},
		},
		Metrics: []Metric{
			{Name: "Latency", Unit: "Milliseconds"},
			{Name: "Fault", Unit: "Count"},
			{Name: "Size", Unit: "Bytes"},
			{Name: "Function:Called", Unit: "Count"},
		},
	}

	// 1ms is the lowest resolution we can go
	duration := time.Second / time.Duration(min(recordsPerSecond, 1000))

	recordsPerRun := max(1, recordsPerSecond/1000)

	log.Printf("[ info] Starting to generate %d EMF records every %v...", recordsPerRun, duration)

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for range ticker.C {
		for i := 0; i < recordsPerRun; i++ {
			// Generate random values
			latency := rand.Float64() * 1000        // 0-1000ms
			size := rand.Intn(1000)                 // 0-1000 bytes
			fault := rand.Intn(2)                   // 0 or 1
			functionCalled := float64(rand.Intn(2)) // 0 or 1

			emf := EMFMetric{
				AWS: AWSMetadata{
					Timestamp:         time.Now().UnixMilli(),
					CloudWatchMetrics: []MetricDefinition{metricDef},
				},
				Latency:   createMetricValue(latency),
				Fault:     fault,
				Size:      size,
				Function:  createMetricValue(functionCalled),
				Service:   services[rand.Intn(len(services))],
				Operation: operations[rand.Intn(len(operations))],
				Client:    clients[rand.Intn(len(clients))],
				RequestID: generateRequestID(),
			}

			// Marshal to JSON and write to socket
			data, err := json.Marshal(emf)
			if err != nil {
				log.Printf("Failed to marshal EMF: %v", err)
				continue
			}

			// Add newline for Fluent Bit parsing
			data = append(data, '\n')

			_, err = conn.Write(data)
			if err != nil {
				log.Printf("Failed to write to socket: %v", err)
				// Try to reconnect
				if conn, err = net.Dial("tcp", fmt.Sprintf("%s:%s", host, port)); err != nil {
					log.Printf("Failed to reconnect: %v", err)
				}
			}
		}
	}
}
