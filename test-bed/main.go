package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
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
	host := flag.String("host", "fluent-bit", "Host to connect to")
	port := flag.Int("port", 5170, "Port to send data to")
	recordsPerSecond := flag.Int("rps", 100, "Records per second to generate")
	maxRetries := flag.Int("retries", 30, "Maximum number of connection retries")
	retryInterval := flag.Duration("retry-interval", 2*time.Second, "Time between retries")
	flag.Parse()

	// Override host from environment if provided
	if envHost := os.Getenv("FLUENT_HOST"); envHost != "" {
		*host = envHost
	}

	var conn net.Conn
	var err error

	// Connection retry loop
	log.Printf("Attempting to connect to %s:%d", *host, *port)
	for i := 0; i < *maxRetries; i++ {
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", *host, *port))
		if err == nil {
			log.Printf("Successfully connected to %s:%d", *host, *port)
			break
		}
		log.Printf("Connection attempt %d failed: %v. Retrying in %v...", i+1, err, *retryInterval)
		time.Sleep(*retryInterval)
	}

	if err != nil {
		log.Fatalf("Failed to connect after %d attempts: %v", *maxRetries, err)
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

	ticker := time.NewTicker(time.Second / time.Duration(*recordsPerSecond))
	defer ticker.Stop()

	log.Printf("Starting to generate %d EMF records per second...", *recordsPerSecond)

	for range ticker.C {
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
			if conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", *host, *port)); err != nil {
				log.Printf("Failed to reconnect: %v", err)
			}
		}
	}
}
