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
	Version    string                 `json:"_aws"`
	Timestamp  time.Time              `json:"timestamp"`
	Metadata   map[string]interface{} `json:"metadata"`
	Dimensions map[string]string      `json:"dimensions"`
	Metrics    []MetricDefinition     `json:"metrics"`
	Values     map[string]float64     `json:"values"`
}

type MetricDefinition struct {
	Name string `json:"name"`
	Unit string `json:"unit"`
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
	services := []string{"api", "web", "auth", "db"}
	environments := []string{"prod", "staging", "dev"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	metricNames := []string{"latency", "cpu_usage", "memory_usage", "request_count"}
	metricUnits := []string{"milliseconds", "percent", "bytes", "count"}

	ticker := time.NewTicker(time.Second / time.Duration(*recordsPerSecond))
	defer ticker.Stop()

	log.Printf("Starting to generate %d EMF records per second...", *recordsPerSecond)

	for range ticker.C {
		// Generate random dimensions
		service := services[rand.Intn(len(services))]
		env := environments[rand.Intn(len(environments))]
		region := regions[rand.Intn(len(regions))]

		// Create EMF record
		emf := EMFMetric{
			Version:   "0",
			Timestamp: time.Now(),
			Metadata: map[string]interface{}{
				"service_info": map[string]string{
					"version": "1.0.0",
				},
			},
			Dimensions: map[string]string{
				"service":     service,
				"environment": env,
				"region":      region,
			},
			Metrics: make([]MetricDefinition, len(metricNames)),
			Values:  make(map[string]float64),
		}

		// Add metrics and values
		for i, name := range metricNames {
			emf.Metrics[i] = MetricDefinition{
				Name: name,
				Unit: metricUnits[i],
			}
			// Generate random values appropriate for each metric
			switch name {
			case "latency":
				emf.Values[name] = rand.Float64() * 100 // 0-100ms
			case "cpu_usage":
				emf.Values[name] = rand.Float64() * 100 // 0-100%
			case "memory_usage":
				emf.Values[name] = rand.Float64() * 1024 * 1024 * 1024 // 0-1GB
			case "request_count":
				emf.Values[name] = float64(rand.Intn(1000)) // 0-1000 requests
			}
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
			if conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", *port)); err != nil {
				log.Printf("Failed to reconnect: %v", err)
			}
		}
	}
}
