package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type LogEvent struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

type LogStream struct {
	LogStreamName string     `json:"logStreamName"`
	CreationTime  time.Time  `json:"creationTime"`
	Events        []LogEvent `json:"events"`
}

type CreateLogStreamRequest struct {
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
}

type PutLogEventsRequest struct {
	LogGroupName  string     `json:"logGroupName"`
	LogStreamName string     `json:"logStreamName"`
	LogEvents     []LogEvent `json:"logEvents"`
}

type PutLogEventsResponse struct {
	NextSequenceToken     string `json:"nextSequenceToken"`
	RejectedLogEventsInfo struct {
		TooNewLogEventStartIndex int `json:"tooNewLogEventStartIndex"`
		TooOldLogEventEndIndex   int `json:"tooOldLogEventEndIndex"`
		ExpiredLogEventEndIndex  int `json:"expiredLogEventEndIndex"`
	} `json:"rejectedLogEventsInfo"`
}

type ErrorResponse struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

type MockCloudWatchLogs struct {
	mu         sync.RWMutex
	logGroups  map[string]map[string]*LogStream // logGroup -> logStreamName -> stream
	tokens     map[string]int                   // stream -> sequence token
	errorCount int                              // Add error counter
}

type CustomWriter struct{}

type Metric struct {
	Name              string `json:"Name"`
	Unit              string `json:"Unit"`
	StorageResolution uint   `json:"StorageResolution,omitempty"`
}

type CloudWatchMetric struct {
	Dimensions [][]string `json:"Dimensions"`
	Metrics    []Metric   `json:"Metrics"`
}

type AWSMetadata struct {
	Timestamp         uint               `json:"Timestamp"`
	CloudWatchMetrics []CloudWatchMetric `json:"CloudWatchMetrics"`
}

type EMFEvent struct {
	AWS         AWSMetadata            `json:"_aws"`
	OtherFields map[string]interface{} `json:",inline"`
}

func (f CustomWriter) Write(bytes []byte) (int, error) {
	return fmt.Print("[" + time.Now().UTC().Format("2006/01/02 15:04:05") + "] " + string(bytes))
}

func NewMockCloudWatchLogs() *MockCloudWatchLogs {
	return &MockCloudWatchLogs{
		logGroups:  make(map[string]map[string]*LogStream),
		tokens:     make(map[string]int),
		errorCount: 0,
	}
}

// AWS v2 SDK expects responses to include these fields
type AWSResponse struct {
	ResultWrapper interface{} `json:""`
}

func (m *MockCloudWatchLogs) handleCreateLogStream(w http.ResponseWriter, r *http.Request) {
	// Check for required AWS headers
	if !m.validateAWSHeaders(w, r) {
		return
	}

	var req CreateLogStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.sendErrorResponse(w, "InvalidParameterException", err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.logGroups[req.LogGroupName]; !exists {
		m.logGroups[req.LogGroupName] = make(map[string]*LogStream)
	}

	if _, exists := m.logGroups[req.LogGroupName][req.LogStreamName]; exists {
		m.sendErrorResponse(w, "ResourceAlreadyExistsException",
			fmt.Sprintf("Log stream %s already exists", req.LogStreamName),
			http.StatusBadRequest)
		return
	}

	m.logGroups[req.LogGroupName][req.LogStreamName] = &LogStream{
		LogStreamName: req.LogStreamName,
		CreationTime:  time.Now(),
		Events:        make([]LogEvent, 0),
	}

	// Return AWS-formatted response
	resp := AWSResponse{
		ResultWrapper: struct{}{},
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func validateEvent(event EMFEvent) error {
	expectedDimensions := make(map[string]struct{})
	expectedMetrics := make(map[string]struct{})

	for _, metric := range event.AWS.CloudWatchMetrics {
		for _, dimension := range metric.Dimensions {
			for _, value := range dimension {
				expectedDimensions[value] = struct{}{}
			}
		}
		for _, metric := range metric.Metrics {
			expectedMetrics[metric.Name] = struct{}{}
		}
	}

	if len(expectedMetrics) == 0 {
		return fmt.Errorf("no metrics found")
	}

	for key := range expectedMetrics {
		if _, exists := event.OtherFields[key]; !exists {
			return fmt.Errorf("missing metric %s", key)
		}
	}
	for key := range expectedDimensions {
		if _, exists := event.OtherFields[key]; !exists {
			return fmt.Errorf("missing dimension %s", key)
		}
	}
	return nil
}

func (m *MockCloudWatchLogs) handlePutLogEvents(w http.ResponseWriter, r *http.Request) {
	if !m.validateAWSHeaders(w, r) {
		return
	}

	var req PutLogEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.sendErrorResponse(w, "InvalidParameterException", err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	logGroup, exists := m.logGroups[req.LogGroupName]
	if !exists {
		m.sendErrorResponse(w, "ResourceNotFoundException",
			fmt.Sprintf("Log group %s does not exist", req.LogGroupName),
			http.StatusBadRequest)
		return
	}

	stream, exists := logGroup[req.LogStreamName]
	if !exists {
		m.sendErrorResponse(w, "ResourceNotFoundException",
			fmt.Sprintf("Log stream %s does not exist", req.LogStreamName),
			http.StatusBadRequest)
		return
	}

	streamKey := fmt.Sprintf("%s:%s", req.LogGroupName, req.LogStreamName)
	stream.Events = append(stream.Events, req.LogEvents...)
	rawEvents, err := json.Marshal(req.LogEvents)

	for _, event := range req.LogEvents {
		var emfEvent EMFEvent
		if err := json.Unmarshal([]byte(event.Message), &emfEvent); err != nil {
			m.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
			return
		}
		if err := validateEvent(emfEvent); err != nil {
			m.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
			return
		}
	}

	log.Println("[ info] All events passed validation")

	if err != nil {
		m.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[ info] Wrote %d events with a total of %d Bytes", len(req.LogEvents), len(rawEvents))
	m.tokens[streamKey]++

	resp := AWSResponse{
		ResultWrapper: PutLogEventsResponse{
			NextSequenceToken: fmt.Sprintf("token-%d", m.tokens[streamKey]),
		},
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	json.NewEncoder(w).Encode(resp)
}

func (m *MockCloudWatchLogs) validateAWSHeaders(w http.ResponseWriter, r *http.Request) bool {
	// Check for required AWS headers
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		m.sendErrorResponse(w, "MissingHeaderException", "Missing X-Amz-Target header", http.StatusBadRequest)
		return false
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/x-amz-json-1.1" {
		m.sendErrorResponse(w, "InvalidHeaderException", "Invalid Content-Type", http.StatusBadRequest)
		return false
	}

	return true
}

func (m *MockCloudWatchLogs) sendErrorResponse(w http.ResponseWriter, code, message string, status int) {
	m.mu.Lock()
	m.errorCount++
	m.mu.Unlock()

	log.Printf("[error] %s: %s", code, message)

	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Code:    code,
		Message: message,
	})
}

func parseAuthHeader(header []string) map[string]string {
	if len(header) == 0 {
		return nil
	}

	auth := make(map[string]string)
	for _, v := range header {
		initParts := strings.Split(v, " ")
		for _, part := range initParts {
			parts := strings.Split(part, "=")
			if len(parts) == 1 {
				auth[parts[0]] = ""
			} else if len(parts) > 2 {
				continue
			} else {
				auth[parts[0]] = strings.Trim(parts[1], "\"")
			}
		}
	}
	return auth
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(CustomWriter))
	mock := NewMockCloudWatchLogs()
	port := ":" + os.Getenv("PORT")

	mux := http.NewServeMux()

	// Create a channel to handle shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	// Update paths to match AWS SDK v2 expectations
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		auth := parseAuthHeader(r.Header["Authorization"])
		if auth == nil {
			mock.sendErrorResponse(w, "MissingHeaderException", "Missing Authorization header", http.StatusBadRequest)
			return
		} else if auth["Signature"] == "" {
			mock.sendErrorResponse(w, "MissingHeaderException", "Missing Signature header", http.StatusBadRequest)
			return
		}
		target := r.Header.Get("X-Amz-Target")
		switch target {
		case "Logs_20140328.CreateLogStream":
			mock.handleCreateLogStream(w, r)
		case "Logs_20140328.PutLogEvents":
			mock.handlePutLogEvents(w, r)
		default:
			log.Printf("404 Not Found: %s %s (Target: %s)", r.Method, r.URL.Path, target)
			mock.sendErrorResponse(w, "UnknownOperationException", "Unknown operation", http.StatusNotFound)
		}
	})

	// Start server in a goroutine
	go func() {
		log.Printf("Starting mock CloudWatch Logs server on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[error] Server error: %v", err)
			mock.errorCount++
		}
	}()

	// Wait for interrupt signal
	<-stop
	log.Println("Shutting down server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("[error] Server shutdown error: %v", err)
		mock.errorCount++
	}

	// Exit with status code 1 if any errors occurred
	if mock.errorCount > 0 {
		log.Printf("[error] Server encountered %d errors during operation", mock.errorCount)
		os.Exit(1)
	}

}
