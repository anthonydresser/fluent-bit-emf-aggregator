package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
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
	mu        sync.RWMutex
	logGroups map[string]map[string]*LogStream // logGroup -> logStreamName -> stream
	tokens    map[string]int                   // stream -> sequence token
}

type CustomWriter struct{}

func (f CustomWriter) Write(bytes []byte) (int, error) {
	return fmt.Print("[" + time.Now().UTC().Format("2006/01/02 15:04:05") + "] " + string(bytes))
}

func NewMockCloudWatchLogs() *MockCloudWatchLogs {
	return &MockCloudWatchLogs{
		logGroups: make(map[string]map[string]*LogStream),
		tokens:    make(map[string]int),
	}
}

// AWS v2 SDK expects responses to include these fields
type AWSResponse struct {
	ResultWrapper interface{} `json:""`
}

func (m *MockCloudWatchLogs) handleCreateLogStream(w http.ResponseWriter, r *http.Request) {
	// Check for required AWS headers
	if !validateAWSHeaders(w, r) {
		return
	}

	var req CreateLogStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "InvalidParameterException", err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.logGroups[req.LogGroupName]; !exists {
		m.logGroups[req.LogGroupName] = make(map[string]*LogStream)
	}

	if _, exists := m.logGroups[req.LogGroupName][req.LogStreamName]; exists {
		sendErrorResponse(w, "ResourceAlreadyExistsException",
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

func (m *MockCloudWatchLogs) handlePutLogEvents(w http.ResponseWriter, r *http.Request) {
	if !validateAWSHeaders(w, r) {
		return
	}

	var req PutLogEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "InvalidParameterException", err.Error(), http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	logGroup, exists := m.logGroups[req.LogGroupName]
	if !exists {
		sendErrorResponse(w, "ResourceNotFoundException",
			fmt.Sprintf("Log group %s does not exist", req.LogGroupName),
			http.StatusBadRequest)
		return
	}

	stream, exists := logGroup[req.LogStreamName]
	if !exists {
		sendErrorResponse(w, "ResourceNotFoundException",
			fmt.Sprintf("Log stream %s does not exist", req.LogStreamName),
			http.StatusBadRequest)
		return
	}

	streamKey := fmt.Sprintf("%s:%s", req.LogGroupName, req.LogStreamName)
	stream.Events = append(stream.Events, req.LogEvents...)
	rawEvents, err := json.Marshal(req.LogEvents)
	if err != nil {
		sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
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

func validateAWSHeaders(w http.ResponseWriter, r *http.Request) bool {
	// Check for required AWS headers
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		sendErrorResponse(w, "MissingHeaderException", "Missing X-Amz-Target header", http.StatusBadRequest)
		return false
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/x-amz-json-1.1" {
		sendErrorResponse(w, "InvalidHeaderException", "Invalid Content-Type", http.StatusBadRequest)
		return false
	}

	return true
}

func sendErrorResponse(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{
		Code:    code,
		Message: message,
	})
}

func main() {
	log.SetFlags(0)
	log.SetOutput(new(CustomWriter))
	mock := NewMockCloudWatchLogs()
	port := ":" + os.Getenv("PORT")

	mux := http.NewServeMux()

	// Update paths to match AWS SDK v2 expectations
	// The actual endpoint paths are determined by the X-Amz-Target header
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		switch target {
		case "Logs_20140328.CreateLogStream":
			mock.handleCreateLogStream(w, r)
		case "Logs_20140328.PutLogEvents":
			mock.handlePutLogEvents(w, r)
		default:
			log.Printf("404 Not Found: %s %s (Target: %s)", r.Method, r.URL.Path, target)
			sendErrorResponse(w, "UnknownOperationException", "Unknown operation", http.StatusNotFound)
		}
	})

	log.Printf("Starting mock CloudWatch Logs server on port %s", port)
	log.Fatal(http.ListenAndServe(port, mux))
}
