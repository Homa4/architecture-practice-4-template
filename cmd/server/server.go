package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Homa4/architecture-practice-4-template/httptools"
	"github.com/Homa4/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const confTeamName = "CONF_TEAM_NAME"

type DBRequest struct {
	Value string `json:"value"`
}

type DBResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DBClient struct {
	baseURL string
	client  *http.Client
}

func NewDBClient(baseURL string) *DBClient {
	return &DBClient{
		baseURL: baseURL,
		client:  http.DefaultClient,
	}
}

func (db *DBClient) Put(key, value string) error {
	url := fmt.Sprintf("%s/db/%s", db.baseURL, key)
	
	reqBody := DBRequest{Value: value}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := db.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("database request failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (db *DBClient) Get(key string) (string, error) {
	url := fmt.Sprintf("%s/db/%s", db.baseURL, key)
	
	resp, err := db.client.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("database request failed with status: %d", resp.StatusCode)
	}

	var dbResp DBResponse
	if err := json.NewDecoder(resp.Body).Decode(&dbResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	return dbResp.Value, nil
}

func main() {
	dbServiceURL := os.Getenv("DB_SERVICE_URL")
	if dbServiceURL == "" {
		dbServiceURL = "http://localhost:8083"
	}

	dbClient := NewDBClient(dbServiceURL)

	teamName := os.Getenv(confTeamName)
	if teamName == "" {
		teamName = "default-team"
	}

	currentDate := time.Now().Format("2006-01-02")
	if err := dbClient.Put(teamName, currentDate); err != nil {
		log.Printf("Warning: failed to initialize team data in database: %v", err)
	} else {
		log.Printf("Initialized team '%s' with date: %s", teamName, currentDate)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "Missing key parameter", http.StatusBadRequest)
			return
		}

		value, err := dbClient.Get(key)
		if err != nil {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		
		response := map[string]interface{}{
			"key":   key,
			"value": value,
		}
		_ = json.NewEncoder(rw).Encode(response)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}