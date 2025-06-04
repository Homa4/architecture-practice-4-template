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
const confDBHost = "CONF_DB_HOST"

type APIResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DBRequest struct {
	Value string `json:"value"`
}

type DBResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func initializeTeamData() error {
	teamName := os.Getenv(confTeamName)
	if teamName == "" {
		teamName = "default-team"
	}

	dbHost := os.Getenv(confDBHost)
	if dbHost == "" {
		dbHost = "db:8083"
	}

	currentDate := time.Now().Format("2006-01-02")

	reqBody := DBRequest{Value: currentDate}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		url := fmt.Sprintf("http://%s/db/%s", dbHost, teamName)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Printf("Successfully initialized team data: %s = %s", teamName, currentDate)
				return nil
			}
		}

		log.Printf("Attempt %d/%d: Waiting for database to be ready...", i+1, maxRetries)
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("failed to initialize team data after %d attempts", maxRetries)
}

func getDataFromDB(key string) (*APIResponse, error) {
	dbHost := os.Getenv(confDBHost)
	if dbHost == "" {
		dbHost = "db:8083"
	}

	url := fmt.Sprintf("http://%s/db/%s", dbHost, key)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get from database: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("database returned status %d", resp.StatusCode)
	}

	var dbResp DBResponse
	if err := json.NewDecoder(resp.Body).Decode(&dbResp); err != nil {
		return nil, fmt.Errorf("failed to decode database response: %w", err)
	}

	return &APIResponse{
		Key:   dbResp.Key,
		Value: dbResp.Value,
	}, nil
}

func main() {
	flag.Parse()

	go func() {
		if err := initializeTeamData(); err != nil {
			log.Printf("Warning: Failed to initialize team data: %v", err)
		}
	}()

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
			http.Error(rw, "Key parameter is required", http.StatusBadRequest)
			return
		}

		data, err := getDataFromDB(key)
		if err != nil {
			log.Printf("Database error: %v", err)
			http.Error(rw, "Internal server error", http.StatusInternalServerError)
			return
		}

		if data == nil {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(rw).Encode(data); err != nil {
			log.Printf("Failed to encode response: %v", err)
		}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
