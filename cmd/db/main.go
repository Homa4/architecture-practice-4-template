package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"strings"

	"github.com/Homa4/architecture-practice-4-template/datastore"
)

type DBResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DBRequest struct {
	Value string `json:"value"`
}

type DBServer struct {
	db *datastore.Db
}

func NewDBServer(dbPath string) (*DBServer, error) {
	db, err := datastore.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &DBServer{db: db}, nil
}

func (s *DBServer) handleGet(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/db/")
	if path == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	key := path

	value, err := s.db.Get(key)
	if err != nil {
		if err == datastore.ErrNotFound {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
		return
	}

	response := DBResponse{
		Key:   key,
		Value: value,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
}

func (s *DBServer) handlePost(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/db/")
	if path == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	key := path

	var req DBRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if err := s.db.Put(key, req.Value); err != nil {
		http.Error(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *DBServer) handleDB(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodPost:
		s.handlePost(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *DBServer) Close() error {
	return s.db.Close()
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./db-data"
	}

	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalf("Failed to create database directory: %v", err)
	}

	dbServer, err := NewDBServer(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database server: %v", err)
	}
	defer dbServer.Close()

	http.HandleFunc("/db/", dbServer.handleDB)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Printf("Database server starting on port %s", port)
	log.Printf("Database path: %s", dbPath)
	
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}