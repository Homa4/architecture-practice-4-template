package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	serverSet := make(map[string]bool)

	for i := 0; i < 20; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}

		lbFrom := resp.Header.Get("lb")
		if lbFrom == "" {
			t.Fatalf("Missing lb-from header in response")
		}
		serverSet[lbFrom] = true
		resp.Body.Close()
	}

	if len(serverSet) < 2 {
		t.Errorf("Expected responses from at least 2 servers, got: %v", serverSet)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}
}
