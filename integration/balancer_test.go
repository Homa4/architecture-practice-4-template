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

	count := make(map[string]int)

	// Викликаємо балансувальник кілька разів
	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Errorf("request %d failed: %s", i, err)
			continue
		}
		server := resp.Header.Get("lb-from")
		if server == "" {
			t.Errorf("request %d: missing lb-from header", i)
		}
		count[server]++
		resp.Body.Close()
	}

	if len(count) < 2 {
		t.Errorf("Expected requests to be distributed to at least 2 servers, got: %+v", count)
	}

	t.Logf("Load distribution: %+v", count)
}


func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
			continue
		}
		resp.Body.Close()
	}
}

