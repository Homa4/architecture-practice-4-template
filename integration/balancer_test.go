package integration

import (
	"fmt"
	"io"
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
	var responses []string

	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Errorf("request %d failed: %s", i, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("request %d: expected status 200, got %d", i, resp.StatusCode)
			continue
		}

		server := resp.Header.Get("lb-from")
		if server == "" {
			t.Errorf("request %d: missing lb-from header", i)
			continue
		}
		count[server]++

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("request %d: failed to read response body: %s", i, err)
			continue
		}

		response := string(body)
		responses = append(responses, response)

		if len(response) == 0 {
			t.Errorf("request %d: empty response from server %s", i, server)
		}

		t.Logf("Request %d: server=%s, response_length=%d", i, server, len(response))
	}

	if len(count) < 2 {
		t.Errorf("Expected requests to be distributed to at least 2 servers, got: %+v", count)
	}

	for server, requestCount := range count {
		if requestCount == 0 {
			t.Errorf("Server %s received no requests", server)
		}
	}

	t.Logf("Load distribution: %+v", count)
	t.Logf("Total unique responses: %d", len(responses))
}

func TestBalancerHealthCheck(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	resp, err := client.Get(fmt.Sprintf("%s/health", baseAddress))
	if err != nil {
		t.Fatalf("Health check failed: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected health check status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read health check response: %s", err)
	}

	t.Logf("Health check response: %s", string(body))
}

func TestBalancerFailover(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	successCount := 0
	failureCount := 0
	serverCounts := make(map[string]int)

	for i := 0; i < 20; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			failureCount++
			t.Logf("Request %d failed: %s", i, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			successCount++
			server := resp.Header.Get("lb-from")
			if server != "" {
				serverCounts[server]++
			}
		} else {
			failureCount++
			t.Logf("Request %d: non-200 status: %d", i, resp.StatusCode)
		}
	}

	if successCount == 0 {
		t.Error("No successful requests - balancer or servers might be down")
	}

	successRate := float64(successCount) / float64(successCount+failureCount) * 100
	t.Logf("Success rate: %.2f%% (%d/%d)", successRate, successCount, successCount+failureCount)
	t.Logf("Server distribution: %+v", serverCounts)

	if successRate < 80.0 {
		t.Errorf("Success rate too low: %.2f%%, expected at least 80%%", successRate)
	}
}

func BenchmarkBalancer(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
			continue
		}

		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func BenchmarkBalancerParallel(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
			if err != nil {
				b.Error(err)
				continue
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}
