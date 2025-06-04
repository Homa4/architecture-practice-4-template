package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 10 * time.Second,
}

type APIResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func waitForBalancer(t *testing.T) {
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		resp, err := client.Get(baseAddress + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Logf("Balancer is ready after %d attempts", i+1)
				return
			}
		}

		t.Logf("Waiting for balancer to be ready... attempt %d/%d", i+1, maxRetries)
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("Balancer is not ready after %d attempts", maxRetries)
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	waitForBalancer(t)

	teamName := os.Getenv("TEAM_NAME")
	if teamName == "" {
		teamName = "choco_pie"
	}

	count := make(map[string]int)

	for i := 0; i < 10; i++ {
		url := fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName)
		resp, err := client.Get(url)
		if err != nil {
			t.Errorf("request %d failed: %s", i, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("request %d: expected status 200, got %d. Response: %s", i, resp.StatusCode, string(body))
			resp.Body.Close()
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("request %d: failed to read response body: %s", i, err)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		if len(body) == 0 {
			t.Errorf("request %d: received empty response body", i)
			continue
		}

		var apiResp APIResponse
		if err := json.Unmarshal(body, &apiResp); err != nil {
			t.Errorf("request %d: failed to parse JSON response: %s", i, err)
			continue
		}

		if apiResp.Key == "" || apiResp.Value == "" {
			t.Errorf("request %d: received empty key or value: key='%s', value='%s'", i, apiResp.Key, apiResp.Value)
			continue
		}

		if apiResp.Key != teamName {
			t.Errorf("request %d: expected key '%s', got '%s'", i, teamName, apiResp.Key)
			continue
		}

		server := resp.Header.Get("lb-from")
		if server == "" {
			t.Errorf("request %d: missing lb-from header", i)
		}
		count[server]++

		t.Logf("request %d: server=%s, key=%s, value=%s", i, server, apiResp.Key, apiResp.Value)
	}

	if len(count) < 2 {
		t.Errorf("Expected requests to be distributed to at least 2 servers, got: %+v", count)
	}

	t.Logf("Load distribution: %+v", count)
}

func TestBalancerWithNonExistentKey(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	waitForBalancer(t)

	url := fmt.Sprintf("%s/api/v1/some-data?key=nonexistent-key", baseAddress)
	resp, err := client.Get(url)
	if err != nil {
		t.Errorf("request failed: %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status 404 for nonexistent key, got %d. Response: %s", resp.StatusCode, string(body))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("failed to read response body: %s", err)
		return
	}

	if len(body) != 0 {
		t.Errorf("expected empty response body for 404, got: %s", string(body))
	}

	t.Logf("Successfully received 404 for nonexistent key")
}

func TestBalancerWithoutKeyParameter(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	waitForBalancer(t)

	url := fmt.Sprintf("%s/api/v1/some-data", baseAddress)
	resp, err := client.Get(url)
	if err != nil {
		t.Errorf("request failed: %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status 400 for missing key parameter, got %d. Response: %s", resp.StatusCode, string(body))
		return
	}

	t.Logf("Successfully received 400 for missing key parameter")
}

func BenchmarkBalancer(b *testing.B) {
	teamName := os.Getenv("TEAM_NAME")
	if teamName == "" {
		teamName = "choco_pie"
	}

	url := fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName)

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(url)
		if err != nil {
			b.Error(err)
			continue
		}
		resp.Body.Close()
	}
}
