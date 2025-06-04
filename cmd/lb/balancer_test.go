package main

import "testing"

type trafficStat struct {
	Traffic int
	Healthy bool
}

func selectBestServer(trafficMap map[string]*trafficStat) string {
	bestServer := ""
	minTraffic := -1
	for server, stat := range trafficMap {
		if !stat.Healthy {
			continue
		}
		if minTraffic == -1 || stat.Traffic < minTraffic {
			minTraffic = stat.Traffic
			bestServer = server
		}
	}
	return bestServer
}

func TestBalancer_SelectBestServer(t *testing.T) {
	trafficMap := map[string]*trafficStat{
		"server1:8080": {Traffic: 500, Healthy: true},
		"server2:8080": {Traffic: 200, Healthy: true},
		"server3:8080": {Traffic: 300, Healthy: false},
	}

	best := selectBestServer(trafficMap)
	expected := "server2:8080"

	if best != expected {
		t.Errorf("Expected best server to be %s, got %s", expected, best)
	}
}

func TestBalancer_AllServersDead(t *testing.T) {
	trafficMap := map[string]*trafficStat{
		"server1:8080": {Traffic: 500, Healthy: false},
		"server2:8080": {Traffic: 200, Healthy: false},
	}
	best := selectBestServer(trafficMap)
	if best != "" {
		t.Errorf("Expected no server to be selected, got %s", best)
	}
}

func TestBalancer_SingleHealthy(t *testing.T) {
	trafficMap := map[string]*trafficStat{
		"server1:8080": {Traffic: 9999, Healthy: false},
		"server2:8080": {Traffic: 100, Healthy: true},
	}
	best := selectBestServer(trafficMap)
	expected := "server2:8080"

	if best != expected {
		t.Errorf("Expected server2:8080 to be selected, got %s", best)
	}
}
