package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()

	type trafficStat struct {
		Traffic int
		Healthy bool
	}

	var trafficMap = make(map[string]*trafficStat)

	for _, server := range serversPool {
		trafficMap[server] = &trafficStat{Traffic: 0, Healthy: false}
	}

	// Періодично оновлюємо статистику
	for _, server := range serversPool {
		srv := server
		go func() {
			for range time.Tick(5 * time.Second) {
				healthy := health(srv)
				trafficMap[srv].Healthy = healthy
				if healthy {
					url := fmt.Sprintf("%s://%s/report", scheme(), srv)
					resp, err := http.Get(url)
					if err != nil {
						log.Printf("error getting report from %s: %s", srv, err)
						continue
					}
					var data map[string][]string
					if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
						log.Printf("error decoding report from %s: %s", srv, err)
						continue
					}
					resp.Body.Close()

					total := 0
					for _, val := range data["responses"] {
						n, err := strconv.Atoi(val)
						if err == nil {
							total += n
						}
					}
					// Підрізаємо до останніх 5, якщо потрібно
					if len(data["responses"]) > 5 {
						data["responses"] = data["responses"][len(data["responses"])-5:]
					}
					trafficMap[srv].Traffic = total
					if trafficMap[srv].Healthy {
						log.Printf("Updated traffic from %s: %d healthy", srv, total)
					} else {
						log.Printf("Updated traffic from %s: %d dead", srv, total)
					}
				}
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Вибір сервера з мінімальним трафіком
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

		if bestServer == "" {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}

		log.Printf("Forwarding request to %s (traffic: %d)", bestServer, minTraffic)
		forward(bestServer, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
