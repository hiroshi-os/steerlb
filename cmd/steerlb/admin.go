package main

import (
	"encoding/json"
	"net/http"

	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/proxy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type EndpointStatus struct {
	Cluster       string  `json:"cluster"`
	Endpoint      string  `json:"endpoint"`
	Healthy       bool    `json:"healthy"`
	Ejected       bool    `json:"ejected"`
	Inflight      int64   `json:"inflight"`
	EwmaMs        float64 `json:"ewma_ms"`
	Cost          float64 `json:"cost"`
	TotalRequests uint64  `json:"total_requests"`
}

func setupAdminHandler(p *proxy.Proxy, m *metrics.Metrics, reloadFn func() error, drainFn func()) http.Handler {
	mux := http.NewServeMux()

	// 1. Prometheus /metrics
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))

	// 2. GET /admin/endpoints
	mux.HandleFunc("/admin/endpoints", func(w http.ResponseWriter, r *http.Request) {
		rt := p.GetRuntime()
		if rt == nil {
			http.Error(w, "runtime not initialized", http.StatusServiceUnavailable)
			return
		}

		var statuses []EndpointStatus
		for clusterName, cl := range rt.Clusters {
			for _, ep := range cl.Endpoints {
				statuses = append(statuses, EndpointStatus{
					Cluster:       clusterName,
					Endpoint:      ep.Addr,
					Healthy:       ep.Healthy.Load(),
					Ejected:       ep.Ejected.Load(),
					Inflight:      ep.Inflight.Load(),
					EwmaMs:        float64(ep.GetEWMA().Microseconds()) / 1000.0,
					Cost:          ep.GetCost(),
					TotalRequests: ep.TotalRequests.Load(),
				})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(statuses)
	})

	// 3. POST /admin/reload
	mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := reloadFn(); err != nil {
			http.Error(w, "reload failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"reloaded"}`))
	})

	// 4. POST /admin/drain
	mux.HandleFunc("/admin/drain", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		go drainFn()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"draining"}`))
	})

	return mux
}
