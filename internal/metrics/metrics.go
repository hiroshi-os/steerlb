package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Registry *prometheus.Registry

	RequestsTotal         *prometheus.CounterVec
	RequestDuration       prometheus.Histogram
	UpstreamDuration      *prometheus.HistogramVec
	RetriesTotal          prometheus.Counter
	RetryBudgetExhausted  prometheus.Counter
	OutlierEjectionsTotal prometheus.Counter
	OutlierSkippedTotal   prometheus.Counter
	OutlierEjected        *prometheus.GaugeVec
	HealthStatus          *prometheus.GaugeVec
	PanicMode             *prometheus.GaugeVec
	InflightRequests      *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,

		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "steer_requests_total",
				Help: "Total number of HTTP requests processed by steerlb.",
			},
			[]string{"cluster", "endpoint", "code"},
		),

		RequestDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "steer_request_duration_seconds",
				Help:    "Client-visible request duration in seconds.",
				Buckets: prometheus.DefBuckets,
			},
		),

		UpstreamDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "steer_upstream_duration_seconds",
				Help:    "Duration of individual upstream requests in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"endpoint"},
		),

		RetriesTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "steer_retries_total",
				Help: "Total number of retry attempts made.",
			},
		),

		RetryBudgetExhausted: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "steer_retry_budget_exhausted_total",
				Help: "Total number of times a retry was rejected due to budget exhaustion.",
			},
		),

		OutlierEjectionsTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "steer_outlier_ejections_total",
				Help: "Total number of outlier ejections.",
			},
		),

		OutlierSkippedTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "outlier_ejections_skipped_total",
				Help: "Total number of outlier ejections skipped due to max ejection percent cap.",
			},
		),

		OutlierEjected: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "steer_outlier_ejected",
				Help: "Current outlier ejection state per endpoint (1 if ejected, 0 otherwise).",
			},
			[]string{"endpoint"},
		),

		HealthStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "steer_health",
				Help: "Current active health check status per endpoint (1 if healthy, 0 otherwise).",
			},
			[]string{"endpoint"},
		),

		PanicMode: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "steer_panic_mode",
				Help: "Cluster panic mode status (1 if all endpoints are unhealthy, 0 otherwise).",
			},
			[]string{"cluster"},
		),

		InflightRequests: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "steer_inflight",
				Help: "Current in-flight requests per endpoint.",
			},
			[]string{"endpoint"},
		),
	}

	reg.MustRegister(
		m.RequestsTotal,
		m.RequestDuration,
		m.UpstreamDuration,
		m.RetriesTotal,
		m.RetryBudgetExhausted,
		m.OutlierEjectionsTotal,
		m.OutlierSkippedTotal,
		m.OutlierEjected,
		m.HealthStatus,
		m.PanicMode,
		m.InflightRequests,
	)

	return m
}
