package config

import (
	"testing"
	"time"
)

func TestParseAndValidate_Valid(t *testing.T) {
	yamlData := `
listen: ":8080"
admin: ":9901"
clusters:
  - name: api
    endpoints: ["127.0.0.1:9001", "127.0.0.1:9002"]
    picker: p2c_ewma
    health_check:
      path: /healthz
      interval: 2s
      timeout: 500ms
      healthy_threshold: 2
      unhealthy_threshold: 3
      jitter: 200ms
    outlier:
      consecutive_5xx: 5
      consecutive_gateway_failure: 3
      base_ejection_time: 5s
      max_ejection_time: 60s
      max_ejection_percent: 50
      interval: 1s
    retry:
      max_attempts: 3
      per_try_timeout: 300ms
      retry_on: [connect-failure, reset, "502", "503", "504"]
      budget_percent: 20
      min_retries_per_sec: 3
      backoff_base: 10ms
      backoff_max: 100ms
    circuit:
      max_active_requests: 1024
      max_pending: 256
routes:
  - path_prefix: /
    cluster: api
`
	cfg, err := ParseAndValidate([]byte(yamlData))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Listen != ":8080" {
		t.Errorf("got listen=%q, want :8080", cfg.Listen)
	}
	if len(cfg.Clusters) != 1 || cfg.Clusters[0].Name != "api" {
		t.Errorf("invalid clusters: %+v", cfg.Clusters)
	}
	if cfg.Clusters[0].HealthCheck.Interval != 2*time.Second {
		t.Errorf("got interval=%v, want 2s", cfg.Clusters[0].HealthCheck.Interval)
	}
}

func TestParseAndValidate_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "missing listen",
			yaml: `
admin: ":9901"
clusters: [{name: api, endpoints: ["127.0.0.1:9001"]}]
routes: [{path_prefix: /, cluster: api}]
`,
		},
		{
			name: "no clusters",
			yaml: `
listen: ":8080"
admin: ":9901"
routes: [{path_prefix: /, cluster: api}]
`,
		},
		{
			name: "duplicate cluster name",
			yaml: `
listen: ":8080"
admin: ":9901"
clusters:
  - name: api
    endpoints: ["127.0.0.1:9001"]
  - name: api
    endpoints: ["127.0.0.1:9002"]
routes: [{path_prefix: /, cluster: api}]
`,
		},
		{
			name: "invalid picker",
			yaml: `
listen: ":8080"
admin: ":9901"
clusters:
  - name: api
    endpoints: ["127.0.0.1:9001"]
    picker: magic_picker
routes: [{path_prefix: /, cluster: api}]
`,
		},
		{
			name: "route references unknown cluster",
			yaml: `
listen: ":8080"
admin: ":9901"
clusters:
  - name: api
    endpoints: ["127.0.0.1:9001"]
routes: [{path_prefix: /, cluster: nonexistent}]
`,
		},
		{
			name: "invalid health check path",
			yaml: `
listen: ":8080"
admin: ":9901"
clusters:
  - name: api
    endpoints: ["127.0.0.1:9001"]
    health_check: {path: healthz, interval: 2s, timeout: 500ms, healthy_threshold: 1, unhealthy_threshold: 1}
routes: [{path_prefix: /, cluster: api}]
`,
		},
		{
			name: "max ejection time < base ejection time",
			yaml: `
listen: ":8080"
admin: ":9901"
clusters:
  - name: api
    endpoints: ["127.0.0.1:9001"]
    outlier: {base_ejection_time: 10s, max_ejection_time: 5s, max_ejection_percent: 50}
routes: [{path_prefix: /, cluster: api}]
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseAndValidate([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("expected validation error for %s, got nil", tt.name)
			}
		})
	}
}
