package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen   string          `yaml:"listen"`
	Admin    string          `yaml:"admin"`
	Clusters []ClusterConfig `yaml:"clusters"`
	Routes   []RouteConfig   `yaml:"routes"`
}

type ClusterConfig struct {
	Name        string             `yaml:"name"`
	Endpoints   []string           `yaml:"endpoints"`
	Picker      string             `yaml:"picker"` // round_robin | least_request_p2c | p2c_ewma
	HealthCheck *HealthCheckConfig `yaml:"health_check,omitempty"`
	Outlier     *OutlierConfig     `yaml:"outlier,omitempty"`
	Retry       *RetryConfig       `yaml:"retry,omitempty"`
	Circuit     *CircuitConfig     `yaml:"circuit,omitempty"`
}

type HealthCheckConfig struct {
	Path               string        `yaml:"path"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	HealthyThreshold   int           `yaml:"healthy_threshold"`
	UnhealthyThreshold int           `yaml:"unhealthy_threshold"`
	Jitter             time.Duration `yaml:"jitter"`
}

type OutlierConfig struct {
	Consecutive5xx            int           `yaml:"consecutive_5xx"`
	ConsecutiveGatewayFailure int           `yaml:"consecutive_gateway_failure"`
	BaseEjectionTime          time.Duration `yaml:"base_ejection_time"`
	MaxEjectionTime           time.Duration `yaml:"max_ejection_time"`
	MaxEjectionPercent        int           `yaml:"max_ejection_percent"`
	Interval                  time.Duration `yaml:"interval"`
}

type RetryConfig struct {
	MaxAttempts      int           `yaml:"max_attempts"`
	PerTryTimeout    time.Duration `yaml:"per_try_timeout"`
	RetryOn          []string      `yaml:"retry_on"` // connect-failure, reset, 502, 503, 504
	BudgetPercent    int           `yaml:"budget_percent"`
	MinRetriesPerSec int           `yaml:"min_retries_per_sec"`
	BackoffBase      time.Duration `yaml:"backoff_base"`
	BackoffMax       time.Duration `yaml:"backoff_max"`
}

type CircuitConfig struct {
	MaxActiveRequests int `yaml:"max_active_requests"`
	MaxPending        int `yaml:"max_pending"`
}

type RouteConfig struct {
	PathPrefix string `yaml:"path_prefix"`
	Cluster    string `yaml:"cluster"`
}

func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	return ParseAndValidate(data)
}

func ParseAndValidate(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if strings.TrimSpace(c.Listen) == "" {
		return fmt.Errorf("missing listen address")
	}
	if strings.TrimSpace(c.Admin) == "" {
		return fmt.Errorf("missing admin address")
	}
	if len(c.Clusters) == 0 {
		return fmt.Errorf("at least one cluster must be configured")
	}

	clusterNames := make(map[string]struct{})
	for i, cl := range c.Clusters {
		if strings.TrimSpace(cl.Name) == "" {
			return fmt.Errorf("clusters[%d]: cluster name cannot be empty", i)
		}
		if _, exists := clusterNames[cl.Name]; exists {
			return fmt.Errorf("duplicate cluster name: %q", cl.Name)
		}
		clusterNames[cl.Name] = struct{}{}

		if len(cl.Endpoints) == 0 {
			return fmt.Errorf("clusters[%s]: at least one endpoint is required", cl.Name)
		}
		for j, ep := range cl.Endpoints {
			if strings.TrimSpace(ep) == "" {
				return fmt.Errorf("clusters[%s].endpoints[%d]: endpoint cannot be empty", cl.Name, j)
			}
		}

		switch cl.Picker {
		case "", "round_robin", "least_request_p2c", "p2c_ewma":
		default:
			return fmt.Errorf("clusters[%s]: unknown picker %q (must be round_robin, least_request_p2c, or p2c_ewma)", cl.Name, cl.Picker)
		}

		if cl.HealthCheck != nil {
			if cl.HealthCheck.Path == "" || !strings.HasPrefix(cl.HealthCheck.Path, "/") {
				return fmt.Errorf("clusters[%s].health_check: path must start with '/'", cl.Name)
			}
			if cl.HealthCheck.Interval <= 0 {
				return fmt.Errorf("clusters[%s].health_check: interval must be > 0", cl.Name)
			}
			if cl.HealthCheck.Timeout <= 0 {
				return fmt.Errorf("clusters[%s].health_check: timeout must be > 0", cl.Name)
			}
			if cl.HealthCheck.HealthyThreshold < 1 {
				return fmt.Errorf("clusters[%s].health_check: healthy_threshold must be >= 1", cl.Name)
			}
			if cl.HealthCheck.UnhealthyThreshold < 1 {
				return fmt.Errorf("clusters[%s].health_check: unhealthy_threshold must be >= 1", cl.Name)
			}
		}

		if cl.Outlier != nil {
			if cl.Outlier.MaxEjectionPercent < 0 || cl.Outlier.MaxEjectionPercent > 100 {
				return fmt.Errorf("clusters[%s].outlier: max_ejection_percent must be between 0 and 100", cl.Name)
			}
			if cl.Outlier.BaseEjectionTime <= 0 {
				return fmt.Errorf("clusters[%s].outlier: base_ejection_time must be > 0", cl.Name)
			}
			if cl.Outlier.MaxEjectionTime < cl.Outlier.BaseEjectionTime {
				return fmt.Errorf("clusters[%s].outlier: max_ejection_time must be >= base_ejection_time", cl.Name)
			}
		}

		if cl.Retry != nil {
			if cl.Retry.MaxAttempts < 1 {
				return fmt.Errorf("clusters[%s].retry: max_attempts must be >= 1", cl.Name)
			}
			if cl.Retry.BudgetPercent < 0 || cl.Retry.BudgetPercent > 100 {
				return fmt.Errorf("clusters[%s].retry: budget_percent must be between 0 and 100", cl.Name)
			}
		}

		if cl.Circuit != nil {
			if cl.Circuit.MaxActiveRequests < 0 {
				return fmt.Errorf("clusters[%s].circuit: max_active_requests must be >= 0", cl.Name)
			}
			if cl.Circuit.MaxPending < 0 {
				return fmt.Errorf("clusters[%s].circuit: max_pending must be >= 0", cl.Name)
			}
		}
	}

	if len(c.Routes) == 0 {
		return fmt.Errorf("at least one route must be configured")
	}
	for i, r := range c.Routes {
		if strings.TrimSpace(r.PathPrefix) == "" || !strings.HasPrefix(r.PathPrefix, "/") {
			return fmt.Errorf("routes[%d]: path_prefix must start with '/'", i)
		}
		if _, exists := clusterNames[r.Cluster]; !exists {
			return fmt.Errorf("routes[%d]: references unknown cluster %q", i, r.Cluster)
		}
	}

	return nil
}
