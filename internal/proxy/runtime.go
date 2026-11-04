package proxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/health"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/outlier"
	"github.com/hiroshi-os/steerlb/internal/picker"
	"github.com/hiroshi-os/steerlb/internal/retry"
)

type ClusterRuntime struct {
	Name            string
	Endpoints       []*picker.Endpoint
	Picker          picker.Picker
	HealthChecker   *health.Checker
	OutlierDetector *outlier.Detector
	RetryPolicy     *retry.Policy
	Budget          *retry.Budget
	Circuit         *config.CircuitConfig
	ActiveRequests  atomic.Int64
	Transport       *http.Transport
}

func NewClusterRuntime(cl config.ClusterConfig, m *metrics.Metrics, clk picker.Clock, seed int64) (*ClusterRuntime, error) {
	if clk == nil {
		clk = picker.RealClock{}
	}
	endpoints := make([]*picker.Endpoint, len(cl.Endpoints))
	for i, addr := range cl.Endpoints {
		endpoints[i] = picker.NewEndpoint(addr, clk, 10*time.Second)
	}

	var p picker.Picker
	switch cl.Picker {
	case "round_robin":
		p = picker.NewRoundRobinPicker()
	case "least_request_p2c":
		p = picker.NewLeastRequestP2CPicker(seed)
	case "p2c_ewma", "":
		p = picker.NewP2CEWMAPicker(seed)
	default:
		return nil, fmt.Errorf("unknown picker: %s", cl.Picker)
	}

	var hc *health.Checker
	if cl.HealthCheck != nil {
		hc = health.NewChecker(cl.Name, cl.HealthCheck, endpoints, m, seed)
	}

	var od *outlier.Detector
	if cl.Outlier != nil {
		od = outlier.NewDetector(cl.Outlier, endpoints, m, clk)
	}

	var budget *retry.Budget
	var policy *retry.Policy
	if cl.Retry != nil {
		budget = retry.NewBudget(cl.Retry.BudgetPercent, cl.Retry.MinRetriesPerSec, m)
		policy = retry.NewPolicy(cl.Retry, budget, m)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0,
	}

	cr := &ClusterRuntime{
		Name:            cl.Name,
		Endpoints:       endpoints,
		Picker:          p,
		HealthChecker:   hc,
		OutlierDetector: od,
		RetryPolicy:     policy,
		Budget:          budget,
		Circuit:         cl.Circuit,
		Transport:       transport,
	}

	return cr, nil
}

func (cr *ClusterRuntime) EligibleEndpoints(attempted map[string]bool) []*picker.Endpoint {
	panicMode := false
	if cr.HealthChecker != nil {
		panicMode = cr.HealthChecker.InPanicMode()
	}

	var eligible []*picker.Endpoint
	var unattempted []*picker.Endpoint

	for _, ep := range cr.Endpoints {
		if ep.Ejected.Load() {
			continue
		}
		if panicMode || ep.Healthy.Load() {
			eligible = append(eligible, ep)
			if !attempted[ep.Addr] {
				unattempted = append(unattempted, ep)
			}
		}
	}

	// If there are unattempted eligible endpoints, prefer them
	if len(unattempted) > 0 {
		return unattempted
	}
	return eligible
}

func (cr *ClusterRuntime) Start(ctx context.Context) {
	if cr.HealthChecker != nil {
		cr.HealthChecker.Start(ctx)
	}
	if cr.OutlierDetector != nil {
		cr.OutlierDetector.Start(ctx)
	}
}

func (cr *ClusterRuntime) Stop() {
	if cr.HealthChecker != nil {
		cr.HealthChecker.Stop()
	}
	if cr.OutlierDetector != nil {
		cr.OutlierDetector.Stop()
	}
	cr.Transport.CloseIdleConnections()
}

type Runtime struct {
	Config   *config.Config
	Clusters map[string]*ClusterRuntime
	Routes   []config.RouteConfig
	Metrics  *metrics.Metrics
	cancel   context.CancelFunc
}

func NewRuntime(cfg *config.Config, m *metrics.Metrics, clk picker.Clock, seed int64) (*Runtime, error) {
	clusters := make(map[string]*ClusterRuntime)
	for _, cl := range cfg.Clusters {
		cr, err := NewClusterRuntime(cl, m, clk, seed)
		if err != nil {
			return nil, err
		}
		clusters[cl.Name] = cr
	}

	rt := &Runtime{
		Config:   cfg,
		Clusters: clusters,
		Routes:   cfg.Routes,
		Metrics:  m,
	}

	return rt, nil
}

func (r *Runtime) Start(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)
	for _, cr := range r.Clusters {
		cr.Start(ctx)
	}
}

func (r *Runtime) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	for _, cr := range r.Clusters {
		cr.Stop()
	}
}
