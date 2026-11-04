package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hiroshi-os/steerlb/internal/config"
	"github.com/hiroshi-os/steerlb/internal/metrics"
	"github.com/hiroshi-os/steerlb/internal/proxy"
)

func main() {
	configPath := flag.String("config", "steerlb.yaml", "Path to configuration file")
	drainTimeout := flag.Duration("drain-timeout", 15*time.Second, "Drain timeout for graceful shutdown")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("initializing steerlb", "config", *configPath)

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	m := metrics.NewMetrics()
	rt, err := proxy.NewRuntime(cfg, m, nil, time.Now().UnixNano())
	if err != nil {
		slog.Error("failed to create runtime", "error", err)
		os.Exit(1)
	}

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	rt.Start(rootCtx)

	p := proxy.NewProxy(rt, m, nil, time.Now().UnixNano())

	var reloadMu sync.Mutex
	reload := func() error {
		reloadMu.Lock()
		defer reloadMu.Unlock()

		slog.Info("reloading configuration", "path", *configPath)
		newCfg, err := config.LoadFile(*configPath)
		if err != nil {
			slog.Error("reload rejected: invalid config", "error", err)
			return fmt.Errorf("invalid config: %w", err)
		}

		newRt, err := proxy.NewRuntime(newCfg, m, nil, time.Now().UnixNano())
		if err != nil {
			slog.Error("reload rejected: runtime creation failed", "error", err)
			return fmt.Errorf("runtime error: %w", err)
		}

		newRt.Start(rootCtx)
		oldRt := p.SwapRuntime(newRt)

		// Stop health checks and tickers on old runtime
		if oldRt != nil {
			go func() {
				time.Sleep(5 * time.Second) // allow inflight to clear
				oldRt.Stop()
			}()
		}

		slog.Info("configuration reloaded successfully")
		return nil
	}

	mainServer := &http.Server{
		Addr:    cfg.Listen,
		Handler: p,
	}

	shutdownChan := make(chan struct{})
	drainFn := func() {
		select {
		case shutdownChan <- struct{}{}:
		default:
		}
	}

	adminHandler := setupAdminHandler(p, m, reload, drainFn)
	adminServer := &http.Server{
		Addr:    cfg.Admin,
		Handler: adminHandler,
	}

	// Start Admin server
	go func() {
		slog.Info("admin server listening", "addr", cfg.Admin)
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("admin server failed", "error", err)
		}
	}()

	// Start Proxy server
	go func() {
		slog.Info("proxy server listening", "addr", cfg.Listen)
		if err := mainServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("proxy server failed", "error", err)
		}
	}()

	// Signal channels
	reloadSignalChan := make(chan os.Signal, 1)
	notifyReload(reloadSignalChan)

	shutdownSignalChan := make(chan os.Signal, 1)
	notifyShutdown(shutdownSignalChan)

	go func() {
		for sig := range reloadSignalChan {
			if isReloadSignal(sig) {
				slog.Info("received SIGHUP signal, triggering reload")
				_ = reload()
			}
		}
	}()

	select {
	case sig := <-shutdownSignalChan:
		slog.Info("received termination signal", "signal", sig.String())
	case <-shutdownChan:
		slog.Info("drain triggered via admin API")
	}

	// Graceful shutdown sequence
	slog.Info("starting graceful drain", "timeout", *drainTimeout)

	// 1. Stop accepting new connections on listener
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = mainServer.Shutdown(shutdownCtx)
	shutdownCancel()

	// 2. Drain in-flight requests
	drained := p.Drain(*drainTimeout)
	if drained {
		slog.Info("all in-flight requests drained successfully")
	} else {
		slog.Warn("drain timeout exceeded, forcing exit")
	}

	// 3. Stop runtime
	currRt := p.GetRuntime()
	if currRt != nil {
		currRt.Stop()
	}

	// 4. Close admin server
	adminCtx, adminCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = adminServer.Shutdown(adminCtx)
	adminCancel()

	slog.Info("steerlb terminated cleanly")
}
