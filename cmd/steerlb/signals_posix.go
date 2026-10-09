//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func notifyReload(c chan<- os.Signal) {
	signal.Notify(c, syscall.SIGHUP)
}

func notifyShutdown(c chan<- os.Signal) {
	signal.Notify(c, syscall.SIGTERM, os.Interrupt)
}

func isReloadSignal(sig os.Signal) bool {
	return sig == syscall.SIGHUP
}
