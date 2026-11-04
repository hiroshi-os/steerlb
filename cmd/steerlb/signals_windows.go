//go:build windows

package main

import (
	"os"
	"os/signal"
)

func notifyReload(_ chan<- os.Signal) {
	// SIGHUP is not supported natively on Windows
}

func notifyShutdown(c chan<- os.Signal) {
	signal.Notify(c, os.Interrupt)
}

func isReloadSignal(_ os.Signal) bool {
	return false
}
