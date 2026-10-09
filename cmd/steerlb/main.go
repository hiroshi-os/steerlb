package main

import (
	"flag"
	"fmt"
	"log/slog"
)

func main() {
	configPath := flag.String("config", "steerlb.yaml", "Path to configuration file")
	flag.Parse()

	slog.Info("starting steerlb", "config", *configPath)
	fmt.Println("steerlb L7 load balancer")
}
