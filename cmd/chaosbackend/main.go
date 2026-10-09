package main

import (
	"flag"
	"fmt"
	"log/slog"
)

func main() {
	addr := flag.String("addr", ":9001", "Listen address")
	flag.Parse()

	slog.Info("starting chaosbackend", "addr", *addr)
	fmt.Println("chaosbackend running on", *addr)
}
