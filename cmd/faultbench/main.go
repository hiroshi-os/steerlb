package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	scenarios := flag.String("scenarios", "all", "Scenarios to run (e.g. S1,S3 or all)")
	duration := flag.Duration("d", 60*time.Second, "Scenario duration")
	rps := flag.Int("rps", 2000, "Scenario target RPS")
	seed := flag.Int64("seed", 42, "Random seed")
	outFile := flag.String("out", "bench/faultbench.json", "Output JSON file")
	flag.Parse()

	fmt.Printf("faultbench scenarios=%s d=%v rps=%d seed=%d out=%s\n", *scenarios, *duration, *rps, *seed, *outFile)

	if *outFile != "" {
		dir := filepath.Dir(*outFile)
		_ = os.MkdirAll(dir, 0755)
		_ = os.WriteFile(*outFile, []byte(`{"status":"skeleton"}`), 0644)
	}
}
