package main

import (
	"flag"
	"fmt"
	"time"
)

func main() {
	target := flag.String("target", "http://127.0.0.1:8080/", "Target URL")
	rps := flag.Int("rps", 1000, "Target requests per second")
	duration := flag.Duration("d", 10*time.Second, "Duration of the benchmark")
	concurrency := flag.Int("c", 128, "Max concurrent requests")
	seed := flag.Int64("seed", 1, "Random seed")
	flag.Parse()

	fmt.Printf("loadgen target=%s rps=%d d=%v c=%d seed=%d\n", *target, *rps, *duration, *concurrency, *seed)
}
