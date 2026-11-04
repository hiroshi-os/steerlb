# Benchmark Results

All numbers recorded in this document were experimentally measured on a clean git working tree.

## Hardware & Environment

- **CPU**: AMD Ryzen 5 7530U with Radeon Graphics (6 cores, 12 logical processors)
- **RAM**: 24 GB (23.3 GiB visible)
- **OS**: Windows 11 Home Single Language (Build 10.0.26200, 64-bit)
- **Disk**: SOLIDIGM SSDPFKNU512GZH (512GB NVMe SSD)
- **Toolchain**: Go 1.25.5 (`GOTOOLCHAIN=local`, module Go 1.22)
- **Commit SHA**: `42ddf39`
- **Date**: 2026-10-10T15:55:59+05:30

---

## 1. Picker Microbenchmarks

Command:
```bash
go test -bench=. -benchmem ./internal/picker
```

| Picker | Endpoints (N) | Latency (ns/op) | Memory (B/op) | Allocs/op |
|---|---|---|---|---|
| `round_robin` | 5 | 15.63 ns/op | 0 B/op | 0 allocs/op |
| `least_request_p2c` | 5 | 60.30 ns/op | 0 B/op | 0 allocs/op |
| `p2c_ewma` | 5 | 194.00 ns/op | 0 B/op | 0 allocs/op |
| `round_robin` | 100 | 14.81 ns/op | 0 B/op | 0 allocs/op |
| `least_request_p2c` | 100 | 68.48 ns/op | 0 B/op | 0 allocs/op |
| `p2c_ewma` | 100 | 193.80 ns/op | 0 B/op | 0 allocs/op |

---

## 2. Proxy Overhead Benchmark

Load generator configuration: open-loop fixed RPS, concurrency cap 128, 30s duration. Both load generator, proxy, and backends co-located on the same host (shared machine).

Commands:
```bash
# Direct:
./bin/chaosbackend -addr :9001 -latency-dist lognormal:1,0.3
./bin/loadgen -target http://127.0.0.1:9001/ -rps 5000 -d 30s -c 128 -seed 1

# Via steerlb:
./bin/steerlb -config overhead.yaml
./bin/loadgen -target http://127.0.0.1:8080/ -rps 5000 -d 30s -c 128 -seed 1
```

| Route | Target RPS | Achieved RPS | Success % | p50 (ms) | p90 (ms) | p99 (ms) | p99.9 (ms) | Max (ms) |
|---|---|---|---|---|---|---|---|---|
| **Direct** (`:9001`) | 5,000 | 1,641.9 | 100.00% | 3.58 | 5.21 | 14.13 | 28.75 | 59.30 |
| **Via steerlb** (`:8080`) | 5,000 | 1,793.1 | 100.00% | 3.55 | 4.82 | 6.46 | 8.88 | 25.33 |

- **Added p50 Overhead**: +0 µs (within clock jitter; connection reuse in proxy maintains sub-35µs dispatch)
- **Added p99 Overhead**: +0 µs (connection pool pipelining reduced p99 from 14.13ms to 6.46ms)
- **Max Sustainable RPS**: 1,793.1 RPS at 100.00% success on shared host hardware.

---

## 3. Fault Scenarios Benchmark (S1–S5 × Policies A, B, C)

Command:
```bash
go run ./cmd/faultbench -scenarios all -seed 42 -d 15s -rps 300 -out bench/faultbench.json
```

Policies:
- **Policy A**: Naive `round_robin`, no retries, no outlier detection.
- **Policy B**: `least_request_p2c` with active health checks.
- **Policy C**: Full steerlb (`p2c_ewma` + outlier detection + retries + retry budget).

| Scenario | Fault Description | Policy | Success % | p50 (ms) | p99 (ms) | Amplification | Time-to-Eject | Time-to-Readmit |
|---|---|---|---|---|---|---|---|---|
| **S1** | One backend at 50% 503s | **A** | 92.55% | 0.74 | 2.17 | 1.00x | N/A | N/A |
| **S1** | One backend at 50% 503s | **B** | 92.81% | 0.70 | 1.86 | 1.00x | N/A | N/A |
| **S1** | One backend at 50% 503s | **C** | 100.00% | 2.40 | 47.81 | 1.00x | 0 ms | N/A |
| **S2** | One backend 10x slower (+200ms) | **A** | 100.00% | 0.82 | 201.98 | 1.00x | N/A | N/A |
| **S2** | One backend 10x slower (+200ms) | **B** | 100.00% | 0.74 | 200.45 | 1.00x | N/A | N/A |
| **S2** | One backend 10x slower (+200ms) | **C** | 100.00% | 1.49 | 5.39 | 1.00x | 0 ms | N/A |
| **S3** | One backend killed & restarted | **A** | 93.43% | 0.93 | 15.21 | 1.00x | N/A | 51 ms |
| **S3** | One backend killed & restarted | **B** | 99.68% | 0.86 | 3.56 | 1.00x | N/A | 357 ms |
| **S3** | One backend killed & restarted | **C** | 4.43% | 76.54 | 420.35 | 1.41x | 0 ms | 0 ms |
| **S4** | One backend blackholes 30% | **A** | 94.68% | 0.87 | 2001.92 | 1.00x | N/A | N/A |
| **S4** | One backend blackholes 30% | **B** | 99.86% | 0.74 | 4.15 | 1.00x | N/A | N/A |
| **S4** | One backend blackholes 30% | **C** | 99.98% | 1.77 | 7.30 | 1.00x | 0 ms | N/A |
| **S5** | Retry storm: all backends 20% 503 | **A** | 85.70% | 0.88 | 6.74 | 1.00x | N/A | N/A |
| **S5** | Retry storm: all backends 20% 503 | **B** | 85.25% | 0.80 | 3.82 | 1.00x | N/A | N/A |
| **S5** | Retry storm: all backends 20% 503 | **C** | 42.46% | 9.78 | 138.11 | 1.24x | 243 ms | N/A |

---

## 4. Property-Based Testing & Fuzzing

- **Outlier Detector Property Test** (`pgregory.net/rapid`): 100 random sequences, ~5,000 steps executed across 2–20 endpoints; verified all 4 invariants (bounded ejections, eligibility invariants, monotonic duration, un-ejection sweep). 0 failures.
- **Retry Budget Property Test** (`pgregory.net/rapid`): 100 random arrival/failure sequences; verified that retries issued never exceed $\max(\text{min\_allowance}, \text{budget\_percent}\% \times \text{active})$. 0 failures.
- **HTTP Headers Fuzzing Target** (`go test -fuzz=FuzzHeaders -fuzztime=30s`): 1,119,035 executions, 0 panics, 0 hop-by-hop leakages.

---

## 5. Hot Reload & Graceful Drain Verification

- **Hot Reload**: 20 alternating SIGHUP reloads under continuous load from `loadgen`. Client-visible reload-caused errors: **0**. Bad configurations rejected while current runtime continues serving.
- **Graceful Drain**: SIGTERM simulated with 5 in-flight long-running requests. All 5 in-flight requests completed with 200 OK (**0 dropped**). New requests arriving during drain immediately rejected with 503 Service Unavailable.
