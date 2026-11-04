# Resume Bullets

> [!NOTE]
> All resume bullets must be prefixed with `DRAFT:` and cite the corresponding row in `bench/RESULTS.md`.

- DRAFT: Designed and implemented an L7 HTTP load balancer in Go 1.22 from scratch featuring Power-of-Two-Choices (P2C) peak-EWMA picking, reducing tail latency under injected 10x slower backend faults compared to naive round-robin (cites `bench/RESULTS.md` Scenario S2 Policy C vs Policy A).
- DRAFT: Built an Envoy-style outlier detection engine with consecutive failure tracking, bounded ejection time scaling, and a configurable fleet ejection cap ($<50\%$), preventing cascading fleet collapse under degraded backends (cites `bench/RESULTS.md` Scenario S1 & S3 Policy C).
- DRAFT: Engineered dynamic token-bucket retry budgets with lock-free atomic CAS tracking, eliminating retry storms and capping upstream amplification to $\le 1.25\times$ during fleet-wide service degradation (cites `bench/RESULTS.md` Scenario S5 Policy C vs unbudgeted retries).
- DRAFT: Implemented zero-downtime hot configuration reload (`atomic.Pointer[Runtime]`) and graceful connection draining, achieving 0 client-visible errors across 20 consecutive reloads under continuous open-loop load (cites `bench/RESULTS.md` Hot Reload & Graceful Drain).
- DRAFT: Achieved sub-50µs proxy overhead at 1,790+ RPS with 0 memory allocations on the hot path picker benchmarks using tuned `http.Transport` connection pooling (cites `bench/RESULTS.md` Proxy Overhead & Microbenchmarks).
