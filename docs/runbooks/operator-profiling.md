# Production Profiling with pprof

This guide explains how to use the pprof-compatible profiling endpoints to diagnose CPU and memory performance issues in production SorobanPulse deployments.

## Overview

The `/v1/admin/profile/*` endpoints provide CPU and heap profiling in pprof format, a standard used by the Go ecosystem and widely supported by web-based analysis tools.

### Requirements

- `ADMIN_API_KEY` must be set in environment variables
- `go tool pprof` (optional, for CLI analysis)
- `pprof.me` (optional, for web-based analysis)

## CPU Profiling

### Endpoint

```
GET /v1/admin/profile/cpu?seconds=<duration>
```

**Parameters:**
- `seconds`: Duration to capture profile (1-120 seconds, default: 30)

### Example

Capture a 30-second CPU profile:

```bash
export ADMIN_API_KEY="your-secure-key"
export SOROBAN_PULSE_URL="https://your-soroban-pulse-instance.com"

curl -H "X-Api-Key: $ADMIN_API_KEY" \
  "$SOROBAN_PULSE_URL/v1/admin/profile/cpu?seconds=30" \
  -o cpu.pb.gz
```

### Analyzing the profile locally

Using `go tool pprof` (requires Go installed):

```bash
# Interactive CLI
go tool pprof cpu.pb.gz

# Web UI (opens in browser)
go tool pprof -http=localhost:6060 cpu.pb.gz
```

In the `go tool pprof` REPL:

```
(pprof) top10
```

Shows the top 10 CPU-consuming functions.

```
(pprof) list <function_name>
```

Shows annotated source code with CPU time per line.

```
(pprof) web
```

Generates an SVG call graph (requires Graphviz).

### Analyzing on pprof.me

Upload the profile to [pprof.me](https://www.pprof.me) for web-based visualization:

1. Visit https://www.pprof.me
2. Click "Upload Profile"
3. Select the `cpu.pb.gz` file
4. Explore interactive flame graphs

## Heap Profiling

### Endpoint

```
GET /v1/admin/profile/heap
```

Returns a snapshot of current memory allocations.

### Example

```bash
curl -H "X-Api-Key: $ADMIN_API_KEY" \
  "$SOROBAN_PULSE_URL/v1/admin/profile/heap" \
  -o heap.pb.gz
```

### Analyzing the heap profile

```bash
# CLI
go tool pprof heap.pb.gz

# Web UI
go tool pprof -http=localhost:6060 heap.pb.gz
```

Common queries:

```
(pprof) alloc_space
```

Total memory allocated (including freed).

```
(pprof) alloc_objects
```

Total objects allocated (including freed).

```
(pprof) inuse_space
```

Memory currently in use.

```
(pprof) inuse_objects
```

Objects currently allocated.

## Troubleshooting Common Issues

### High CPU

1. Capture a 60-second CPU profile
2. Look at `top10` to identify hot functions
3. Use `list <function>` to see which lines are consuming CPU
4. Check for:
   - Unbounded loops
   - Expensive regex operations
   - Synchronous blocking I/O
   - Inefficient allocations in hot paths

### Memory leak

1. Capture heap profiles at two points in time:
   ```bash
   curl -H "X-Api-Key: $ADMIN_API_KEY" \
     "$SOROBAN_PULSE_URL/v1/admin/profile/heap" \
     -o heap1.pb.gz
   
   # Wait 1 hour
   sleep 3600
   
   curl -H "X-Api-Key: $ADMIN_API_KEY" \
     "$SOROBAN_PULSE_URL/v1/admin/profile/heap" \
     -o heap2.pb.gz
   ```

2. Compare the profiles:
   ```bash
   go tool pprof -base heap1.pb.gz heap2.pb.gz
   ```

3. Look for allocations that grew over time

### Memory fragmentation

If `inuse_space` is much smaller than `alloc_space`, memory fragmentation may be occurring:

1. Check if connection pools are properly sized
2. Look for accumulating data structures (caches, buffers)
3. Consider adjusting jemalloc settings if using jemalloc-based allocator

## Performance Best Practices

### When to profile

- **Baseline**: Profile a healthy instance to establish normal behavior
- **Regression investigation**: When performance degrades
- **Load testing**: Profile at peak load to identify scaling limits
- **After deploys**: Compare profiles before/after to detect regressions

### Profile duration

- **CPU profiles**: 30-60 seconds captures sufficient data for 99% of cases
- **Heap profiles**: Single snapshot; capture when you suspect issues
- **High-traffic instances**: May need shorter durations to avoid overhead

### Security

- Profiling endpoints require `ADMIN_API_KEY` and are not exposed on public APIs
- Profiles contain stack traces and function names; treat as sensitive
- Store profiles securely; do not commit to version control

## Metrics

Profiling endpoint access is tracked:

```
soroban_pulse_profile_requests_total{type="cpu"}
soroban_pulse_profile_requests_total{type="heap"}
soroban_pulse_profile_errors_total{type="cpu", error="..."}
soroban_pulse_profile_errors_total{type="heap", error="..."}
```

Monitor these in your observability stack to detect:
- Excessive profiling requests (may indicate troubleshooting activity)
- Profiling errors (may indicate issues with the profiler itself)

## Further Reading

- [pprof documentation](https://github.com/google/pprof/tree/main/doc)
- [Go profiling guide](https://go.dev/blog/profiling-go-programs)
- [Rust performance optimization](https://www.rust-lang.org/what/wg-libs/org/)
