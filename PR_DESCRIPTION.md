# benchmark the per connection cost, and measure nbio against it (#3884)

## Problem

Issue #3884 asks whether `github.com/lesismal/nbio` would lower SeaweedFS's
memory cost when a server holds many HTTP connections, and names the blocker:

> Current weed benchmark re-uses http connections. So the total number of
> connections are not big. Need to have a different way to benchmark it.

That is exactly right. `weed benchmark` drives everything through
`operation.Upload` and the shared HTTP client, so `-c=16` means sixteen pooled
connections no matter how many files are written. It measures throughput, and
it cannot say anything about what one *open* connection costs. Without that
number there is nothing to compare nbio against.

So this PR adds the missing benchmark first, then uses it to answer the
question.

## Part 1: `weed benchmark.connections`

A new command that holds `-n` separate TCP connections against one SeaweedFS
HTTP endpoint and reports what they cost the server.

```
weed benchmark.connections -server=localhost:8080 -n=50000 -metrics=localhost:9325
```

* Every connection is dialed once and held open for the whole run. Connections
  are never pooled and never shared between workers, so `-n` connections on the
  client are `-n` connections on the server. Each one does an initial request,
  so the server has really allocated its per connection state before the
  connection is counted.
* `-interval=0` (the default) leaves the connections idle after that first
  request, which isolates the cost of merely being connected. `-interval=10s`
  keeps each connection warm with one request every ten seconds, spread evenly
  in time rather than fired in bursts.
* `-metrics=host:port` points at the target's existing `-metricsPort`. The
  command samples `go_goroutines`, `go_memstats_heap_alloc_bytes`,
  `go_memstats_heap_sys_bytes`, `process_resident_memory_bytes` and
  `process_open_fds` and reports baseline, peak, delta and **cost per
  established connection** — the number the issue is actually asking for.

Client cost is kept low enough that the client is not the bottleneck: bounded
worker goroutines rather than one per connection, a 1 KiB read buffer per
connection, the request serialized once and written as raw bytes, and latencies
folded into a 0.1 ms histogram instead of a slice.

Against a real volume server:

```
$ weed server -dir=/tmp/w -volume.port=8090 -metricsPort=9335 &
$ weed benchmark.connections -server=127.0.0.1:8090 -n=20000 -metrics=127.0.0.1:9335

Server cost of 20000 connections
                                   baseline           peak          delta   per connection
goroutines                               67          20067          20000             1.00
open file descriptors                    22          20022          20000             1.00
go heap in use                     14.1 MiB      235.3 MiB      221.1 MiB         11.3 KiB
go heap reserved                   26.2 MiB      410.2 MiB      384.0 MiB         19.7 KiB
process resident memory           104.1 MiB      502.3 MiB      398.2 MiB         20.4 KiB
```

So today: one goroutine and about **20 KiB of resident memory per idle
connection**.

## Part 2: `test/nbio`, the actual experiment

A small program that serves one trivial handler with either `net/http` or nbio,
selected by `-engine`, and publishes its own process metrics under the same
names SeaweedFS exports — so `weed benchmark.connections` drives both engines
and the real server identically. `run.sh` runs the A/B; `README.md` documents
it.

It is a **separate Go module**, so nbio never enters the SeaweedFS dependency
graph. Nothing in `weed/` imports it, `go build ./...` does not see it, and
dropping the directory removes the dependency entirely.

### Results

20000 connections, one listener, 32 cores, Go 1.25, nbio v1.6.12.

Idle connections:

| | goroutines | heap in use / conn | RSS / conn | RSS total |
| --- | --- | --- | --- | --- |
| `net/http` | 20000 (1 per conn) | 11.0 KiB | 20.2 KiB | 394 MiB |
| nbio | 0 (16 total, flat) | 1.1 KiB | 1.8 KiB | 34 MiB |

Warm connections, one request per connection every 10s (~80000 requests):

| | goroutines | heap in use / conn | RSS / conn | RSS total | p99 |
| --- | --- | --- | --- | --- | --- |
| `net/http` | 20000 | 14.7 KiB | 25.1 KiB | 491 MiB | 26.5 ms |
| nbio | 1 | 1.8 KiB | 2.5 KiB | 49 MiB | 23.8 ms |

The `net/http` microbenchmark lands on 20.2 KiB per connection and the real
volume server on 20.4 KiB, which is the evidence that the microbenchmark is a
fair stand-in.

**Answer to the issue: yes, for open-but-idle connections nbio costs roughly an
order of magnitude less** — about 2 KiB per connection instead of about 20 KiB,
with a constant handful of goroutines instead of one per connection, and no
throughput or latency penalty at this request rate.

### What this does not answer

The handler measured above is trivial. Before nbio could serve real SeaweedFS
traffic:

* **Blocking work in handlers.** Under nbio's default `IOModNonBlocking`,
  handlers run on a task pool (`NumCPU() * 1024` goroutines). Volume server
  handlers do blocking disk reads, so a slow disk would occupy pool slots rather
  than park a per connection goroutine. `IOModMixed` exists for this, but above
  `MaxBlockingOnline` it falls back to a goroutine per connection, giving back
  exactly the memory measured here.
* **Backpressure on large responses.** `net/http` throttles a slow reader by
  blocking the handler goroutine. nbio writes are non-blocking and queue in user
  space (`MaxWriteBufferSize`, unlimited by default), so a volume server
  streaming large needles to slow clients could hold *more* memory than it does
  today. This is the risk that most needs measuring next, and
  `benchmark.connections` is the right harness for it.
* **TLS.** nbio terminates TLS with `github.com/lesismal/llib`, not
  `crypto/tls`, so the https paths, reloading certificates and client
  certificate checks in `weed/security` would all need revalidating.
* **Coverage.** Hijacking, HTTP/2 and `http.ResponseController` are not
  exercised at all.

I am deliberately not proposing an nbio swap in this PR. This gives the project
the measurement, the numbers, and a reproducible way to re-run both.

## Tests

New `weed/command/benchmark_connections_test.go` drives the benchmark against
an `httptest` server whose `ConnState` hook counts connections:

* `TestConnectionBenchOpensOneConnectionPerRequestedConnection` — 25 requested
  connections are 25 distinct server connections, all open at once, no pooling.
* `TestConnectionBenchKeepsConnectionsWarmWithoutRedialing` — with an interval
  the extra requests ride on the connections already open; the server still sees
  only the original 8.
* `TestConnectionBenchHoldsIdleConnections` — an idle hold really holds, and
  issues no requests past the first one per connection.
* `TestConnectionBenchStopsOnContextCancel` — a cancelled context unwinds the
  run instead of holding for the full duration.
* `TestConnectionBenchRejectsBadOptions`, `TestParsePrometheusMetric`,
  `TestServerSamplerReadsMetrics`, `TestNormalizeBenchURL`,
  `TestLatencyHistogram` — option validation, metric parsing (including that
  `go_goroutines` does not match `go_goroutines_stopped` and that a labelled
  series is skipped), URL handling, and the histogram's percentiles, overflow
  and merge.

```
go test -race ./weed/command/    ok
go build ./...                   ok
go vet ./weed/command/           clean
(cd test/nbio && go vet ./...)   clean
```

## Compatibility

`benchmark.connections` is a new command; nothing existing changes except the
one line registering it. No new dependency in the main module, no configuration
change, no on-disk format change.

Refs #3884
