# nbio vs net/http: what an open connection costs

This directory answers issue
[#3884](https://github.com/seaweedfs/seaweedfs/issues/3884): is
[github.com/lesismal/nbio](https://github.com/lesismal/nbio) meaningfully
cheaper than `net/http` when a SeaweedFS server holds a large number of open
HTTP connections?

It is an experiment, not a proposed replacement. It lives in its own Go module
so that measuring nbio does not add nbio to the SeaweedFS dependency graph.

## What is measured

`main.go` builds `httpengine`, a server that serves one trivial `/healthz`
handler with either `net/http` or nbio, chosen by `-engine`. Both engines get
the same `http.Handler`, the same keepalive timeout, and the same plain
`net/http` metrics endpoint on a separate port. That metrics endpoint publishes
the process's own numbers under the names SeaweedFS already exports
(`go_goroutines`, `go_memstats_heap_alloc_bytes`, `go_memstats_heap_sys_bytes`,
`process_resident_memory_bytes`, `process_open_fds`), so the same client can
drive both engines and a real SeaweedFS server.

That client is `weed benchmark.connections`. `weed benchmark` reuses a small
connection pool, which is why it says nothing about this question; the
connection benchmark instead dials `-n` separate TCP connections, holds every
one of them open for the whole run, and reports the server's cost per
established connection.

## Running it

```sh
ulimit -n 200000
CONNS=20000 HOLD=30s ./run.sh
```

`run.sh` builds `weed` and `httpengine`, then for each engine starts the server,
runs the benchmark against it, and shuts it down. Useful knobs:

| variable | default | meaning |
| --- | --- | --- |
| `CONNS` | 20000 | connections held open |
| `HOLD` | 30s | how long they are held after they are all up |
| `INTERVAL` | 0 | per connection request interval; 0 leaves them idle |
| `ENGINES` | `nethttp nbio` | which engines to run |
| `ADDR` / `METRICS` | `127.0.0.1:8081` / `127.0.0.1:9101` | listen addresses |

Every connection costs a file descriptor on both sides, and the client needs one
ephemeral port per connection, so `CONNS` above roughly 25000 against a single
listener needs a wider `net.ipv4.ip_local_port_range` or a second listener.

## Results

20000 connections, one listener, Linux 7.0, 32 cores, Go 1.25, nbio v1.6.12.
"per connection" is the peak minus the baseline, divided by the connections
established.

Idle connections (`INTERVAL=0`, one request each, then held open):

| | goroutines | heap in use / conn | RSS / conn | RSS total |
| --- | --- | --- | --- | --- |
| `net/http` | 20000 (1 per conn) | 11.0 KiB | 20.2 KiB | 394 MiB |
| nbio | 0 (16 total, flat) | 1.1 KiB | 1.8 KiB | 34 MiB |

Warm connections (`INTERVAL=10s`, ~80000 requests over the run):

| | goroutines | heap in use / conn | RSS / conn | RSS total | p99 latency |
| --- | --- | --- | --- | --- | --- |
| `net/http` | 20000 | 14.7 KiB | 25.1 KiB | 491 MiB | 26.5 ms |
| nbio | 1 | 1.8 KiB | 2.5 KiB | 49 MiB | 23.8 ms |

For reference, the same benchmark against a real volume server
(`weed server -volume.port=8090 -metricsPort=9335`, 20000 idle connections)
costs **20.4 KiB RSS and one goroutine per connection** — the same figure as
the `net/http` microbenchmark, which is the evidence that the microbenchmark is
a fair stand-in for the real server.

So the answer to the issue is yes: for connections that are open but mostly
idle, nbio costs roughly **an order of magnitude less memory** (about 2 KiB per
connection against about 20 KiB) and keeps a constant, tiny number of
goroutines, with no throughput or latency penalty at this request rate.

## What this does not answer

The measurement above is a trivial handler. Before nbio could serve real
SeaweedFS traffic:

* **Blocking work in handlers.** In the default `IOModNonBlocking`, handlers run
  on nbio's task pool (`runtime.NumCPU() * 1024` goroutines). Volume server
  handlers do blocking disk reads, so a slow disk would occupy pool slots
  instead of parking a per connection goroutine. `IOModMixed` exists for this,
  but it falls back to a goroutine per connection above
  `MaxBlockingOnline`, which gives back exactly the memory measured here.
* **Backpressure on large responses.** `net/http` throttles a slow reader by
  blocking the handler. nbio writes are non-blocking and queue in user space
  (`MaxWriteBufferSize`, unlimited by default), so a volume server streaming
  large needles to slow clients could hold far more memory than it does today.
  This is the risk that most needs measuring next.
* **TLS.** nbio terminates TLS with `github.com/lesismal/llib`, not
  `crypto/tls`, so the https paths, the reloading certificate support and the
  client certificate checks in `weed/security` would all need revalidating.
* **Coverage.** Hijacking, HTTP/2 and `http.ResponseController` behaviour are
  not exercised here at all.

A sensible next step is to repeat this against a handler that streams a
multi-megabyte body to deliberately slow readers, which is where the two models
differ most.
