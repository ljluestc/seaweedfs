package command

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// connCounter records how many distinct TCP connections a test server saw, and
// how many of them were open at the high water mark.
type connCounter struct {
	mu       sync.Mutex
	total    int
	open     int
	maxOpen  int
	requests int
}

func (c *connCounter) connState(_ net.Conn, state http.ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch state {
	case http.StateNew:
		c.total++
		c.open++
		if c.open > c.maxOpen {
			c.maxOpen = c.open
		}
	case http.StateClosed, http.StateHijacked:
		c.open--
	}
}

func (c *connCounter) countRequest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
}

func (c *connCounter) snapshot() (total, maxOpen, requests int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total, c.maxOpen, c.requests
}

func newCountingServer(t *testing.T) (*httptest.Server, *connCounter) {
	t.Helper()
	counter := &connCounter{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.countRequest()
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
	server.Config.ConnState = counter.connState
	server.Start()
	t.Cleanup(server.Close)
	return server, counter
}

// Every benchmark connection has to be its own TCP connection: pooling is
// exactly what "weed benchmark" does and what this command has to avoid.
func TestConnectionBenchOpensOneConnectionPerRequestedConnection(t *testing.T) {
	server, counter := newCountingServer(t)

	result, err := runConnectionBench(context.Background(), connectionBenchOptions{
		address:        server.Listener.Addr().String(),
		path:           "/healthz",
		connections:    25,
		dialers:        4,
		requestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("runConnectionBench: %v", err)
	}

	if result.Established != 25 {
		t.Fatalf("established = %d, want 25 (dial errors %d)", result.Established, result.DialErrors)
	}
	if result.Requests != 25 {
		t.Fatalf("requests = %d, want 25", result.Requests)
	}
	total, maxOpen, requests := counter.snapshot()
	if total != 25 {
		t.Fatalf("server saw %d connections, want 25", total)
	}
	if maxOpen != 25 {
		t.Fatalf("server held %d connections at once, want 25 held open together", maxOpen)
	}
	if requests != 25 {
		t.Fatalf("server served %d requests, want 25", requests)
	}
}

// With a request interval the connections are kept warm, and the extra
// requests must ride on the connections that are already open.
func TestConnectionBenchKeepsConnectionsWarmWithoutRedialing(t *testing.T) {
	server, counter := newCountingServer(t)

	result, err := runConnectionBench(context.Background(), connectionBenchOptions{
		address:         server.Listener.Addr().String(),
		path:            "/healthz",
		connections:     8,
		dialers:         2,
		requestInterval: 10 * time.Millisecond,
		holdDuration:    300 * time.Millisecond,
		requestTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("runConnectionBench: %v", err)
	}

	if result.Established != 8 {
		t.Fatalf("established = %d, want 8", result.Established)
	}
	if result.Requests <= 8 {
		t.Fatalf("requests = %d, want more than the 8 initial requests", result.Requests)
	}
	if result.RequestErrors != 0 {
		t.Fatalf("request errors = %d, want 0", result.RequestErrors)
	}
	total, _, _ := counter.snapshot()
	if total != 8 {
		t.Fatalf("server saw %d connections, want 8 reused connections", total)
	}
}

// A hold with no request interval leaves the connections idle, which is the
// mode that isolates the per connection cost.
func TestConnectionBenchHoldsIdleConnections(t *testing.T) {
	server, counter := newCountingServer(t)

	start := time.Now()
	result, err := runConnectionBench(context.Background(), connectionBenchOptions{
		address:        server.Listener.Addr().String(),
		path:           "/healthz",
		connections:    5,
		dialers:        5,
		holdDuration:   200 * time.Millisecond,
		requestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("runConnectionBench: %v", err)
	}

	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("returned after %s, want the connections held for at least 200ms", elapsed)
	}
	if result.Requests != 5 {
		t.Fatalf("requests = %d, want 5 idle connections after their first request", result.Requests)
	}
	if _, maxOpen, _ := counter.snapshot(); maxOpen != 5 {
		t.Fatalf("server held %d connections at once, want 5", maxOpen)
	}
}

// A cancelled context has to unwind the run instead of holding for the full
// duration.
func TestConnectionBenchStopsOnContextCancel(t *testing.T) {
	server, _ := newCountingServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := runConnectionBench(ctx, connectionBenchOptions{
		address:        server.Listener.Addr().String(),
		path:           "/healthz",
		connections:    4,
		dialers:        2,
		holdDuration:   time.Minute,
		requestTimeout: 5 * time.Second,
	}); err != nil {
		t.Fatalf("runConnectionBench: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("run took %s, want it to stop with the context", elapsed)
	}
}

func TestConnectionBenchRejectsBadOptions(t *testing.T) {
	for name, opt := range map[string]connectionBenchOptions{
		"no connections": {address: "localhost:8080", connections: 0, dialers: 1},
		"no dialers":     {address: "localhost:8080", connections: 1, dialers: 0},
		"no server":      {address: "", connections: 1, dialers: 1},
	} {
		if _, err := runConnectionBench(context.Background(), opt); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestParsePrometheusMetric(t *testing.T) {
	body := []byte(`# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 42
go_goroutines_stopped 7
go_memstats_heap_alloc_bytes 1.234567e+06
process_resident_memory_bytes 4.6989312e+07
SeaweedFS_volumeServer_request_total{type="read"} 99
`)

	for _, tc := range []struct {
		name  string
		want  float64
		found bool
	}{
		{"go_goroutines", 42, true},
		{"go_goroutines_stopped", 7, true},
		{"go_memstats_heap_alloc_bytes", 1234567, true},
		{"process_resident_memory_bytes", 46989312, true},
		{"process_open_fds", 0, false},
		// A labelled series is not an unlabelled gauge and must not match.
		{"SeaweedFS_volumeServer_request_total", 0, false},
	} {
		got, found := parsePrometheusMetric(body, tc.name)
		if found != tc.found || got != tc.want {
			t.Errorf("parsePrometheusMetric(%q) = %v, %v; want %v, %v", tc.name, got, found, tc.want, tc.found)
		}
	}
}

func TestServerSamplerReadsMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, "go_goroutines 17\ngo_memstats_heap_alloc_bytes 2048\nprocess_open_fds 9\n")
	}))
	defer server.Close()

	sample, ok := newServerSampler(server.Listener.Addr().String()).sample()
	if !ok {
		t.Fatal("sample() reported no reading")
	}
	if sample.Goroutines != 17 || sample.HeapAlloc != 2048 || sample.OpenFDs != 9 {
		t.Fatalf("sample = %+v, want goroutines 17, heap 2048, fds 9", sample)
	}

	// No metrics address means no sampling, not a failure.
	if _, ok := newServerSampler("").sample(); ok {
		t.Fatal("sample() with no metrics address should report no reading")
	}
}

func TestNormalizeBenchURL(t *testing.T) {
	for _, tc := range []struct{ address, path, want string }{
		{"localhost:8080", "/healthz", "http://localhost:8080/healthz"},
		{"localhost:8080", "healthz", "http://localhost:8080/healthz"},
		{"http://localhost:8080/", "/healthz", "http://localhost:8080/healthz"},
		{"localhost:8080", "", "http://localhost:8080"},
	} {
		got, err := normalizeBenchURL(tc.address, tc.path)
		if err != nil || got != tc.want {
			t.Errorf("normalizeBenchURL(%q, %q) = %q, %v; want %q", tc.address, tc.path, got, err, tc.want)
		}
	}
	if _, err := normalizeBenchURL("", "/healthz"); err == nil {
		t.Error("normalizeBenchURL with no address should fail")
	}
}

func TestLatencyHistogram(t *testing.T) {
	h := newLatencyHistogram()
	for i := 1; i <= 100; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	if got := h.min(); got != time.Millisecond {
		t.Errorf("min = %s, want 1ms", got)
	}
	if got := h.max(); got != 100*time.Millisecond {
		t.Errorf("max = %s, want 100ms", got)
	}
	if got := h.average(); got < 50*time.Millisecond || got > 51*time.Millisecond {
		t.Errorf("average = %s, want about 50.5ms", got)
	}
	if got := h.percentile(50); got < 50*time.Millisecond || got > 51*time.Millisecond {
		t.Errorf("p50 = %s, want about 50ms", got)
	}
	if got := h.percentile(99); got < 99*time.Millisecond || got > 100*time.Millisecond {
		t.Errorf("p99 = %s, want about 99ms", got)
	}

	// Latencies past the last bucket still count and still move the max.
	h.add(5 * time.Second)
	if h.overflow != 1 {
		t.Errorf("overflow = %d, want 1", h.overflow)
	}
	if got := h.max(); got != 5*time.Second {
		t.Errorf("max = %s, want 5s", got)
	}

	// Merging keeps every worker's requests.
	other := newLatencyHistogram()
	other.add(time.Microsecond * 50)
	h.merge(other)
	if h.count != 102 {
		t.Errorf("count after merge = %d, want 102", h.count)
	}
	if got := h.min(); got != 50*time.Microsecond {
		t.Errorf("min after merge = %s, want 50us", got)
	}
}
