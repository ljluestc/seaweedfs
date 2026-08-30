package command

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	cmdBenchmarkConnections.Run = runBenchmarkConnections // break init cycle
	bc.server = cmdBenchmarkConnections.Flag.String("server", "localhost:8080", "target http address, host:port")
	bc.path = cmdBenchmarkConnections.Flag.String("path", "/healthz", "path requested on each connection")
	bc.connections = cmdBenchmarkConnections.Flag.Int("n", 10000, "number of concurrent connections to hold open")
	bc.dialers = cmdBenchmarkConnections.Flag.Int("c", 256, "number of goroutines dialing and driving the connections")
	bc.requestInterval = cmdBenchmarkConnections.Flag.Duration("interval", 0, "per connection request interval, 0 keeps the connections idle")
	bc.holdDuration = cmdBenchmarkConnections.Flag.Duration("hold", time.Minute, "how long to hold the connections open after they are all established")
	bc.requestTimeout = cmdBenchmarkConnections.Flag.Duration("timeout", 30*time.Second, "per request timeout")
	bc.metrics = cmdBenchmarkConnections.Flag.String("metrics", "", "the target's metrics address, host:port, as started by -metricsPort")
	bc.sampleInterval = cmdBenchmarkConnections.Flag.Duration("sampleInterval", 5*time.Second, "how often the target's metrics are sampled")
}

var bc BenchmarkConnectionsOptions

type BenchmarkConnectionsOptions struct {
	server          *string
	path            *string
	connections     *int
	dialers         *int
	requestInterval *time.Duration
	holdDuration    *time.Duration
	requestTimeout  *time.Duration
	metrics         *string
	sampleInterval  *time.Duration
}

var cmdBenchmarkConnections = &Command{
	UsageLine: "benchmark.connections -server=localhost:8080 -n=50000 -metrics=localhost:9325",
	Short:     "benchmark the server memory cost of many concurrent http connections",
	Long: `Hold a large number of concurrent HTTP connections against one SeaweedFS
  http endpoint and report what they cost the server.

  "weed benchmark" measures throughput through a small, heavily reused
  connection pool, so it never says anything about the per connection cost.
  This command answers the other question: how much memory, how many
  goroutines and how many file descriptors does the server spend per open
  connection.

  Every connection here is a separate TCP connection that is dialed once and
  then held open for the whole run. Connections are never returned to a pool
  and are never shared between workers, so -n connections on the client are
  -n connections on the server.

  With -interval=0 the connections go idle after their first request, which
  isolates the cost of merely being connected. With -interval=10s each
  connection is kept warm by one request every 10 seconds, spread evenly in
  time rather than issued in bursts.

  Point -metrics at the target's metrics port (the -metricsPort of "weed
  volume", "weed filer", "weed master", ...). The command then samples
  go_goroutines, go_memstats_heap_alloc_bytes, go_memstats_heap_sys_bytes,
  process_resident_memory_bytes and process_open_fds and reports the
  baseline, the peak, and the cost per established connection.

  Examples:
    # 50k idle connections against a volume server, memory sampled every 5s
    weed benchmark.connections -server=localhost:8080 -n=50000 -metrics=localhost:9325

    # 20k connections kept warm with one request per connection every 10s
    weed benchmark.connections -server=localhost:8888 -path=/healthz \
        -n=20000 -interval=10s -hold=5m -metrics=localhost:9326

  Each connection needs a file descriptor on both sides, so raise the open
  file limit before a large run, for example "ulimit -n 200000".

  `,
}

func runBenchmarkConnections(cmd *Command, args []string) bool {
	opt := connectionBenchOptions{
		address:         *bc.server,
		path:            *bc.path,
		connections:     *bc.connections,
		dialers:         *bc.dialers,
		requestInterval: *bc.requestInterval,
		holdDuration:    *bc.holdDuration,
		requestTimeout:  *bc.requestTimeout,
		metricsAddress:  *bc.metrics,
		sampleInterval:  *bc.sampleInterval,
		progress:        os.Stdout,
	}

	result, err := runConnectionBench(context.Background(), opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection benchmark failed: %v\n", err)
		return false
	}
	result.print(os.Stdout, opt)

	return true
}

// connectionBenchOptions is the flag free form of the command options so that
// the benchmark can also be driven from tests.
type connectionBenchOptions struct {
	address         string // host:port, or a full http:// url
	path            string
	connections     int
	dialers         int
	requestInterval time.Duration // 0 leaves the connections idle
	holdDuration    time.Duration
	requestTimeout  time.Duration
	metricsAddress  string // host:port, or a full http:// url; empty disables sampling
	sampleInterval  time.Duration
	progress        io.Writer // nil discards the progress lines
}

type connectionBenchResult struct {
	Established   int
	DialErrors    int64
	Requests      int64
	RequestErrors int64
	Latency       *latencyHistogram
	Elapsed       time.Duration
	Samples       []serverSample
}

func runConnectionBench(ctx context.Context, opt connectionBenchOptions) (*connectionBenchResult, error) {
	if opt.connections <= 0 {
		return nil, fmt.Errorf("-n must be positive, got %d", opt.connections)
	}
	if opt.dialers <= 0 {
		return nil, fmt.Errorf("-c must be positive, got %d", opt.dialers)
	}
	if opt.requestTimeout <= 0 {
		opt.requestTimeout = 30 * time.Second
	}
	workers := min(opt.dialers, opt.connections)

	targetURL, err := normalizeBenchURL(opt.address, opt.path)
	if err != nil {
		return nil, err
	}
	// The request bytes are identical on every connection, so serialize the
	// request once and write those bytes instead of re-encoding per request.
	requestBytes, requestTemplate, err := buildRawRequest(targetURL)
	if err != nil {
		return nil, err
	}
	dialAddress, err := hostPortOf(targetURL)
	if err != nil {
		return nil, err
	}

	sampler := newServerSampler(opt.metricsAddress)
	result := &connectionBenchResult{Latency: newLatencyHistogram()}
	if s, ok := sampler.sample(); ok {
		result.Samples = append(result.Samples, s)
	}

	conns := make([]*benchConn, opt.connections)
	stats := make([]*workerStats, workers)
	for i := range stats {
		stats[i] = &workerStats{latency: newLatencyHistogram()}
	}

	start := time.Now()
	// established is read by the sampler goroutine while the dialers are still
	// filling conns, so the progress count cannot come from scanning the slice.
	var established atomic.Int64
	stopSampling := sampler.run(ctx, opt.sampleInterval, func(s serverSample) {
		result.Samples = append(result.Samples, s)
		printProgress(opt.progress, "%s established %d/%d connections, %s",
			time.Since(start).Round(time.Second), established.Load(), opt.connections, s)
	})

	dialer := &net.Dialer{Timeout: opt.requestTimeout}
	var next atomic.Int64
	next.Store(-1)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(ws *workerStats) {
			defer wg.Done()
			for {
				i := int(next.Add(1))
				if i >= opt.connections || ctx.Err() != nil {
					return
				}
				c, err := dialBenchConn(ctx, dialer, dialAddress, requestBytes, requestTemplate)
				if err != nil {
					ws.dialErrors++
					continue
				}
				// The first request completes the HTTP exchange, so the
				// server has really allocated its per connection state by the
				// time the connection is counted as established.
				d, err := c.roundTrip(opt.requestTimeout)
				if err != nil {
					c.close()
					ws.dialErrors++
					continue
				}
				ws.requests++
				ws.latency.add(d)
				conns[i] = c
				established.Add(1)
			}
		}(stats[w])
	}
	wg.Wait()

	printProgress(opt.progress, "established %d of %d connections in %s",
		established.Load(), opt.connections, time.Since(start).Round(time.Millisecond))

	if established.Load() > 0 && opt.holdDuration > 0 {
		holdConnections(ctx, conns, stats, opt)
	}

	stopSampling()
	if s, ok := sampler.sample(); ok {
		result.Samples = append(result.Samples, s)
	}
	result.Elapsed = time.Since(start)
	result.Established = int(established.Load())

	for _, ws := range stats {
		result.DialErrors += ws.dialErrors
		result.Requests += ws.requests
		result.RequestErrors += ws.requestErrors
		result.Latency.merge(ws.latency)
	}

	for _, c := range conns {
		c.close()
	}

	return result, nil
}

// holdConnections keeps every established connection open for the hold
// duration. With a request interval the connections are also kept warm: each
// worker walks its own stripe of connections and paces itself so that every
// connection is used about once per interval, rather than the whole stripe
// firing at once.
func holdConnections(ctx context.Context, conns []*benchConn, stats []*workerStats, opt connectionBenchOptions) {
	holdCtx, cancel := context.WithTimeout(ctx, opt.holdDuration)
	defer cancel()

	if opt.requestInterval <= 0 {
		<-holdCtx.Done()
		return
	}

	var wg sync.WaitGroup
	for w := range stats {
		stripe := make([]*benchConn, 0, len(conns)/len(stats)+1)
		for i := w; i < len(conns); i += len(stats) {
			if conns[i] != nil {
				stripe = append(stripe, conns[i])
			}
		}
		if len(stripe) == 0 {
			continue
		}
		wg.Add(1)
		go func(ws *workerStats, stripe []*benchConn) {
			defer wg.Done()
			spacing := opt.requestInterval / time.Duration(len(stripe))
			timer := time.NewTimer(spacing)
			defer timer.Stop()
			for {
				for _, c := range stripe {
					if c.isClosed() {
						continue
					}
					d, err := c.roundTrip(opt.requestTimeout)
					if err != nil {
						// A broken connection stays broken: the point of the
						// run is to hold connections, not to reconnect and
						// hide that the server dropped them.
						ws.requestErrors++
						c.close()
					} else {
						ws.requests++
						ws.latency.add(d)
					}
					timer.Reset(spacing)
					select {
					case <-holdCtx.Done():
						return
					case <-timer.C:
					}
				}
			}
		}(stats[w], stripe)
	}
	wg.Wait()
}

type workerStats struct {
	dialErrors    int64
	requests      int64
	requestErrors int64
	latency       *latencyHistogram
}

// benchConn is one dedicated TCP connection. It is never pooled and never
// shared, so the connection count on the client is the connection count on the
// server.
type benchConn struct {
	conn     net.Conn
	reader   *bufio.Reader
	request  []byte
	template *http.Request
	closed   atomic.Bool
}

func dialBenchConn(ctx context.Context, dialer *net.Dialer, address string, request []byte, template *http.Request) (*benchConn, error) {
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Idle connections are the point of this benchmark, so do not let the
		// client side quietly drop them.
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	return &benchConn{
		conn: conn,
		// A small reader keeps the client cheap enough that the client is not
		// what limits the connection count.
		reader:   bufio.NewReaderSize(conn, 1024),
		request:  request,
		template: template,
	}, nil
}

func (c *benchConn) roundTrip(timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	if err := c.conn.SetDeadline(start.Add(timeout)); err != nil {
		return 0, err
	}
	if _, err := c.conn.Write(c.request); err != nil {
		return 0, err
	}
	// ReadResponse only reads the method off the template request, so one
	// template is safely shared by every connection.
	resp, err := http.ReadResponse(c.reader, c.template)
	if err != nil {
		return 0, err
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return 0, fmt.Errorf("unexpected response status %s", resp.Status)
	}
	return time.Since(start), nil
}

func (c *benchConn) isClosed() bool {
	return c == nil || c.closed.Load()
}

func (c *benchConn) close() {
	if c == nil || !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.conn.Close()
}

func normalizeBenchURL(address, path string) (string, error) {
	if address == "" {
		return "", fmt.Errorf("-server is required")
	}
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimSuffix(address, "/") + path, nil
}

func hostPortOf(rawURL string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	host := req.URL.Host
	if req.URL.Port() == "" {
		host = net.JoinHostPort(host, "80")
	}
	return host, nil
}

func buildRawRequest(rawURL string) ([]byte, *http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", "weed-benchmark-connections")
	var buf bytes.Buffer
	if err := req.Write(&buf); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), req, nil
}

func printProgress(w io.Writer, format string, args ...interface{}) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, format+"\n", args...)
}

// serverSample is one reading of the target's own runtime metrics.
type serverSample struct {
	At           time.Time
	Goroutines   float64
	HeapAlloc    float64
	HeapSys      float64
	ResidentSize float64
	OpenFDs      float64
}

func (s serverSample) String() string {
	return fmt.Sprintf("server goroutines=%.0f heap=%s rss=%s fds=%.0f",
		s.Goroutines, humanBytes(s.HeapAlloc), humanBytes(s.ResidentSize), s.OpenFDs)
}

type serverSampler struct {
	url    string
	client *http.Client
	mu     sync.Mutex
}

func newServerSampler(metricsAddress string) *serverSampler {
	if metricsAddress == "" {
		return &serverSampler{}
	}
	url := metricsAddress
	if !strings.Contains(url, "://") {
		url = "http://" + url
	}
	if !strings.Contains(strings.TrimPrefix(url, "http://"), "/") {
		url = strings.TrimSuffix(url, "/") + "/metrics"
	}
	return &serverSampler{
		url: url,
		// A dedicated client, not the shared global one: the sampler must not
		// inherit the upload/download timeouts or the jwt middleware.
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (p *serverSampler) sample() (serverSample, bool) {
	if p.url == "" {
		return serverSample{}, false
	}
	resp, err := p.client.Get(p.url)
	if err != nil {
		return serverSample{}, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return serverSample{}, false
	}
	s := serverSample{At: time.Now()}
	s.Goroutines, _ = parsePrometheusMetric(body, "go_goroutines")
	s.HeapAlloc, _ = parsePrometheusMetric(body, "go_memstats_heap_alloc_bytes")
	s.HeapSys, _ = parsePrometheusMetric(body, "go_memstats_heap_sys_bytes")
	s.ResidentSize, _ = parsePrometheusMetric(body, "process_resident_memory_bytes")
	s.OpenFDs, _ = parsePrometheusMetric(body, "process_open_fds")
	return s, true
}

// run samples in the background until the returned stop func is called. The
// callback runs on a single goroutine, so it needs no locking of its own.
func (p *serverSampler) run(ctx context.Context, interval time.Duration, onSample func(serverSample)) func() {
	if p.url == "" || interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s, ok := p.sample(); ok {
					onSample(s)
				}
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

// parsePrometheusMetric reads one unlabelled gauge out of a metrics exposition
// body. The metrics sampled here are all process wide and carry no labels, so
// this avoids pulling in a full exposition parser.
func parsePrometheusMetric(body []byte, name string) (float64, bool) {
	for _, raw := range strings.Split(string(body), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rest, found := strings.CutPrefix(line, name)
		if !found || rest == "" {
			continue
		}
		// Require whitespace after the name so that go_goroutines does not
		// match go_goroutines_total, and so that a labelled series is skipped.
		if rest[0] != ' ' && rest[0] != '\t' {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			continue
		}
		return value, true
	}
	return 0, false
}

func (r *connectionBenchResult) print(w io.Writer, opt connectionBenchOptions) {
	targetURL, _ := normalizeBenchURL(opt.address, opt.path)
	fmt.Fprintf(w, "\n------------ connection benchmark ----------\n")
	fmt.Fprintf(w, "Target:                  %s\n", targetURL)
	fmt.Fprintf(w, "Connections requested:   %d\n", opt.connections)
	fmt.Fprintf(w, "Connections established: %d\n", r.Established)
	fmt.Fprintf(w, "Connection errors:       %d\n", r.DialErrors)
	fmt.Fprintf(w, "Requests completed:      %d\n", r.Requests)
	fmt.Fprintf(w, "Request errors:          %d\n", r.RequestErrors)
	if opt.requestInterval > 0 {
		fmt.Fprintf(w, "Request interval:        %s per connection\n", opt.requestInterval)
	} else {
		fmt.Fprintf(w, "Request interval:        idle after the first request\n")
	}
	fmt.Fprintf(w, "Time taken:              %s\n", r.Elapsed.Round(time.Millisecond))

	if r.Latency.count > 0 {
		fmt.Fprintf(w, "\nRequest latency (ms)\n")
		fmt.Fprintf(w, "  min %.1f  avg %.1f  p50 %.1f  p95 %.1f  p99 %.1f  max %.1f\n",
			ms(r.Latency.min()), ms(r.Latency.average()), ms(r.Latency.percentile(50)),
			ms(r.Latency.percentile(95)), ms(r.Latency.percentile(99)), ms(r.Latency.max()))
	}

	r.printServerCost(w)
}

func (r *connectionBenchResult) printServerCost(w io.Writer) {
	if len(r.Samples) < 2 {
		if len(r.Samples) == 0 {
			fmt.Fprintf(w, "\nNo server metrics sampled. Pass -metrics=host:port to measure the server cost.\n")
		}
		return
	}
	baseline := r.Samples[0]
	peak := r.Samples[0]
	for _, s := range r.Samples[1:] {
		peak.Goroutines = math.Max(peak.Goroutines, s.Goroutines)
		peak.HeapAlloc = math.Max(peak.HeapAlloc, s.HeapAlloc)
		peak.HeapSys = math.Max(peak.HeapSys, s.HeapSys)
		peak.ResidentSize = math.Max(peak.ResidentSize, s.ResidentSize)
		peak.OpenFDs = math.Max(peak.OpenFDs, s.OpenFDs)
	}

	fmt.Fprintf(w, "\nServer cost of %d connections\n", r.Established)
	fmt.Fprintf(w, "%-28s %14s %14s %14s %16s\n", "", "baseline", "peak", "delta", "per connection")
	r.printCostRow(w, "goroutines", baseline.Goroutines, peak.Goroutines, false)
	r.printCostRow(w, "open file descriptors", baseline.OpenFDs, peak.OpenFDs, false)
	r.printCostRow(w, "go heap in use", baseline.HeapAlloc, peak.HeapAlloc, true)
	r.printCostRow(w, "go heap reserved", baseline.HeapSys, peak.HeapSys, true)
	r.printCostRow(w, "process resident memory", baseline.ResidentSize, peak.ResidentSize, true)
}

func (r *connectionBenchResult) printCostRow(w io.Writer, label string, baseline, peak float64, asBytes bool) {
	if baseline == 0 && peak == 0 {
		return
	}
	delta := peak - baseline
	perConn := 0.0
	if r.Established > 0 {
		perConn = delta / float64(r.Established)
	}
	if asBytes {
		fmt.Fprintf(w, "%-28s %14s %14s %14s %16s\n", label,
			humanBytes(baseline), humanBytes(peak), humanBytes(delta), humanBytes(perConn))
		return
	}
	fmt.Fprintf(w, "%-28s %14.0f %14.0f %14.0f %16.2f\n", label, baseline, peak, delta, perConn)
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func humanBytes(v float64) string {
	sign := ""
	if v < 0 {
		sign, v = "-", -v
	}
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%s%.1f GiB", sign, v/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%s%.1f MiB", sign, v/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%s%.1f KiB", sign, v/(1<<10))
	default:
		return fmt.Sprintf("%s%.0f B", sign, v)
	}
}

// latencyHistogram keeps request latencies in 0.1ms buckets, the same
// resolution "weed benchmark" reports, so that a run holding millions of
// requests stays bounded in memory.
const (
	latencyBucketWidth = 100 * time.Microsecond
	latencyBucketCount = 20000 // up to 2 seconds
)

type latencyHistogram struct {
	buckets  []int64
	overflow int64
	count    int64
	sumNs    int64
	minNs    int64
	maxNs    int64
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{buckets: make([]int64, latencyBucketCount)}
}

func (h *latencyHistogram) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	index := int(d / latencyBucketWidth)
	if index < latencyBucketCount {
		h.buckets[index]++
	} else {
		h.overflow++
	}
	if h.count == 0 || int64(d) < h.minNs {
		h.minNs = int64(d)
	}
	if int64(d) > h.maxNs {
		h.maxNs = int64(d)
	}
	h.count++
	h.sumNs += int64(d)
}

func (h *latencyHistogram) merge(other *latencyHistogram) {
	if other == nil || other.count == 0 {
		return
	}
	for i, v := range other.buckets {
		h.buckets[i] += v
	}
	h.overflow += other.overflow
	if h.count == 0 || other.minNs < h.minNs {
		h.minNs = other.minNs
	}
	if other.maxNs > h.maxNs {
		h.maxNs = other.maxNs
	}
	h.count += other.count
	h.sumNs += other.sumNs
}

func (h *latencyHistogram) min() time.Duration { return time.Duration(h.minNs) }
func (h *latencyHistogram) max() time.Duration { return time.Duration(h.maxNs) }

func (h *latencyHistogram) average() time.Duration {
	if h.count == 0 {
		return 0
	}
	return time.Duration(h.sumNs / h.count)
}

// percentile returns the upper edge of the bucket holding the requested
// percentile. Latencies past the last bucket are reported as the histogram max.
func (h *latencyHistogram) percentile(p float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	target := int64(math.Ceil(p / 100 * float64(h.count)))
	if target < 1 {
		target = 1
	}
	var seen int64
	for i, v := range h.buckets {
		seen += v
		if seen >= target {
			return time.Duration(i+1) * latencyBucketWidth
		}
	}
	return h.max()
}
