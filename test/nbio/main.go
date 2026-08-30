// Command httpengine serves one trivial endpoint with either net/http or nbio
// and publishes its own process metrics under the names SeaweedFS already
// exports, so that
//
//	weed benchmark.connections -server=... -metrics=...
//
// can measure both engines exactly the same way.
//
// This exists to answer issue #3884: is github.com/lesismal/nbio meaningfully
// cheaper than net/http when a SeaweedFS server holds a large number of open
// HTTP connections? It deliberately lives in its own Go module so that the
// experiment does not add nbio to the SeaweedFS dependency graph.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lesismal/nbio/nbhttp"
)

var (
	engine      = flag.String("engine", "nethttp", "which http server to run: nethttp or nbio")
	addr        = flag.String("addr", "127.0.0.1:8081", "address serving /healthz")
	metricsAddr = flag.String("metrics", "127.0.0.1:9101", "address serving /metrics")
	pollers     = flag.Int("pollers", 0, "nbio poller goroutines, 0 uses the nbio default of GOMAXPROCS/4")
	readBuffer  = flag.Int("readBuffer", 0, "nbio per read buffer size in bytes, 0 uses the nbio default")
	idleTimeout = flag.Duration("idleTimeout", time.Hour, "how long an idle connection is kept, both engines")
)

func main() {
	flag.Parse()

	// The metrics endpoint is plain net/http on its own port in both runs, so
	// its own cost cancels out of the comparison.
	go serveMetrics(*metricsAddr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	switch strings.ToLower(*engine) {
	case "nethttp", "net/http", "std":
		serveNetHTTP(stop)
	case "nbio":
		serveNbio(stop)
	default:
		log.Fatalf("unknown -engine %q, want nethttp or nbio", *engine)
	}
}

// healthz is the handler both engines serve. It is deliberately trivial: this
// measures the cost of holding a connection, not the cost of doing work.
func healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

func handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/", healthz)
	return mux
}

func serveNetHTTP(stop <-chan os.Signal) {
	server := &http.Server{
		Addr:        *addr,
		Handler:     handler(),
		IdleTimeout: *idleTimeout,
	}
	go func() {
		log.Printf("net/http listening on %s", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("net/http serve: %v", err)
		}
	}()
	<-stop
	server.Close()
}

func serveNbio(stop <-chan os.Signal) {
	config := nbhttp.Config{
		Network:       "tcp",
		Addrs:         []string{*addr},
		Handler:       handler(),
		MaxLoad:       1 << 21,
		KeepaliveTime: *idleTimeout,
		IOMod:         nbhttp.IOModNonBlocking,
	}
	if *pollers > 0 {
		config.NPoller = *pollers
	}
	if *readBuffer > 0 {
		config.ReadBufferSize = *readBuffer
	}
	server := nbhttp.NewEngine(config)
	if err := server.Start(); err != nil {
		log.Fatalf("nbio serve: %v", err)
	}
	log.Printf("nbio listening on %s", *addr)
	<-stop
	server.Stop()
}

func serveMetrics(address string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())
		fmt.Fprintf(w, "go_memstats_heap_alloc_bytes %d\n", mem.HeapAlloc)
		fmt.Fprintf(w, "go_memstats_heap_sys_bytes %d\n", mem.HeapSys)
		fmt.Fprintf(w, "go_memstats_stack_sys_bytes %d\n", mem.StackSys)
		fmt.Fprintf(w, "go_memstats_sys_bytes %d\n", mem.Sys)
		fmt.Fprintf(w, "process_resident_memory_bytes %d\n", residentBytes())
		fmt.Fprintf(w, "process_open_fds %d\n", openFDs())
	})
	if err := http.ListenAndServe(address, mux); err != nil {
		log.Fatalf("metrics serve: %v", err)
	}
}

// residentBytes reports VmRSS, the same number the prometheus process
// collector reports for a SeaweedFS server. Linux only, 0 elsewhere.
func residentBytes() uint64 {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(status), "\n") {
		rest, found := strings.CutPrefix(line, "VmRSS:")
		if !found {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return 0
		}
		kb, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}

// openFDs counts the entries in /proc/self/fd in small batches. Reading the
// whole directory at once would allocate one entry per open connection on
// every scrape, which shows up as heap growth in the very measurement this
// program exists to take.
func openFDs() int {
	dir, err := os.Open("/proc/self/fd")
	if err != nil {
		return 0
	}
	defer dir.Close()
	count := 0
	for {
		names, err := dir.Readdirnames(256)
		count += len(names)
		if err != nil || len(names) == 0 {
			return count
		}
	}
}
