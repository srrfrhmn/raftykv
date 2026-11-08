// Command raftykv-server runs one raftykv node.
//
// Status: skeleton (RKV-005). It serves /metrics only. The Raft driver and
// client API arrive in later sprints (docs/DESIGN.md §16).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/srrfrhmn/raftykv/obs"
)

func main() {
	var (
		nodeID      = flag.Uint64("id", 1, "node ID")
		metricsAddr = flag.String("metrics-addr", "127.0.0.1:9090", "address for /metrics")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		slog.Error("bad -log-level", "err", err)
		os.Exit(2)
	}
	log := obs.NewLogger(os.Stderr, lvl).With(slog.Uint64(obs.KeyNode, *nodeID))

	reg := obs.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("/metrics", obs.Handler(reg))
	srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("metrics server listening", "addr", *metricsAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("stopped")
}
