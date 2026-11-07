// Package obs holds logging and metrics setup (docs/DESIGN.md §13).
package obs

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Standard log attribute keys. Every Raft log line SHOULD carry them.
const (
	KeyNode  = "node"
	KeyGroup = "group"
	KeyTerm  = "term"
	KeyIndex = "index"
)

// NewLogger returns a JSON logger that writes to w at level.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// ForGroup returns l with the node and group attributes set.
func ForGroup(l *slog.Logger, node, group uint64) *slog.Logger {
	return l.With(slog.Uint64(KeyNode, node), slog.Uint64(KeyGroup, group))
}

// Pos returns the term and index attributes for one log line.
func Pos(term, index uint64) slog.Attr {
	return slog.Group("", slog.Uint64(KeyTerm, term), slog.Uint64(KeyIndex, index))
}

// NewRegistry returns a registry with Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return r
}

// Handler returns the /metrics HTTP handler for r.
func Handler(r *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(r, promhttp.HandlerOpts{Registry: r})
}
