package obs

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestLoggerFields(t *testing.T) {
	var buf bytes.Buffer
	l := ForGroup(NewLogger(&buf, slog.LevelInfo), 3, 7)
	l.Info("became leader", Pos(5, 42))
	l.Debug("hidden")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1: %q", len(lines), buf.String())
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]float64{KeyNode: 3, KeyGroup: 7, KeyTerm: 5, KeyIndex: 42} {
		if got, ok := m[k].(float64); !ok || got != want {
			t.Errorf("%s = %v, want %v", k, m[k], want)
		}
	}
}

func TestMetricsHandler(t *testing.T) {
	r := NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "raftykv_test_total", Help: "test"})
	r.MustRegister(c)
	c.Add(3)

	srv := httptest.NewServer(Handler(r))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"raftykv_test_total 3", "go_goroutines"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
