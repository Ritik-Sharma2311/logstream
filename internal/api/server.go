// Package api exposes the HTTP interface for ingesting and searching logs.
package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Ritik-Sharma2311/logstream/internal/index"
	"github.com/Ritik-Sharma2311/logstream/internal/ingest"
	"github.com/Ritik-Sharma2311/logstream/internal/model"
)

const maxLineBytes = 1 << 20

// Server wires HTTP handlers to the ingest pipeline and the index.
type Server struct {
	pipe *ingest.Pipeline
	idx  *index.Index
}

// New returns a Server backed by pipe and idx.
func New(pipe *ingest.Pipeline, idx *index.Index) *Server {
	return &Server{pipe: pipe, idx: idx}
}

// Handler returns the routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /ingest", s.handleIngest)
	mux.HandleFunc("GET /search", s.handleSearch)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// handleIngest reads newline-delimited JSON entries. If the queue fills
// mid-request it answers 429 with the count already accepted, so the client
// can resend only the remainder.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)

	accepted := 0
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e model.Entry
		if err := json.Unmarshal(line, &e); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json line", "accepted": accepted})
			return
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = time.Now().UTC()
		}
		if err := s.pipe.Submit(e); err != nil {
			status := http.StatusTooManyRequests
			if errors.Is(err, ingest.ErrClosed) {
				status = http.StatusServiceUnavailable
			}
			w.Header().Set("Retry-After", "1")
			writeJSON(w, status, map[string]any{"error": err.Error(), "accepted": accepted})
			return
		}
		accepted++
	}
	if err := sc.Err(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "accepted": accepted})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": accepted})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	limit, _ := strconv.Atoi(qs.Get("limit"))
	q := index.Query{
		Text:    qs.Get("q"),
		Level:   qs.Get("level"),
		Service: qs.Get("service"),
		Limit:   limit,
	}
	res := s.idx.Search(q)
	if res == nil {
		res = []model.Entry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(res), "results": res})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"pipeline": s.pipe.Stats(),
		"indexed":  s.idx.Len(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
