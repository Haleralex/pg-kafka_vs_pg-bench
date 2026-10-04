// Package httpapi exposes a Store over the HTTP contract used by k6 and cmd/bench.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/event"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
)

type Server struct {
	store          store.Store
	profile        string
	requestTimeout time.Duration
	adminTimeout   time.Duration
	// Admin operations take the write side so that no load overlaps a reseed.
	gate sync.RWMutex
}

func New(s store.Store, profile string, requestTimeout, adminTimeout time.Duration) *Server {
	return &Server{store: s, profile: profile, requestTimeout: requestTimeout, adminTimeout: adminTimeout}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /read", s.read)
	mux.HandleFunc("POST /write", s.write)
	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /explain", s.explain)
	mux.HandleFunc("POST /admin/seed", s.seed)
	mux.HandleFunc("POST /admin/maintain", s.maintain)
	return mux
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("response encoding: %v", err)
	}
}

func fail(w http.ResponseWriter, status int, err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	respond(w, status, map[string]any{"error": err.Error()})
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("body must contain exactly one JSON object")
	}
	return nil
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		fail(w, http.StatusServiceUnavailable, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"status": "ok", "profile": s.profile})
}

func (s *Server) read(w http.ResponseWriter, r *http.Request) {
	query, err := ParseReadQuery(r.URL.Query())
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	events, duration, err := s.store.Read(ctx, query)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"events": events, "count": len(events), "db_ms": millis(duration)})
}

func (s *Server) write(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StartID int64 `json:"start_id"`
		Count   int   `json:"count"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	if body.Count > 1000 || !event.ValidRange(body.StartID, body.Count) {
		fail(w, http.StatusBadRequest, fmt.Errorf("count must be 1..1000 and ids must be 1..%d", event.MaxID))
		return
	}
	// Generation is excluded from db_ms, but remains part of end-to-end HTTP latency.
	events := event.Batch(body.StartID, body.Count)
	s.gate.RLock()
	defer s.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	duration, err := s.store.Write(ctx, events)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"count": body.Count, "db_ms": millis(duration)})
}

func (s *Server) explain(w http.ResponseWriter, r *http.Request) {
	query, err := ParseReadQuery(r.URL.Query())
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	plan, err := s.store.Explain(ctx, query)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, plan)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	stats, err := s.store.Stats(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, stats)
}

func (s *Server) seed(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Count     int `json:"count"`
		BatchSize int `json:"batch_size"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	if body.BatchSize == 0 {
		body.BatchSize = 1000
	}
	if body.Count < 1 || body.Count > 2_000_000 || body.BatchSize < 1 || body.BatchSize > 1000 {
		fail(w, http.StatusBadRequest, fmt.Errorf("count must be 1..2000000 and batch_size must be 1..1000"))
		return
	}
	s.gate.Lock()
	defer s.gate.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), s.adminTimeout)
	defer cancel()
	start := time.Now()
	if err := s.store.Reset(ctx); err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	for first := 1; first <= body.Count; first += body.BatchSize {
		count := min(body.BatchSize, body.Count-first+1)
		if _, err := s.store.Write(ctx, event.Batch(int64(first), count)); err != nil {
			fail(w, http.StatusInternalServerError, fmt.Errorf("seed failed at id %d: %w", first, err))
			return
		}
	}
	maintenance, err := s.store.Maintain(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"count": body.Count, "elapsed_ms": millis(time.Since(start)), "profile": s.profile, "base_time_ms": event.BaseTime.UnixMilli(), "first_id": 1, "last_id": body.Count, "maintenance": maintenance})
}

func (s *Server) maintain(w http.ResponseWriter, r *http.Request) {
	s.gate.Lock()
	defer s.gate.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), s.adminTimeout)
	defer cancel()
	result, err := s.store.Maintain(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, result)
}
