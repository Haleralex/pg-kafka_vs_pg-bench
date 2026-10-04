package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type api struct {
	store          Store
	profile        string
	requestTimeout time.Duration
	adminTimeout   time.Duration
	gate           sync.RWMutex
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback, minimum, maximum int) int {
	value, err := strconv.Atoi(env(name, strconv.Itoa(fallback)))
	if err != nil || value < minimum || value > maximum {
		log.Fatalf("%s must be an integer between %d and %d", name, minimum, maximum)
	}
	return value
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

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("GET /read", a.read)
	mux.HandleFunc("POST /write", a.write)
	mux.HandleFunc("GET /stats", a.stats)
	mux.HandleFunc("GET /explain", a.explain)
	mux.HandleFunc("POST /admin/seed", a.seed)
	mux.HandleFunc("POST /admin/maintain", a.maintain)
	return mux
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		fail(w, http.StatusServiceUnavailable, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"status": "ok", "profile": a.profile})
}

func (a *api) read(w http.ResponseWriter, r *http.Request) {
	query, err := parseReadQuery(r.URL.Query())
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	a.gate.RLock()
	defer a.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), a.requestTimeout)
	defer cancel()
	events, duration, err := a.store.Read(ctx, query)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"events": events, "count": len(events), "db_ms": millis(duration)})
}

func (a *api) write(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StartID int64 `json:"start_id"`
		Count   int   `json:"count"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	if body.Count > 1000 || !validIDRange(body.StartID, body.Count) {
		fail(w, http.StatusBadRequest, fmt.Errorf("count must be 1..1000 and ids must be 1..%d", maxEventID))
		return
	}
	// Generation is excluded from db_ms, but remains part of end-to-end HTTP latency.
	events := generateBatch(body.StartID, body.Count)
	a.gate.RLock()
	defer a.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), a.requestTimeout)
	defer cancel()
	duration, err := a.store.Write(ctx, events)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"count": body.Count, "db_ms": millis(duration)})
}

func (a *api) explain(w http.ResponseWriter, r *http.Request) {
	query, err := parseReadQuery(r.URL.Query())
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	a.gate.RLock()
	defer a.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), a.requestTimeout)
	defer cancel()
	plan, err := a.store.Explain(ctx, query)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, plan)
}

func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	a.gate.RLock()
	defer a.gate.RUnlock()
	ctx, cancel := context.WithTimeout(r.Context(), a.requestTimeout)
	defer cancel()
	stats, err := a.store.Stats(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, stats)
}

func (a *api) seed(w http.ResponseWriter, r *http.Request) {
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
	a.gate.Lock()
	defer a.gate.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), a.adminTimeout)
	defer cancel()
	start := time.Now()
	if err := a.store.Reset(ctx); err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	for first := 1; first <= body.Count; first += body.BatchSize {
		count := min(body.BatchSize, body.Count-first+1)
		if _, err := a.store.Write(ctx, generateBatch(int64(first), count)); err != nil {
			fail(w, http.StatusInternalServerError, fmt.Errorf("seed failed at id %d: %w", first, err))
			return
		}
	}
	maintenance, err := a.store.Maintain(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, map[string]any{"count": body.Count, "elapsed_ms": millis(time.Since(start)), "profile": a.profile, "base_time_ms": baseTime.UnixMilli(), "first_id": 1, "last_id": body.Count, "maintenance": maintenance})
}

func (a *api) maintain(w http.ResponseWriter, r *http.Request) {
	a.gate.Lock()
	defer a.gate.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), a.adminTimeout)
	defer cancel()
	result, err := a.store.Maintain(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	respond(w, http.StatusOK, result)
}

func main() {
	profile := env("BENCH_PROFILE", "pg_gin_path_ops")
	poolSize := envInt("DB_POOL_SIZE", 64, 1, 512)
	requestTimeout := time.Duration(envInt("REQUEST_TIMEOUT_SECONDS", 30, 1, 30)) * time.Second
	adminTimeout := time.Duration(envInt("ADMIN_TIMEOUT_SECONDS", 1200, 30, 7200)) * time.Second
	var store Store
	var err error
	switch profile {
	case "pg_gin_path_ops", "pg_gin_ops", "pg_targeted":
		store, err = newPostgres(context.Background(), env("PG_URL", "postgres://bench:bench@postgres:5432/docbench?sslmode=disable"), profile, poolSize)
	case "mongo_targeted":
		store, err = newMongo(env("MONGO_URL", "mongodb://mongo:27017/docbench"), poolSize)
	default:
		log.Fatalf("unknown BENCH_PROFILE %q", profile)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	a := &api{store: store, profile: profile, requestTimeout: requestTimeout, adminTimeout: adminTimeout}
	server := &http.Server{Addr: env("LISTEN_ADDR", "0.0.0.0:8080"), Handler: a.routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: adminTimeout + 30*time.Second, IdleTimeout: 90 * time.Second}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopped
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("profile=%s pool=%d request_timeout=%s listening=%s", profile, poolSize, requestTimeout, server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
