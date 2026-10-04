// Command api serves one database under one index profile for the k6 load test.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Haleralex/pg-mongo-bench/internal/httpapi"
	"github.com/Haleralex/pg-mongo-bench/internal/store"
	"github.com/Haleralex/pg-mongo-bench/internal/store/mongodb"
	"github.com/Haleralex/pg-mongo-bench/internal/store/postgres"
)

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

func openStore(ctx context.Context, profile string, poolSize int) (store.Store, error) {
	if p, ok := postgres.Lookup(profile); ok {
		return postgres.Open(ctx, env("PG_URL", "postgres://docbench:docbench-local@postgres:5432/docbench?sslmode=disable"), p, poolSize)
	}
	if p, ok := mongodb.Lookup(profile); ok {
		return mongodb.Open(env("MONGO_URL", "mongodb://mongo:27017/docbench"), p, poolSize)
	}
	known := append(postgres.Names(), mongodb.Names()...)
	return nil, fmt.Errorf("unknown BENCH_PROFILE %q; known: %s", profile, strings.Join(known, ", "))
}

func main() {
	profile := env("BENCH_PROFILE", "pg_gin_path_ops")
	poolSize := envInt("DB_POOL_SIZE", 64, 1, 512)
	requestTimeout := time.Duration(envInt("REQUEST_TIMEOUT_SECONDS", 30, 1, 30)) * time.Second
	adminTimeout := time.Duration(envInt("ADMIN_TIMEOUT_SECONDS", 1200, 30, 7200)) * time.Second

	db, err := openStore(context.Background(), profile, poolSize)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	server := &http.Server{
		Addr:              env("LISTEN_ADDR", "0.0.0.0:8080"),
		Handler:           httpapi.New(db, profile, requestTimeout, adminTimeout).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      adminTimeout + 30*time.Second,
		IdleTimeout:       90 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("profile=%s pool=%d request_timeout=%s listening=%s", profile, poolSize, requestTimeout, server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
