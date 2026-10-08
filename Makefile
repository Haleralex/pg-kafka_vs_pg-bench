BENCH := go run ./cmd/bench

.PHONY: test smoke bench summary report monitor down

## test: static checks and unit tests; no Docker required
test:
	go vet ./...
	go test ./...

## smoke: few-minute end-to-end check of every profile
smoke:
	$(BENCH) run -backlog 20000 -rates 500,2000 -step-warmup 2s -step 5s $(ARGS)

## bench: the full experiment, three passes in alternating profile order
bench:
	$(BENCH) run -repetitions 3 $(ARGS)

## summary: per-run CSV and median tables from results/
summary:
	$(BENCH) summarize $(ARGS)

## report: results/report.html with charts, served on port 8090 (Ctrl+C stops)
report:
	$(BENCH) report -serve :8090 $(ARGS)

## monitor: start Prometheus and Grafana alone, e.g. to look at a past run
monitor:
	docker compose --profile monitor up -d --wait prometheus grafana

## down: remove the benchmark containers and their volumes
down:
	docker compose --profile load --profile monitor down -v --remove-orphans
