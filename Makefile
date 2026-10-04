BENCH := go run ./cmd/bench

.PHONY: test smoke bench flex flex-smoke summary down

## test: static checks and unit tests; no Docker required
test:
	go vet ./...
	go test ./...

## smoke: two-minute end-to-end check of every default profile
smoke:
	$(BENCH) run -seed 3000 -rates 10,30 -warmup 2s -step 5s -transition 2s $(ARGS)

## bench: the full experiment, three passes in alternating profile order
bench:
	$(BENCH) run -repetitions 3 $(ARGS)

FLEX := -profiles pg_gin_path_ops,mongo_wildcard,pg_targeted,mongo_targeted 	-read-kinds adhoc,trace,adhoc_count -rates 100,200,300

## flex: "index everything" profiles on queries no targeted index was built for
flex:
	$(BENCH) run $(FLEX) -repetitions 3 $(ARGS)

## flex-smoke: two-minute check of the flex experiment
flex-smoke:
	$(BENCH) run $(FLEX) -seed 3000 -rates 10,30 -warmup 2s -step 5s -transition 2s $(ARGS)

## summary: per-run CSV and median table from results/
summary:
	$(BENCH) summarize $(ARGS)

## down: remove the benchmark containers and their database volumes
down:
	docker compose --profile load down -v
