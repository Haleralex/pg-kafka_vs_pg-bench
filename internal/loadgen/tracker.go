package loadgen

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// tracker hands out sequence numbers and checks every consumed message against
// them: nothing may be lost, duplicates are counted.
type tracker struct {
	next       atomic.Uint64 // next unreserved sequence number
	acked      atomic.Int64  // messages whose Send succeeded
	unique     atomic.Int64
	duplicates atomic.Int64
	seen       []atomic.Uint64 // bitset over sequence numbers
}

func newTracker(capacity uint64) *tracker {
	return &tracker{seen: make([]atomic.Uint64, capacity/64+1)}
}

func (t *tracker) capacity() uint64 { return uint64(len(t.seen)) * 64 }

// reserve claims up to n sequence numbers below limit and returns the first one
// and how many were claimed; 0 means limit is reached.
func (t *tracker) reserve(n int, limit uint64) (uint64, int) {
	limit = min(limit, t.capacity())
	for {
		first := t.next.Load()
		if first >= limit {
			return 0, 0
		}
		count := min(uint64(n), limit-first)
		if t.next.CompareAndSwap(first, first+count) {
			return first, int(count)
		}
	}
}

// mark records a consumed message and reports whether it was seen before.
func (t *tracker) mark(seq uint64) (duplicate bool, err error) {
	if seq >= t.capacity() {
		return false, fmt.Errorf("sequence %d was never produced", seq)
	}
	bit := uint64(1) << (seq % 64)
	if t.seen[seq/64].Or(bit)&bit != 0 {
		t.duplicates.Add(1)
		return true, nil
	}
	t.unique.Add(1)
	return false, nil
}

// backlog is acknowledged but not yet consumed. A consumer can see a message
// before its producer gets the acknowledgement, hence the clamp.
func (t *tracker) backlog() int64 { return max(0, t.acked.Load()-t.unique.Load()) }

// samples collects durations per goroutine; each owner appends to its own shard.
type samples struct {
	recording atomic.Bool
	shards    []shard
}

type shard struct {
	mu     sync.Mutex
	values []time.Duration
}

func newSamples(shards int) *samples { return &samples{shards: make([]shard, shards)} }

func (s *samples) add(i int, d time.Duration) {
	if !s.recording.Load() {
		return
	}
	sh := &s.shards[i]
	sh.mu.Lock()
	sh.values = append(sh.values, d)
	sh.mu.Unlock()
}

// take returns everything recorded so far and empties the shards.
func (s *samples) take() []time.Duration {
	var all []time.Duration
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		all = append(all, sh.values...)
		sh.values = sh.values[:0]
		sh.mu.Unlock()
	}
	return all
}

// Latency summarizes a distribution in milliseconds.
type Latency struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P999  float64 `json:"p999_ms"`
	Max   float64 `json:"max_ms"`
}

func summarizeLatency(values []time.Duration) Latency {
	if len(values) == 0 {
		return Latency{}
	}
	slices.Sort(values)
	at := func(p float64) float64 {
		i := max(0, int(math.Ceil(p*float64(len(values))))-1) // nearest rank
		return float64(values[min(i, len(values)-1)]) / float64(time.Millisecond)
	}
	return Latency{Count: len(values), P50: at(0.50), P95: at(0.95), P99: at(0.99), P999: at(0.999), Max: at(1)}
}
