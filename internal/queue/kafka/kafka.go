// Package kafka is the same work queue as a Kafka topic read by one consumer
// group: one partition per worker, offsets committed after each batch.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Haleralex/pg-mongo-bench/internal/queue"
)

// Config is one Kafka profile.
type Config struct {
	Brokers []string
	// Fsync sets flush.messages=1 on the topic: the broker fsyncs the log before
	// acknowledging, like PostgreSQL with synchronous_commit=on. Kafka's default
	// relies on replication instead, and this cluster has a single broker.
	Fsync  bool
	Linger time.Duration
}

type Backend struct {
	cfg      Config
	admin    *kadm.Client
	producer *kgo.Client
	topic    string
}

func New(ctx context.Context, cfg Config) (*Backend, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return nil, err
	}
	admin := kadm.NewClient(cl)
	if _, err := admin.ApiVersions(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka is not reachable: %w", err)
	}
	return &Backend{cfg: cfg, admin: admin}, nil
}

// Reset creates a fresh topic and consumer group instead of deleting the old
// ones: topic deletion in Kafka is asynchronous.
func (b *Backend) Reset(ctx context.Context, consumers int) error {
	b.topic = "queue-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	configs := map[string]*string{}
	if b.cfg.Fsync {
		configs["flush.messages"] = kadm.StringPtr("1")
	}
	resp, err := b.admin.CreateTopic(ctx, int32(consumers), 1, configs, b.topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return fmt.Errorf("create topic %s: %w", b.topic, err)
	}
	if b.producer != nil {
		b.producer.Close()
	}
	b.producer, err = kgo.NewClient(
		kgo.SeedBrokers(b.cfg.Brokers...),
		kgo.DefaultProduceTopic(b.topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(b.cfg.Linger),
		// Payloads are random; compression would only cost CPU.
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	return err
}

func (b *Backend) Producer() queue.Producer { return b }

// Send waits until every record of the batch is acknowledged by the broker.
func (b *Backend) Send(ctx context.Context, payloads [][]byte) error {
	records := make([]*kgo.Record, len(payloads))
	for i, p := range payloads {
		records[i] = &kgo.Record{Value: p}
	}
	return b.producer.ProduceSync(ctx, records...).FirstErr()
}

func (b *Backend) NewConsumer(context.Context) (queue.Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.cfg.Brokers...),
		kgo.ConsumeTopics(b.topic),
		kgo.ConsumerGroup(b.topic),
		kgo.DisableAutoCommit(),
		// Partitions are not reassigned between poll and commit, which would
		// hand already processed records to another worker.
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, err
	}
	return consumer{cl: cl}, nil
}

func (b *Backend) Blocking() bool { return true }

func (b *Backend) Stats(ctx context.Context) (map[string]any, error) {
	dirs, err := b.admin.DescribeAllLogDirs(ctx, kadm.TopicsSet{b.topic: nil})
	if err != nil {
		return nil, fmt.Errorf("kafka stats: %w", err)
	}
	var size int64
	for _, broker := range dirs {
		size += broker.Size()
	}
	return map[string]any{"topic": b.topic, "log_bytes": size, "flush_messages_1": b.cfg.Fsync}, nil
}

func (b *Backend) Close() {
	if b.producer != nil {
		b.producer.Close()
	}
	b.admin.Close()
}

type consumer struct {
	cl *kgo.Client
}

func (c consumer) Receive(ctx context.Context, limit int, handle func([]byte)) (int, error) {
	defer c.cl.AllowRebalance()
	fetches := c.cl.PollRecords(ctx, limit)
	for _, fe := range fetches.Errors() {
		if !errors.Is(fe.Err, context.Canceled) && !errors.Is(fe.Err, context.DeadlineExceeded) {
			return 0, fe.Err
		}
	}
	n := 0
	fetches.EachRecord(func(r *kgo.Record) {
		handle(r.Value)
		n++
	})
	if n == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return n, c.cl.CommitUncommittedOffsets(ctx)
}

func (c consumer) Close() { c.cl.Close() }
