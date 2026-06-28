package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// messageReader is the subset of *kafka.Reader the consume loop depends on. Coding
// against this interface instead of the concrete reader lets a test drive the loop
// with canned messages and capture commits. *kafka.Reader satisfies it natively, so
// production needs no adapter.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// eventSink is the durable store matched events are written to. *Store is the
// production implementation; a test can pass a temp-file *Store or a fake that
// injects errors.
type eventSink interface {
	Append(rec storedEvent) (seq uint64, written bool, err error)
}

// lagReporter reports how far the consumer is behind the topic head. The kafka-backed
// implementation (kafkaLagReporter) talks to the broker; a test can substitute a fake
// so the heartbeat can be exercised without a cluster.
type lagReporter interface {
	Lag(ctx context.Context) (lag, head int64, err error)
}

// Consumer runs the fetch→filter→store→commit loop. It is decoupled from how messages
// are produced (messageReader), where they are stored (eventSink), and how lag is
// measured (lagReporter), so its logic can be unit-tested without a Kafka cluster.
type Consumer struct {
	reader    messageReader
	sink      eventSink
	targetTag int

	// Live counters, also read by the heartbeat goroutine; atomic to avoid a race.
	read, hits                           atomic.Int64
	lastOffset, lastPartition, lastMsgMs atomic.Int64
}

func NewConsumer(reader messageReader, sink eventSink, targetTag int) *Consumer {
	return &Consumer{reader: reader, sink: sink, targetTag: targetTag}
}

// Run consumes until ctx is cancelled. It commits every message's offset (matched or
// not) only after any matched message is durably stored, so the committed offset never
// runs ahead of persisted data. A durable-write failure is fatal: it crashes without
// committing so a restart re-reads from the last committed offset (dedup makes the
// redo safe).
func (c *Consumer) Run(ctx context.Context) {
	for {
		msg, err := c.reader.FetchMessage(ctx) // fetch only; we commit manually below
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("received shutdown signal, stopped. matched %d RWA_XSTOCK events this run", c.hits.Load())
				return
			}
			log.Printf("error reading message: %v", err)
			continue
		}
		c.read.Add(1)
		c.lastOffset.Store(msg.Offset)
		c.lastPartition.Store(int64(msg.Partition))
		c.lastMsgMs.Store(msg.Time.UnixMilli())

		if err := c.process(msg); err != nil {
			// Unrecoverable sink failure (e.g. disk full). Crash WITHOUT committing.
			log.Fatalf("%v", err)
		}

		// Advance the committed offset now that any matched message is durably stored.
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				log.Printf("received shutdown signal, stopped. matched %d RWA_XSTOCK events this run", c.hits.Load())
				return
			}
			log.Printf("failed to commit offset (partition=%d offset=%d): %v", msg.Partition, msg.Offset, err)
		}
	}
}

// process parses, filters, and (if it matches the target tag) stores msg. It returns a
// non-nil error only when the durable write itself fails — the caller treats that as
// fatal. Unparseable and non-matching messages are logged/skipped and return nil so
// their offset is still committed and the backlog keeps draining.
func (c *Consumer) process(msg kafka.Message) error {
	var env eventEnvelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		log.Printf("skipping unparseable message (offset=%d): %v", msg.Offset, err)
		return nil
	}
	ev := env.Data
	if !slices.Contains(ev.Tags, c.targetTag) {
		return nil // not RWA_XSTOCK, skip
	}

	fmt.Printf("Received event: %+v\n", ev) // debug print

	rec := storedEvent{
		ReceivedAt: time.Now(),
		Partition:  msg.Partition,
		Offset:     msg.Offset,
		KafkaTime:  msg.Time,
		TokenID:    ev.ID,
		IsDeleted:  ev.IsDeleted,
		Raw:        json.RawMessage(msg.Value),
	}
	seq, written, err := c.sink.Append(rec)
	if err != nil {
		return fmt.Errorf("failed to write to store (partition=%d offset=%d): %w", msg.Partition, msg.Offset, err)
	}
	if written {
		h := c.hits.Add(1)
		log.Printf("[RWA_XSTOCK] saved #%d seq=%d symbol=%s id=%s tags=%v isRwa=%v (partition=%d offset=%d)",
			h, seq, ev.Symbol, ev.ID, ev.Tags, ev.IsRwa, msg.Partition, msg.Offset)
	} else {
		log.Printf("[RWA_XSTOCK] duplicate skipped (partition=%d offset=%d, already stored at seq=%d)",
			msg.Partition, msg.Offset, seq)
	}
	return nil
}

// RunHeartbeat logs a liveness line every interval until ctx is cancelled. Unlike a
// count-based log it fires on a wall clock even when no messages flow, and it queries
// the live head each tick to show the true remaining lag (≈0 caught up, not shrinking
// stuck, shrinking catching up).
func (c *Consumer) RunHeartbeat(ctx context.Context, lag lagReporter, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			l, head, err := lag.Lag(qctx)
			cancel()
			age := "n/a"
			if ms := c.lastMsgMs.Load(); ms > 0 {
				age = time.Since(time.UnixMilli(ms)).Round(time.Second).String()
			}
			if err != nil {
				log.Printf("heartbeat: alive read=%d hits=%d lastOffset=%d(p%d) lastMsgAge=%s lag=unknown(%v)",
					c.read.Load(), c.hits.Load(), c.lastOffset.Load(), c.lastPartition.Load(), age, err)
				continue
			}
			log.Printf("heartbeat: alive read=%d hits=%d lastOffset=%d(p%d) | behind head by %d msgs (head=%d) lastMsgAge=%s",
				c.read.Load(), c.hits.Load(), c.lastOffset.Load(), c.lastPartition.Load(), l, head, age)
		}
	}
}

// kafkaLagReporter is the production lagReporter; it queries the brokers via computeLag.
type kafkaLagReporter struct {
	brokers []string
	groupID string
	topic   string
}

func (r kafkaLagReporter) Lag(ctx context.Context) (lag, head int64, err error) {
	return computeLag(ctx, r.brokers, r.groupID, r.topic)
}
