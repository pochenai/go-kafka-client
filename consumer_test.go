package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// mockReader implements messageReader: it hands out canned messages in order, then
// cancels the run via onDrain so Consumer.Run returns. Every committed offset is
// recorded so a test can assert the commit behaviour.
type mockReader struct {
	msgs      []kafka.Message
	idx       int
	committed []int64
	onDrain   context.CancelFunc
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.idx >= len(m.msgs) {
		m.onDrain()                       // no more input: cancel ctx...
		return kafka.Message{}, ctx.Err() // ...and report it so Run exits
	}
	msg := m.msgs[m.idx]
	m.idx++
	return msg, nil
}

func (m *mockReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, msg := range msgs {
		m.committed = append(m.committed, msg.Offset)
	}
	return nil
}

// failingSink is an eventSink whose Append always fails, to exercise the
// durable-write error path without bbolt.
type failingSink struct{ err error }

func (f failingSink) Append(storedEvent) (seq uint64, written bool, err error) {
	return 0, false, f.err
}

// msg builds a Kafka message whose envelope carries the given tags.
func msg(partition int, offset int64, tags ...int) kafka.Message {
	env := eventEnvelope{Data: tokenEvent{Tags: tags, Symbol: "SYM", ID: "id"}}
	b, _ := json.Marshal(env)
	return kafka.Message{Partition: partition, Offset: offset, Value: b, Time: time.Unix(1, 0)}
}

func newTestStore(t *testing.T) *boltStore {
	t.Helper()
	s, err := openStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Drives the full loop through the mock reader: only tag-36 messages are stored, but
// every message's offset is committed so the backlog keeps draining.
func TestConsumerFiltersAndCommitsAll(t *testing.T) {
	store := newTestStore(t)
	in := []kafka.Message{
		msg(0, 100, 36),    // match
		msg(0, 101, 1, 2),  // no match
		msg(0, 102),        // no tags
		msg(0, 103, 7, 36), // match
	}

	ctx, cancel := context.WithCancel(context.Background())
	mr := &mockReader{msgs: in, onDrain: cancel}
	NewConsumer(mr, store, 36).Run(ctx)

	// Every offset committed, in order — lag clears even for filtered messages.
	want := []int64{100, 101, 102, 103}
	if len(mr.committed) != len(want) {
		t.Fatalf("committed %v, want %v", mr.committed, want)
	}
	for i, off := range want {
		if mr.committed[i] != off {
			t.Fatalf("committed %v, want %v", mr.committed, want)
		}
	}

	// Only the two matches landed, with sequential seqs.
	recs, latest, err := store.Query(0, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("stored %d records, want 2", len(recs))
	}
	if recs[0].Offset != 100 || recs[1].Offset != 103 {
		t.Fatalf("stored offsets %d,%d, want 100,103", recs[0].Offset, recs[1].Offset)
	}
	if recs[0].Seq != 1 || recs[1].Seq != 2 || latest != 2 {
		t.Fatalf("seqs %d,%d latest=%d, want 1,2 latest=2", recs[0].Seq, recs[1].Seq, latest)
	}
}

// A message re-delivered at the same (partition, offset) — e.g. after a group_id change
// or an uncommitted-but-stored redo — is stored once and skipped the second time.
func TestConsumerDeduplicatesReplay(t *testing.T) {
	store := newTestStore(t)
	in := []kafka.Message{
		msg(0, 100, 36),
		msg(0, 100, 36), // exact replay
	}

	ctx, cancel := context.WithCancel(context.Background())
	mr := &mockReader{msgs: in, onDrain: cancel}
	NewConsumer(mr, store, 36).Run(ctx)

	recs, _, err := store.Query(0, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("stored %d records, want 1 (replay should dedup)", len(recs))
	}
}

// A durable-write failure must surface as an error from process (which Run treats as
// fatal), never be swallowed — otherwise the offset would be committed past data that
// was never persisted. process is called directly to avoid Run's log.Fatalf.
func TestConsumerProcessReportsSinkFailure(t *testing.T) {
	sentinel := errors.New("disk full")
	c := NewConsumer(&mockReader{}, failingSink{err: sentinel}, 36)

	// A matching message must be stored; the sink failure has to propagate.
	err := c.process(msg(0, 100, 36))
	if err == nil {
		t.Fatal("process returned nil, want error on sink failure")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("process error = %v, want it to wrap %v", err, sentinel)
	}

	// A non-matching message never reaches the sink, so it must not error.
	if err := c.process(msg(0, 101, 1)); err != nil {
		t.Fatalf("non-matching message must not touch the sink, got %v", err)
	}
}
