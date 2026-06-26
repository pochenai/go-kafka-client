package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Bucket names. events holds the records keyed by a local monotonic sequence
// (the feed cursor); dedup maps (partition,offset) -> seq so a re-read of the
// same physical message (e.g. after changing group_id) is skipped; meta keeps
// the last consumed offset per partition for observability.
var (
	bktEvents = []byte("events")
	bktDedup  = []byte("dedup")
	bktMeta   = []byte("meta")
)

// eventReader is the read side of the store, used by the query/API layer.
type eventReader interface {
	Query(sinceSeq uint64, limit int) (recs []storedEvent, latestSeq uint64, err error)
	MaxSeq() (uint64, error)
}

// EventStore is the full storage contract: the write side (eventSink, defined in
// consumer.go), the read side (eventReader), and lifecycle. boltStore is the bbolt
// implementation — dropping in Pebble later just means writing another EventStore,
// nothing upstream changes.
type EventStore interface {
	eventSink
	eventReader
	io.Closer
}

// boltStore is the bbolt-backed EventStore: it deduplicates by the physical Kafka
// coordinates and exposes records as a monotonically-ordered feed an API can pull
// incrementally. bbolt gives us transactional writes (record + cursor advance commit
// together or not at all) and crash recovery for free.
type boltStore struct {
	db *bolt.DB
}

var _ EventStore = (*boltStore)(nil) // compile-time check that boltStore satisfies the contract

// openStore opens (or creates) the bbolt database at path and ensures the buckets
// exist. A short open timeout surfaces a stuck/locked file instead of hanging.
func openStore(path string) (*boltStore, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bktEvents, bktDedup, bktMeta} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) Close() error { return s.db.Close() }

// Append stores rec in a single transaction. If a record with the same
// (partition, offset) already exists it is a duplicate (typically a re-read after
// a group_id change) and the write is skipped: written is false and seq is the
// sequence the record was originally assigned. On a fresh insert it assigns the
// next sequence, persists the record, the dedup index, and the per-partition
// consumed offset, and returns that sequence with written true.
//
// Durability: bbolt fsyncs on commit, so once Append returns nil the record is on
// disk. Callers must therefore commit the Kafka offset only AFTER Append succeeds.
func (s *boltStore) Append(rec storedEvent) (seq uint64, written bool, err error) {
	dk := dedupKey(rec.Partition, rec.Offset)
	err = s.db.Update(func(tx *bolt.Tx) error {
		dedup := tx.Bucket(bktDedup)
		if existing := dedup.Get(dk); existing != nil {
			seq = binary.BigEndian.Uint64(existing)
			written = false
			return nil
		}

		events := tx.Bucket(bktEvents)
		n, err := events.NextSequence() // monotonic, persisted, the feed cursor
		if err != nil {
			return fmt.Errorf("next sequence: %w", err)
		}
		rec.Seq = n

		buf, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}
		if err := events.Put(be64(n), buf); err != nil {
			return err
		}
		if err := dedup.Put(dk, be64(n)); err != nil {
			return err
		}
		if err := tx.Bucket(bktMeta).Put(offsetKey(rec.Partition), be64(uint64(rec.Offset))); err != nil {
			return err
		}
		seq, written = n, true
		return nil
	})
	return seq, written, err
}

// Query returns records with sequence strictly greater than sinceSeq, in order,
// together with latestSeq — the sequence of the last record returned (or sinceSeq
// if none), which the caller passes back as the cursor for the next pull. limit
// caps the batch size; limit <= 0 means no cap. Reads run in a bbolt snapshot, so
// the result is a consistent point-in-time view even under concurrent writes.
func (s *boltStore) Query(sinceSeq uint64, limit int) (recs []storedEvent, latestSeq uint64, err error) {
	latestSeq = sinceSeq
	err = s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bktEvents).Cursor()
		for k, v := c.Seek(be64(sinceSeq + 1)); k != nil; k, v = c.Next() {
			var rec storedEvent
			if err := json.Unmarshal(v, &rec); err != nil {
				return fmt.Errorf("unmarshal record at seq=%d: %w", binary.BigEndian.Uint64(k), err)
			}
			recs = append(recs, rec)
			latestSeq = binary.BigEndian.Uint64(k)
			if limit > 0 && len(recs) >= limit {
				break
			}
		}
		return nil
	})
	return recs, latestSeq, err
}

// MaxSeq returns the highest sequence assigned so far (0 if the store is empty),
// letting an API report "caught up" even when a Query returns no new records.
func (s *boltStore) MaxSeq() (uint64, error) {
	var max uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		max = tx.Bucket(bktEvents).Sequence()
		return nil
	})
	return max, err
}

// dedupKey is the stable physical identity of a message: partition + offset. It is
// independent of the consumer group, so re-reading the topic under a new group_id
// produces the same key and is deduplicated.
func dedupKey(partition int, offset int64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b[0:4], uint32(partition))
	binary.BigEndian.PutUint64(b[4:12], uint64(offset))
	return b
}

// offsetKey is the meta-bucket key holding the last consumed offset of a partition.
func offsetKey(partition int) []byte {
	b := make([]byte, 8)
	copy(b[0:4], "off:")
	binary.BigEndian.PutUint32(b[4:8], uint32(partition))
	return b
}

// be64 big-endian encodes v so that byte order matches numeric order, which is
// what makes the bbolt cursor iterate sequences in ascending value order.
func be64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
