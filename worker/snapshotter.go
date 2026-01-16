package main

import (
	"log/slog"
	"sync/atomic"
	"time"
)

// Snapshotter triggers periodic (or load-based) snapshot creation for a single
// trades-topic partition consumer.
//
// It creates one Kafka message per stock (key = symbol) to the log-compacted
// snapshots topic.
type Snapshotter struct {
	partition int32
	interval  time.Duration
	maxOrders int64

	eng      *engine
	producer *TopicProducer

	lastOffset      atomic.Int64
	ordersSinceSnap atomic.Int64

	triggerCh chan struct{}
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

func NewSnapshotter(partition int32, interval time.Duration, maxOrders int, eng *engine, producer *TopicProducer) *Snapshotter {
	s := &Snapshotter{
		partition: partition,
		interval:  interval,
		maxOrders: int64(maxOrders),
		eng:       eng,
		producer:  producer,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
	s.lastOffset.Store(-1)
	return s
}

func (s *Snapshotter) Start() { go s.loop() }

func (s *Snapshotter) Stop() {
	close(s.stopCh)
	<-s.stoppedCh
}

// ObserveProcessed should be called after a trade request has been fully processed
// (including emitting executed-trade events, if any).
func (s *Snapshotter) ObserveProcessed(offset int64) {
	s.lastOffset.Store(offset)

	if s.maxOrders <= 0 {
		return
	}

	if s.ordersSinceSnap.Add(1) >= s.maxOrders {
		select {
		case s.triggerCh <- struct{}{}:
		default:
		}
	}
}

func (s *Snapshotter) loop() {
	defer close(s.stoppedCh)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flush("timer")
		case <-s.triggerCh:
			s.flush("threshold")
		case <-s.stopCh:
			return
		}
	}
}

func (s *Snapshotter) flush(reason string) {
	s.ordersSinceSnap.Store(0)

	offset := s.lastOffset.Load()
	if offset < 0 {
		return
	}

	createdAt := time.Now().UTC()
	snaps := s.eng.createSnapshot(createdAt, s.partition, offset)
	if len(snaps) == 0 {
		return
	}

	for symbol, snap := range snaps {
		if err := s.producer.TPSend(symbol, snap); err != nil {
			// Next trigger will retry, since we snapshot everything anyway.
			slog.Error("WORKER: failed to publish snapshot", "error", err, "symbol", symbol, "partition", s.partition, "reason", reason)
			return
		}
	}

	slog.Info("WORKER: snapshot batch published", "partition", s.partition, "count", len(snaps), "reason", reason, "lastOffset", offset)
}
