package main

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
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
	// The Kafka client used by snapshot topic consumers
	snapshotClient sarama.Client

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

	s.eng.mut.Lock()
	offset := s.lastOffset.Load()
	if offset < 0 {
		s.eng.mut.Unlock()
		return
	}
	createdAt := time.Now().UTC()
	snaps := s.eng.createSnapshotLocked(createdAt, s.partition, offset)
	s.eng.mut.Unlock()
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

func (s *Snapshotter) findSnapshotInPartition(symbol string, topic string, partition int32, newestOffset int64, topicConsumer sarama.Consumer) (*pb.OrderBookSnapshot, error) {
	if newestOffset == 0 {
		// Partition is empty - no snapshots
		return nil, nil
	}
	pc, err := topicConsumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("failed to start partition consumer: %w", err)
	}
	defer pc.Close()

	var latestSnapshot *pb.OrderBookSnapshot = nil
	var lastOffset int64 = -1
	for lastOffset < newestOffset-1 {
		select {
		case msg := <-pc.Messages():
			lastOffset = msg.Offset
			if string(msg.Key) != symbol {
				continue
			}
			snapshot := &pb.OrderBookSnapshot{}
			if err := proto.Unmarshal(msg.Value, snapshot); err != nil {
				slog.Warn("Error unmarshalling snapshot", "symbol", symbol, "error", err)
				continue
			}
			// Log compaction runs asynchronously, so there may be outdated snapshots
			if latestSnapshot == nil || latestSnapshot.OrderbookSeq < snapshot.OrderbookSeq || latestSnapshot.CreatedAt.AsTime().Before(snapshot.CreatedAt.AsTime()) {
				latestSnapshot = snapshot
			}

		case err := <-pc.Errors():
			return nil, fmt.Errorf("consumer error: %w", err)
		}
	}
	return latestSnapshot, nil
}

func (s *Snapshotter) GetSnapshotForSymbol(symbol string, topic string, client sarama.Client) (*pb.OrderBookSnapshot, error) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("NewConsumerFromClient error: %v", err)
	}
	defer consumer.Close()
	watermarksPerTopicAndPartition := consumer.HighWaterMarks()
	watermarks, exists := watermarksPerTopicAndPartition[topic]
	if !exists {
		return nil, nil
	}

	var latestSnapshot *pb.OrderBookSnapshot = nil
	for partition, watermark := range watermarks {
		snapshot, err := s.findSnapshotInPartition(symbol, topic, partition, watermark, consumer)
		if err != nil {
			slog.Warn("Error when looking for snapshot in partition", "symbol", symbol, "topic", topic, "partition", partition)
			continue
		}
		if snapshot == nil {
			continue
		}
		if latestSnapshot == nil || latestSnapshot.OrderbookSeq < snapshot.OrderbookSeq || latestSnapshot.CreatedAt.AsTime().Before(snapshot.CreatedAt.AsTime()) {
			latestSnapshot = snapshot
		}
	}
	return latestSnapshot, nil
}
