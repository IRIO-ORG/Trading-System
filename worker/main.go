package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/IRIO-ORG/Trading-System/common"
	"github.com/IRIO-ORG/Trading-System/common/kafka"
	pb "github.com/IRIO-ORG/Trading-System/proto"
)

// TODO remove once config map kafka is setup
const (
	defaultRequestsTopic   = "trade-requests"
	defaultExecutedTopic   = "executed-trades"
	defaultSnapshotsTopic  = "orderbook-snapshots"
	defaultConsumerGroupID = "worker-group"
	defaultWorkerMode      = "engine"
	snapshotInterval       = 500 * time.Millisecond
)

type TopicProducer struct {
	topic    string
	producer *kafka.ProtoProducer
}

func NewTopicProducer(topic string, producer *kafka.ProtoProducer) *TopicProducer {
	return &TopicProducer{topic: topic, producer: producer}
}

func (tp *TopicProducer) TPSend(key string, msg proto.Message) error {
	return tp.producer.Send(tp.topic, key, msg)
}

// TODO once MVP is done, move to internal package
type WorkerHandler struct {
	mode             string
	snapshotTopic    string
	producer         *TopicProducer
	snapshotProducer *TopicProducer
}

func main() {
	requestsTopic, _ := common.GetEnv("REQUESTS_TOPIC", defaultRequestsTopic)
	executedTopic, _ := common.GetEnv("EXECUTED_TRADES_TOPIC", defaultExecutedTopic)
	snapshotTopic, _ := common.GetEnv("SNAPSHOTS_TOPIC", defaultSnapshotsTopic)
	groupID, _ := common.GetEnv("WORKER_GROUP_ID", defaultConsumerGroupID)
	mode, _ := common.GetEnv("WORKER_MODE", defaultWorkerMode)

	slog.Info("WORKER: Starting Trades topic worker...", "mode", mode)

	if mode != "engine" && mode != "dump" {
		slog.Warn("WORKER: Unknown mode")
		mode = defaultWorkerMode
	}

	executedTradesProducer, err := kafka.NewProtoProducer()
	if err != nil {
		slog.Error("WORKER: Critical error creating producer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := executedTradesProducer.Close(); err != nil {
			slog.Error("WORKER: Error closing producer", "error", err)
		}
	}()

	snapshotProducer, err := kafka.NewProtoProducer()
	if err != nil {
		slog.Error("WORKER: Critical error creating snapshot producer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := snapshotProducer.Close(); err != nil {
			slog.Error("WORKER: Error closing snapshot producer", "error", err)
		}
	}()

	handler := &WorkerHandler{
		mode:             mode,
		snapshotTopic:    snapshotTopic,
		producer:         NewTopicProducer(executedTopic, executedTradesProducer),
		snapshotProducer: NewTopicProducer(snapshotTopic, snapshotProducer),
	}

	err = kafka.RunConsumerGroup(groupID, []string{requestsTopic}, handler)
	if err != nil {
		slog.Error("WORKER: error running consumer group", "error", err)
		os.Exit(1)
	}
}

func (h *WorkerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *WorkerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

type PartitionHandler struct {
	partitionId int32
	eng         *engine
	worker      *WorkerHandler
}

func (h *PartitionHandler) processMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	if msg == nil {
		return nil
	}

	if h.worker.mode == "dump" {
		dumpTradeEvent(msg.Value)
		session.MarkMessage(msg, "")
		return nil
	}

	req := &pb.TradeEvent{}
	if err := proto.Unmarshal(msg.Value, req); err != nil {
		slog.Error("WORKER: Failed to unmarshal trade request",
			"error", err,
			"value", msg.Value,
		)
		session.MarkMessage(msg, "")
		return nil
	}

	slog.Info("Processing Order",
		"requestId", req.RequestId,
		"symbol", req.Trade.Instrument.Symbol,
		"side", req.Trade.Side,
		"price", req.Trade.Price,
		"size", req.Trade.Size,
	)

	reqID := req.GetRequestId()
	symbol := req.GetTrade().GetInstrument().GetSymbol()

	execs, err := h.eng.onTrade(req, func(symbol string) *orderBook {
		snapshot, err := GetSnapshotForSymbol(h.worker.snapshotTopic, symbol)
		if err != nil {
			slog.Warn("WORKER: failed to fetch snapshot", "symbol", symbol)
		}
		if snapshot == nil {
			snapshot = &pb.OrderBookSnapshot{}
		}
		return newOrderBookFromSnapshot(snapshot)
	})
	if err != nil {
		slog.Warn("WORKER: onTrade failed",
			"error", err,
			"request_id", reqID,
			"symbol", symbol,
			"partition", h.partitionId,
			"offset", msg.Offset,
		)
		session.MarkMessage(msg, "")
		return nil
	}

	if len(execs) == 0 {
		slog.Info("WORKER: accepted (no match)",
			"request_id", reqID,
			"symbol", symbol,
			"partition", h.partitionId,
			"offset", msg.Offset,
		)
		session.MarkMessage(msg, "")
		return nil
	}

	for _, ex := range execs {
		executedTrade := &pb.ExecutedTradeEvent{
			Symbol:       ex.symbol,
			Price:        ex.price,
			Size:         ex.size,
			ExecutionId:  newExecutedID(),
			BidRequestId: ex.buyID,
			AskRequestId: ex.sellID,
			ExecutedAt:   timestamppb.New(ex.execTime),
		}
		if err := h.worker.producer.TPSend(ex.symbol, executedTrade); err != nil {
			slog.Error("WORKER: failed to send executed trade",
				"error", err,
				"executed-trade", executedTrade,
			)
			return err
		}

		slog.Info("WORKER: executed", "executed-trade", executedTrade.String())
	}

	session.MarkMessage(msg, "")
	return nil
}

func (h *PartitionHandler) makeSnapshot(symbol string) error {
	book, exists := h.eng.books[symbol]
	if !exists {
		slog.Warn("Snapshot request for unknown symbol", "symbol", symbol)
		return nil
	}

	orders := make([]*pb.Order, 1)
	for _, ask := range book.asks {
		orders = append(orders, &pb.Order{
			RequestId: ask.id,
			Side:      ask.side,
			Price:     ask.price,
			Size:      ask.remaining,
			Seq:       ask.seq,
		})
	}
	for _, bid := range book.bids {
		orders = append(orders, &pb.Order{
			RequestId: bid.id,
			Side:      bid.side,
			Price:     bid.price,
			Size:      bid.remaining,
			Seq:       bid.seq,
		})
	}

	return h.worker.snapshotProducer.TPSend(symbol, &pb.OrderBookSnapshot{
		CreatedAt: timestamppb.Now(),
		Symbol:    symbol,
		NextSeq:   book.seq + 1,
		Orders:    orders,
	})
}

func (h *WorkerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partitionHandler := PartitionHandler{
		partitionId: claim.Partition(),
		eng:         newEngine(),
		worker:      h,
	}
	snapshotTicker := time.NewTicker(snapshotInterval)

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// Channel closed
				return nil
			}
			partitionHandler.processMessage(msg, session)
		case t := <-snapshotTicker.C:
			slog.Info("Starting snapshot...", "partition", partitionHandler.partitionId, "time", t)
			for symbol, _ := range partitionHandler.eng.books {
				partitionHandler.makeSnapshot(symbol)
			}
			slog.Info("Snapshot finished!", "partition", partitionHandler.partitionId)
		}
	}
}

func newExecutedID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("exec-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
