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
	defaultConsumerGroupID = "worker-group"
	defaultWorkerMode      = "engine"
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
	mode     string
	producer *TopicProducer
}

func main() {
	requestsTopic, _ := common.GetEnv("REQUESTS_TOPIC", defaultRequestsTopic)
	executedTopic, _ := common.GetEnv("EXECUTED_TRADES_TOPIC", defaultExecutedTopic)
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

	handler := &WorkerHandler{
		mode:     mode,
		producer: NewTopicProducer(executedTopic, executedTradesProducer),
	}

	err = kafka.RunConsumerGroup(groupID, []string{requestsTopic}, handler)
	if err != nil {
		slog.Error("WORKER: error running consumer group", "error", err)
		os.Exit(1)
	}
}

func (h *WorkerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *WorkerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *WorkerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	partition := claim.Partition()
	eng := newEngine()

	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		if h.mode == "dump" {
			dumpTradeEvent(msg.Value)
			session.MarkMessage(msg, "")
			continue
		}

		req := &pb.TradeEvent{}
		if err := proto.Unmarshal(msg.Value, req); err != nil {
			slog.Error("WORKER: Failed to unmarshal trade request",
				"error", err,
				"value", msg.Value,
			)
			session.MarkMessage(msg, "")
			continue
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

		execs, err := eng.onTrade(req)
		if err != nil {
			slog.Warn("WORKER: onTrade failed",
				"error", err,
				"request_id", reqID,
				"symbol", symbol,
				"partition", partition,
				"offset", msg.Offset,
			)
			session.MarkMessage(msg, "")
			continue
		}

		if len(execs) == 0 {
			slog.Info("WORKER: accepted (no match)",
				"request_id", reqID,
				"symbol", symbol,
				"partition", partition,
				"offset", msg.Offset,
			)
			session.MarkMessage(msg, "")
			continue
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
			if err := h.producer.TPSend(ex.symbol, executedTrade); err != nil {
				slog.Error("WORKER: failed to send executed trade",
					"error", err,
					"executed-trade", executedTrade,
				)
				return err
			}

			slog.Info("WORKER: executed", "executed-trade", executedTrade.String())
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func newExecutedID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("exec-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
