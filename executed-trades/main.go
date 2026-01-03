package main

import (
	"log/slog"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	"github.com/IRIO-ORG/Trading-System/common/kafka"
	pb "github.com/IRIO-ORG/Trading-System/proto"
)

// TODO: read from env when config map for kafka is setup
const (
	topic   = "executed-trades"
	groupID = "executed-trades-consumer-group"
)

func main() {
	slog.Info("Starting Executed Trades Consumer...")

	handler := &ExecutedTradesHandler{}

	// Blocks main() operation until receives CTRL+C/SIGTERM
	err := kafka.RunConsumerGroup(groupID, []string{topic}, handler)
	if err != nil {
		slog.Error("ERROR running consumer group", "error", err)
	}
}

// TODO: move to internal package
type ExecutedTradesHandler struct{}

func (h *ExecutedTradesHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ExecutedTradesHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ExecutedTradesHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		trade := &pb.ExecutedTradeEvent{}
		if err := proto.Unmarshal(msg.Value, trade); err != nil {
			slog.Error(
				"Error unmarshalling message value",
				"value", msg.Value,
				"error", err,
			)
			session.MarkMessage(msg, "")
			continue
		}

		slog.Info(
			"Executed trade",
			"symbol", trade.Symbol,
			"price", trade.Price,
			"size", trade.Size,
		)
		session.MarkMessage(msg, "")
	}
	return nil
}
