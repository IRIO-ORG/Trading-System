package main

import (
	"log/slog"
	"os"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	"github.com/IRIO-ORG/Trading-System/common/kafka"
	pb "github.com/IRIO-ORG/Trading-System/proto"
)

// TODO remove once config map kafka is setup
const (
	requestTopic        = "trade-requests"
	executedTradesTopic = "executed-trades"
	consumerGroupID     = "worker-group"
)

func main() {
	slog.Info("Starting Trades topic worker...")

	executedTradesProducer, err := kafka.NewProtoProducer()
	if err != nil {
		slog.Error("Critical error creating producer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := executedTradesProducer.Close(); err != nil {
			slog.Error("Error closing producer", "error", err)
		}
	}()

	handler := &WorkerHandler{
		producer: executedTradesProducer,
	}

	err = kafka.RunConsumerGroup(consumerGroupID, []string{requestTopic}, handler)
	if err != nil {
		slog.Error("ERROR running consumer group", "error", err)
	}
}

// TODO once MVP is done, move to internal package
type WorkerHandler struct {
	producer *kafka.ProtoProducer
}

func (h *WorkerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *WorkerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *WorkerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		req := &pb.TradeEvent{}
		if err := proto.Unmarshal(msg.Value, req); err != nil {
			slog.Error("Failed to unmarshal trade request",
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

		//TODO business logic
		// TODO mieć na uwadze fail przy wysyłaniu i restart processingu linia 93
		executedTrade := &pb.ExecutedTradeEvent{
			Symbol:       "Placeholder",
			Price:        1,
			Size:         1,
			ExecutionId:  "placeholder",
			BidRequestId: "placeholder",
			AskRequestId: "placeholder",
		}

		err := h.producer.Send(executedTradesTopic, "", executedTrade)
		if err != nil {
			slog.Error("Failed to send executed trade",
				"error", err,
				"executed-trade", executedTrade,
			)
			// Mark message does not happen, and we return the error, thus the message will be reprocessed
			// because the consumer will not commit the offset and will receive the message again after
			// restart in the consumer_runner.go loop
			return err
		}

		// Do usunięcia po zrozumieniu: oznaczenie wiadomości jako przetworzonej
		// to zachowuje się w pamięci ram workera i co jakiś czas (domyślnie 1s) worker
		// aktualizuje kafce, że ta wiadomość została skonsumowana
		session.MarkMessage(msg, "")
	}
	return nil
}
