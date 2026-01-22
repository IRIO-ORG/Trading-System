package main

import (
	"database/sql"
	"log/slog"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	pb "github.com/IRIO-ORG/Trading-System/proto"
)

type ExecutedTradesHandler struct {
	// A pool of connections to the database, threadsafe.
	db *sql.DB
}

func (h *ExecutedTradesHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ExecutedTradesHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ExecutedTradesHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		trade := &pb.ExecutedTradeEvent{}
		if err := proto.Unmarshal(msg.Value, trade); err != nil {
			slog.Error(
				"Error unmarshalling message value",
				"error", err,
				"value", msg.Value,
			)

			session.MarkMessage(msg, "")
			continue
		}

		insertSQL := `
		INSERT INTO trades (execution_id, symbol, price, size, bid_request_id, ask_request_id, executed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (execution_id) DO NOTHING;`

		_, err := h.db.Exec(insertSQL,
			trade.ExecutionId,
			trade.Symbol,
			trade.Price,
			trade.Size,
			trade.BidRequestId,
			trade.AskRequestId,
			trade.ExecutedAt.AsTime(),
		)

		if err != nil {
			slog.Error("Failed to insert trade", "error", err)
		} else {
			slog.Info("Persisted executed trade",
				"id", trade.ExecutionId,
				"symbol", trade.Symbol,
				"price", trade.Price,
				"size", trade.Size)
			session.MarkMessage(msg, "")
		}
	}
	return nil
}
