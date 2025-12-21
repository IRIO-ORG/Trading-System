package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"

	"github.com/IRIO_ORG/Trading-System/model"
)


func main() {
	broker := getenv("KAFKA_BROKER_ADDR", "my-kafka:9092")
	executedTopic := getenv("EXECUTED_TRADES_TOPIC", "executed-trades")
	groupID := getenv("CONSUMER_GROUP_ID", "executed-consumer")

	log.Printf("EXEC-CONSUMER: starting (kafka=%s group=%s topic=%s)", broker, groupID, executedTopic)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.MaxVersion		// to be declared
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group := mustConsumerGroup(broker, groupID, cfg)
	defer group.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	handler := &execHandler{}

	go func() {
		for err := range group.Errors() {
			log.Printf("EXEC-CONSUMER: consumer group error: %v", err)
		}
	}()

	for {
		if err := group.Consume(ctx, []string{executedTopic}, handler); err != nil {
			log.Printf("EXEC-CONSUMER: consume error: %v", err)
		}
		if ctx.Err() != nil {
			log.Printf("EXEC-CONSUMER: shutting down")
			return
		}
	}
}

type execHandler struct{}

func (h *execHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *execHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *execHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var t model.ExecutedTrade
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("EXEC-CONSUMER: invalid json (partition=%d offset=%d): %v; raw=%s", msg.Partition, msg.Offset, err, string(msg.Value))
			sess.MarkMessage(msg, "")
			continue
		}
		log.Printf("EXECUTED: tradeId=%s ticker=%s qty=%d price=%.2f buy=%s sell=%s ts=%s",
			t.TradeID, t.Ticker, t.Quantity, t.Price, t.BuyRequestID, t.SellRequestID, t.Timestamp.Format(time.RFC3339))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func mustConsumerGroup(broker, groupID string, cfg *sarama.Config) sarama.ConsumerGroup {
	var lastErr error
	for i := 0; i < 24; i++ {
		g, err := sarama.NewConsumerGroup([]string{broker}, groupID, cfg)
		if err == nil {
			return g
		}
		lastErr = err
		log.Printf("EXEC-CONSUMER: waiting for Kafka consumer group (%v)", err)
		time.Sleep(5 * time.Second)
	}
	log.Fatalf("EXEC-CONSUMER: failed to start consumer group: %v", lastErr)
	return nil
}

func getenv(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}