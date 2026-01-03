package main

import (
	"log"

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
	log.Println("Starting Executed Trades Consumer...")

	handler := &ExecutedTradesHandler{}

	// Blocks main() operation until receives CTRL+C/SIGTERM
	kafka.RunConsumerGroup(groupID, []string{topic}, handler)
}

// TODO: move to internal package
type ExecutedTradesHandler struct{}

func (h *ExecutedTradesHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ExecutedTradesHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ExecutedTradesHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		trade := &pb.ExecutedTradeEvent{}
		if err := proto.Unmarshal(msg.Value, trade); err != nil {
			log.Printf("ERROR unmarshalling: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		log.Printf(">>> EXECUTED TRADE: %s | Price: %d | Size: %d", trade.Symbol, trade.Price, trade.Size)
		session.MarkMessage(msg, "")
	}
	return nil
}
