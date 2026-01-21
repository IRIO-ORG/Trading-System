package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/IRIO-ORG/Trading-System/common"
)

// GetBrokers returns a list of Kafka brokers from the environment variable KAFKA_BROKER_ADDR.
func GetBrokers() []string {
	addr, _ := common.GetEnv("KAFKA_BROKER_ADDR", "localhost:9092")
	// Split on comma to allow for multiple brokers
	return strings.Split(addr, ",")
}

// NewProducer creates a SyncProducer with a reliable configuration and connection retry mechanism.
func NewProducer() (sarama.SyncProducer, error) {
	brokers := GetBrokers()

	config := sarama.NewConfig()
	// The producer waits for the message to be committed by the broker.
	config.Producer.Return.Successes = true
	// WaitForAll ensures the message is committed by the leader AND all in-sync replicas.
	config.Producer.RequiredAcks = sarama.WaitForAll
	// The producer retries up to 5 times before giving up.
	config.Producer.Retry.Max = 5

	var prod sarama.SyncProducer
	var err error

	for i := 0; i < 10; i++ {
		prod, err = sarama.NewSyncProducer(brokers, config)
		if err == nil {
			return prod, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("failed to start producer after retries: %w", err)
}

// NewConsumerGroup creates a Consumer Group with a retry mechanism and reliable offsets.
func NewConsumerGroup(groupID string) (sarama.ConsumerGroup, error) {
	brokers := GetBrokers()

	config := sarama.NewConfig()
	// The consumer returns errors to the Errors() channel.
	// Has to be handled, otherwise deadlocks may occur.
	config.Consumer.Return.Errors = true
	// OffsetOldest ensures we process messages sent while the consumer was down.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// For k8s efficiency, reduces partition movement.
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategySticky()

	var cg sarama.ConsumerGroup
	var err error

	for i := 0; i < 10; i++ {
		cg, err = sarama.NewConsumerGroup(brokers, groupID, config)
		if err == nil {
			return cg, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("failed to start consumer group after retries: %w", err)
}
