package kafka

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// RunConsumerGroup handles the full lifecycle of a Sarama Consumer Group,
// including connection, rebalancing loops, error draining, and graceful shutdown.
// Blocks until the ctrl+c signal is received  or the program is terminated.
func RunConsumerGroup(groupID string, topics []string, handler sarama.ConsumerGroupHandler) {
	log.Printf("Initializing Consumer Group: %s for topics: %v", groupID, topics)

	consumerGroup, err := NewConsumerGroup(groupID)
	if err != nil {
		log.Fatalf("Critical error creating consumer group: %v", err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			log.Printf("Error closing consumer group: %v", err)
		}
	}()

	// Drain the Errors channel to prevent deadlocks (required when config...Return.Errors = true).
	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("Background consumer error: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// Consume blocks until the session is terminated (e.g., by a rebalance).
			// The loop ensures the consumer automatically rejoins the group.
			if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Consumer is up and running...")

	<-sigterm
	log.Println("Shutdown signal received. Stopping consumer...")

	cancel()
	wg.Wait()
	log.Println("Consumer stopped gracefully.")
}
