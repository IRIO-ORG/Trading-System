package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// RunConsumerGroup handles the full lifecycle of a Sarama Consumer Group,
// including connection, rebalancing loops, error draining, and graceful shutdown.
// Blocks until the ctrl+c signal is received  or the program is terminated.
func RunConsumerGroup(groupID string, topics []string, handler sarama.ConsumerGroupHandler) error {
	slog.Info("Initializing Consumer Group for topics", "groupID", groupID, "topics", topics)

	consumerGroup, err := NewConsumerGroup(groupID)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %v", err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			slog.Error("Error closing consumer group", "error", err)
		}
	}()

	// Drain the Errors channel to prevent deadlocks (required when config...Return.Errors = true).
	go func() {
		for err := range consumerGroup.Errors() {
			slog.Error("Background consumer error", "error", err)
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
				slog.Error("Error from consumer", "error", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	slog.Info("Consumer is up and running...")

	<-sigterm
	slog.Info("Shutdown signal received. Stopping consumer...")

	cancel()
	wg.Wait()
	slog.Info("Consumer stopped gracefully.")

	return nil
}
