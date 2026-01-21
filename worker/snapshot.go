package main

import (
	"fmt"
	"log"

	pb "github.com/IRIO-ORG/Trading-System/proto"

	"github.com/IBM/sarama"
	"github.com/IRIO-ORG/Trading-System/common/kafka"
	"google.golang.org/protobuf/proto"
)

func GetSnapshotForSymbol(topic string, symbol string) (*pb.OrderBookSnapshot, error) {
	const partition = 0
	// 1. Configure Sarama Client
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create a client to interact with metadata (offsets)
	client, err := sarama.NewClient(kafka.GetBrokers(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	// 2. Find the "End" of the topic (High Watermark)
	// OffsetNewest is the next offset to be written. The last existing message is OffsetNewest - 1.
	newestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("failed to get offset: %w", err)
	}

	// Edge case: Partition is empty
	if newestOffset == 0 {
		return nil, nil // No snapshots exist yet
	}

	// 3. Create a Consumer for the specific partition
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, fmt.Errorf("failed to start partition consumer: %w", err)
	}
	defer pc.Close()

	var latestSnapshot *pb.OrderBookSnapshot
	targetOffset := newestOffset - 1 // The last message we need to check

	// 4. Read Loop
	// We use a label to break out of the loop cleanly
ReadLoop:
	for {
		select {
		case msg := <-pc.Messages():
			// A. Filter: Is this the symbol we want?
			// Since the topic is compacted, we might see multiple updates for "AAPL".
			// We keep overwriting 'latestSnapshot' so we end up with the most recent one.
			if string(msg.Key) == symbol {
				snapshot := &pb.OrderBookSnapshot{}
				if err := proto.Unmarshal(msg.Value, snapshot); err != nil {
					log.Printf("Error unmarshalling snapshot for %s: %v", symbol, err)
					// Don't return error, just skip bad message
				} else {
					latestSnapshot = snapshot
				}
			}

			// B. Stop Condition: Have we reached the end of the log?
			if msg.Offset >= targetOffset {
				break ReadLoop
			}

		case err := <-pc.Errors():
			return nil, fmt.Errorf("consumer error: %w", err)
		}
	}

	if latestSnapshot == nil {
		return nil, fmt.Errorf("snapshot not found for symbol: %s", symbol)
	}

	return latestSnapshot, nil
}
