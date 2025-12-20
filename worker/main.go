package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	kafkaAddr := os.Getenv("KAFKA_BROKER_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "my-kafka:9092"
	}
	topic := "trading-updates"

	fmt.Printf("WORKER: Connecting to Kafka at %s...\n", kafkaAddr)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Retry loop
	var consumer sarama.Consumer
	var err error
	for i := 0; i < 10; i++ {
		consumer, err = sarama.NewConsumer([]string{kafkaAddr}, config)
		if err == nil {
			break
		}
		fmt.Printf("WORKER: Waiting for Kafka... (%v)\n", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Fatalf("WORKER: Failed to start consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("WORKER: Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	fmt.Println("WORKER: Connected! Listening for messages...")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("WORKER received: %s\n", string(msg.Value))
		case err := <-partitionConsumer.Errors():
			log.Printf("WORKER error: %v\n", err)
		case <-signals:
			fmt.Println("WORKER: Interrupt is detected")
			return
		}
	}
}
