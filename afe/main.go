package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	kafkaAddr := os.Getenv("KAFKA_BROKER_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "my-kafka:9092" // Default address in K8s (Bitnami)
	}
	topic := "trading-updates"

	fmt.Printf("AFE: Connecting to Kafka at %s...\n", kafkaAddr)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Retry loop for connection (Kafka might take a moment to start)
	var producer sarama.SyncProducer
	var err error
	for i := 0; i < 10; i++ {
		producer, err = sarama.NewSyncProducer([]string{kafkaAddr}, config)
		if err == nil {
			break
		}
		fmt.Printf("AFE: Waiting for Kafka... (%v)\n", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Fatalf("AFE: Failed to start producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("AFE: Connected! Starting to produce...")

	for {
		msgValue := fmt.Sprintf("Trade event at %s", time.Now().Format(time.RFC3339))
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msgValue),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("AFE: Failed to send message: %v\n", err)
		} else {
			fmt.Printf("AFE: Message sent to partition %d at offset %d: %s\n", partition, offset, msgValue)
		}

		time.Sleep(5 * time.Second)
	}
}
