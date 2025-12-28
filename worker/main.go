package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"
	"strconv"

	"github.com/IBM/sarama"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
)

func main() {
	kafkaAddr := getenv("KAFKA_BROKER_ADDR", "my-kafka:9092")
	topic := getenv("TRADES_TOPIC", "trades")
	partition := getenvInt("TRADES_PARTITION", 0)

	fmt.Printf("WORKER: kafka=%s topic=%s partition=%d\n", kafkaAddr, tradesTopic, partition)

	consumer, err = makeConsumer(kafkaAddr)
	if errr != nil {
		log.Fatalf("WORKER: %v", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(tradesTopic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("WORKER: failed to create partition consumer: %v", err)
	}
	defer pc.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

for {
	select {
	case msg := <-pc.Messages():
		if msg == nil {
			continue
		}

		dumpTradeInternal(msg.value)
	case err := <-pc.Errors():
		if err != nil {
			log.Printf("WORKER error: %v\n", err)
		}
	case <-signals:
		fmt.Println("WORKER: Interrupt is detected")
		return
	}
}

func getenv(k string, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
} 

func getenvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
