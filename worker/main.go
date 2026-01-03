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
	tradesTopic := getenv("TRADES_TOPIC", "trades")
	partition := getenvInt("TRADES_PARTITION", 0)
	mode := getenv("WORKER_MODE", "dump")

	fmt.Printf("WORKER: kafka=%s topic=%s partition=%d\n", kafkaAddr, tradesTopic, partition)

	consumer, err := makeConsumer(kafkaAddr)
	if err != nil {
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

	var eng *engine
	if mode == "engine" {
		eng = newEngine()
		fmt.Println("WORKER: matching engine enabled")
	} else {
		fmt.Println("WORKER: dump mode enabled")
	}

	for {
		select {
		case msg := <-pc.Messages():
			if msg == nil {
				continue
			}

			if mode == "dump" {
				dumpTradeInternal(msg.Value)
				continue
			}

			// engine / default mode
			var ti pb.TradeInternal
			if err := proto.Unmarshal(msg.Value, &ti); err != nil {
				fmt.Printf("ENGINE: invalid TradeInternal protobuf: %v\n", err)
				continue
			}

			execs, err := eng.onTrade(&ti)
			if err != nil {
				fmt.Printf("ENGINE: reject: %v\n", err)
				continue
			}

			if len(execs) == 0 {
				fmt.Printf("ENGINE: accepted order request_id=%s symbol=%s (no match)\n", ti.RequestId, ti.Trade.Instrument.Symbol)
				continue
			}
			for _, ex := range execs {
				fmt.Printf("EXECUTED: symbol=%s price=%d size=%d buy=%s sell=%s ts=%s\n",
					ex.symbol, ex.price, ex.size, ex.buyID, ex.sellID, ex.execTime.Format(time.RFC3339Nano))
			}

		case err := <-pc.Errors():
			if err != nil {
				log.Printf("WORKER: partition consumer error: %v\n", err)
			}
		case <-signals:
			fmt.Println("WORKER: interrupt")
			return
		}
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
