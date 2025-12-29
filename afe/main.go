package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/IRIO-ORG/Trading-System/common"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/grpc"
)

const RequestsTopic = "trading-updates"

func makeProducer() (sarama.SyncProducer, error) {
	kafkaAddr := os.Getenv("KAFKA_BROKER_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "my-kafka:9092" // Default address in K8s (Bitnami)
	}

	slog.Info("AFE: Connecting to Kafka...", "address", kafkaAddr)

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
		slog.Info("AFE: Waiting for Kafka...\n", "error", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to start producer: %v", err)
	}
	slog.Info("AFE: Connected!")
	return producer, nil
}

func main() {
	producer, err := makeProducer()
	if err != nil {
		slog.Error("MakeProducer failed", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	port, err := common.GetEnv("SERVER_PORT", uint16(50051))
	if err != nil {
		slog.Error("Invalid AFE server port, using default", "error", err)
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		slog.Error("Failed to listen", "error", err)
		os.Exit(1)
	}
	slog.Info("Listening for gRPCs", "address", lis.Addr())

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(common.LoggingInterceptor))
	afeServer := makeAfeServer(tradesTopic, producer)
	pb.RegisterApplicationFrontendServer(grpcServer, afeServer)
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("Failed to serve gRPC", "error", err)
		os.Exit(1)
	}
}
