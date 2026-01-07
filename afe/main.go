package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/IRIO-ORG/Trading-System/common"
	"github.com/IRIO-ORG/Trading-System/common/kafka"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/grpc"
)

const (
	defaultRequestsTopic   	= "trade-requests"
)

func main() {
	producer, err := kafka.NewProtoProducer()
	if err != nil {
		slog.Error("NewProducer failed", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	tradesTopic, _ := common.GetEnv("TRADES_TOPIC", defaultRequestsTopic)

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
