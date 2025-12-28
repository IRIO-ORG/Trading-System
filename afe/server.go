package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/IBM/sarama"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type afeServer struct {
	pb.UnimplementedApplicationFrontendServer
	producer      sarama.SyncProducer
	requestsTopic string
}

func makeAfeServer(requestsTopic string, producer sarama.SyncProducer) pb.ApplicationFrontendServer {
	return &afeServer{
		requestsTopic: requestsTopic,
		producer:      producer,
	}
}

func (s *afeServer) SubmitTrade(ctx context.Context, req *pb.TradeRequest) (*pb.TradeResponse, error) {
	if req == nil || req.Trade == nil || req.Trade.Instrument == nil {
		return nil, status.Error(codes.InvalidArgument, "missing trade.instrument")
	}
	symbol := strings.ToUpper(strings.Trimspace(req.Trade.Instrument.Symbol))
	if symbol == "" {
		return nil, status.Error(codes.InvalidArgument, "instrument.symbol is required")
	}
	if req.Trade.Size == 0 {
		return nil, status.Error(codes.InvalidArgument, "size must be > 0")
	}
	if req.Trade.Price == 0 {
		return nil, status.Error(codes.InvalidArgument, "price must be > 0")
	}

	requestID := strings.TrimSpace(req.RequestId)
	if requestID == "" {
		requestID = newRequestID
	}

	ti := &pb.TradeEvent {
		Trade: 		req.Trade
		ReceivedAt: timestamppb.Now()
		RequestId: 	requestID
	}
	ti.Trade.Instrument.Symbol = symbol

	msgValue, err := proto.Marshal(ti)
	if err != nil {
		return nil, fmt.Errorf("Failed to Marshal to TradeEvent: [%v]", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: s.requestsTopic,
		Key:   sarama.StringEncoder(symbol)
		Value: sarama.ByteEncoder(msgValue),
	}

	partition, offset, err := s.producer.SendMessage(msg)
	if err != nil {
		slog.Warn("AFE: Failed to send message", "error", err)
	} else {
		slog.Debug("AFE: Message sent!", "partition", partition, "offset", offset, "message", msgValue)
	}
	return &pb.TradeResponse{}, nil
}

func newRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("req-%d", timestamppb.Now().AsTime().UnixNano())
	}
	return hex.EncodeToString(b)
}
