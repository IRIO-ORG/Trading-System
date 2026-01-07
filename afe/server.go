package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	"crypto/rand"
	"strings"
	"encoding/hex"

	"github.com/IBM/sarama"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ckafka "github.com/IRIO-ORG/Trading-System/common/kafka"
)

type afeServer struct {
	pb.UnimplementedApplicationFrontendServer
	producer      *ckafka.ProtoProducer
	requestsTopic string
}

func makeAfeServer(requestsTopic string, producer *ckafka.ProtoProducer) pb.ApplicationFrontendServer {
	return &afeServer{
		requestsTopic: requestsTopic,
		producer:      producer,
	}
}

func validateTradeRequest(req *pb.TradeRequest) (symbol string, requestID string, err error) {
	if req == nil || req.Trade == nil || req.Trade.Instrument == nil {
		return "", "", status.Error(codes.InvalidArgument, "missing trade.instrument")
	}

	symbol = strings.ToUpper(strings.TrimSpace(req.Trade.Instrument.Symbol))
	if symbol == "" {
		return "", "", status.Error(codes.InvalidArgument, "instrument.symbol is required")
	}
	if req.Trade.Size == 0 {
		return "", "", status.Error(codes.InvalidArgument, "size must be > 0")
	}
	if req.Trade.Price == 0 {
		return "", "", status.Error(codes.InvalidArgument, "price must be > 0")
	}

	requestID = newRequestID()
	req.Trade.Instrument.Symbol = symbol
	return symbol, requestID, nil
}

func (s *afeServer) SubmitTrade(ctx context.Context, req *pb.TradeRequest) (*pb.TradeResponse, error) {
	symbol, requestID, err := validateTradeRequest(req)
	if err != nil {
		return nil, err
	}

	msgValue, err := proto.Marshal(&pb.TradeEvent{
		Trade:      req.GetTrade(),
		ReceivedAt: timestamppb.Now(),
		RequestId: 	requestID,
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to Marshal to TradeEvent: [%v]", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: s.requestsTopic,
		Key:   sarama.StringEncoder(symbol),
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
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}