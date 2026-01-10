package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	ckafka "github.com/IRIO-ORG/Trading-System/common/kafka"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	ev := &pb.TradeEvent{
		Trade:      req.GetTrade(),
		ReceivedAt: timestamppb.Now(),
		RequestId:  requestID,
	}

	if err := s.producer.Send(s.requestsTopic, symbol, ev); err != nil {
		slog.Warn("AFE: Failed to send message", "err", err, "request_id", requestID, "symbol", symbol)
		return nil, status.Errorf(codes.Unavailable, "failed to publish trade: %v", err)
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
