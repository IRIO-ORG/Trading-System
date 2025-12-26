package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	msgValue, err := proto.Marshal(&pb.TradeInternal{
		Trade:      req.GetTrade(),
		ReceivedAt: timestamppb.New(time.Now()),
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to Marshal to TradeInternal: [%v]", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: s.requestsTopic,
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
