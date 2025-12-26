package common

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func LoggingInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	msg, ok := req.(protoreflect.ProtoMessage)
	if !ok {
		log.Printf("Invalid cast to protoreflect.ProtoMessage in logging interceptor")
	} else {
		marshaler := protojson.MarshalOptions{
			Multiline:       false,
			EmitUnpopulated: true,
		}
		jsonReq, err := marshaler.Marshal(msg)
		if err != nil {
			log.Printf("Marshal error: %v", err)
		}
		log.Printf("RPC: %s | Request: %s", info.FullMethod, string(jsonReq))
	}
	return handler(ctx, req)
}
