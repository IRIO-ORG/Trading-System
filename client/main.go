package main

import (
	"context"
	"flag"
	"log"
	"strings"

	pb "github.com/IRIO-ORG/Trading-System/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	afeAddress = flag.String("server_address", "localhost:50051", "The gRPC server address and port of an AFE instance.")
	endpoint   = flag.String("endpoint", "", "AFE endpoint. One of: SubmitTrade.")
	body       = flag.String("body", "", "Request body in JSON.")
)

// Sends ApplicationFrontend.SubmitTrade request. Example:
// ./client --endpoint SubmitTrade --body '{"trade": {"instrument": {"symbol": "GOOG"},"price": "5990000","size": "1"}}'
func submitTrade(client pb.ApplicationFrontendClient, json_request string) {
	var proto_request pb.TradeRequest
	protojson.Unmarshal([]byte(json_request), &proto_request)
	log.Printf("TradeRequest: %s", protojson.Format(&proto_request))
	resp, err := client.SubmitTrade(context.Background(), &proto_request)
	if err != nil {
		log.Fatalf("SubmitTrade error: [%v]", err)
	}
	log.Printf("TradeResponse: [%s]", protojson.Format(resp))
}

func main() {
	flag.Parse()

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(*afeAddress, opts...)
	if err != nil {
		log.Fatalf("Failed to connect to [%s]: [%v]", *afeAddress, err)
	}
	defer conn.Close()

	client := pb.NewApplicationFrontendClient(conn)
	switch strings.ToLower(*endpoint) {
	case "submittrade":
		submitTrade(client, *body)
	default:
		log.Fatalf("Unrecognized endpoint: [%s]", *endpoint)
	}
}
