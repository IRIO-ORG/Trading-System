package main

import (
	"fmt"
	"log/slog"

	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
)

func dumpTradeEvent(raw []byte) {
	var ti pb.TradeEvent
	if err := proto.Unmarshal(raw, &ti); err != nil {
		slog.Error("DUMP: failed to decode TradeEvent", "err", err)
		return
	}

	fmt.Printf("DUMP: %s\n", ti.String())
}