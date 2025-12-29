package main

import (
	"fmt"
	"time"

	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
)

func dumpTradeInternal(raw []byte) {
	var ti pb.TradeInternal
	if err := proto.Unmarshal(raw, &ti); err != nil {
		fmt.Printf("DUMP: failed to decode TradeInternal: %v\n", err)
		return
	}

	t := ti.Trade
	symbol := ""
	if t != nil && t.Instrument != nil {
		symbol = t.Instrument.Symbol
	}
	received := time.Time{}
	if ti.ReceivedAt != nil {
		received = ti.ReceivedAt.AsTime()
	}

	fmt.Printf(
	"DUMP: request_id=%s symbol=%s side=%s price=%d size=%d received_at=%s\n",
		ti.RequestId,
		symbol,
		sideToString(t),
		t.GetPrice(),
		t.GetSize(),
		received.UTC().Format(time.RFC3339Nano),
	)
}

func sideToString(t *pb.Trade) string {
	if t == nil {
		return "UNKNOWN"
	}
	switch t.Side {
	case pb.Side_BUY:
		return "BUY"
	case pb.Side_SELL:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}
