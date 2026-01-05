package main

import (
	"fmt"

	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
)

func dumpTradeEvent(raw []byte) {
	var ti pb.TradeEvent
	if err := proto.Unmarshal(raw, &ti); err != nil {
		fmt.Printf("DUMP: failed to decode TradeEvent: %v\n", err)
		return
	}

	fmt.Printf("DUMP: %s\n", ti.String())
}