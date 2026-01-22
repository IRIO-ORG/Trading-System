package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	pb "github.com/IRIO-ORG/Trading-System/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type executed struct {
	symbol   string
	price    uint64
	size     uint64
	buyID    string
	sellID   string
	execTime time.Time
}

type engine struct {
	books map[string]*orderBook
	mut   sync.Mutex
}

func newEngine() *engine {
	return &engine{
		books: map[string]*orderBook{},
	}
}

func (e *engine) createSnapshotLocked(createdAt time.Time, tradesPartition int32, lastOffset int64) map[string]*pb.OrderBookSnapshot {
	out := make(map[string]*pb.OrderBookSnapshot)
	for symbol, ob := range e.books {
		snap := &pb.OrderBookSnapshot{
			CreatedAt:       timestamppb.New(createdAt),
			Symbol:          symbol,
			LastOffset:      lastOffset,
			OrderbookSeq:    ob.seq,
			Bids:            make([]*pb.Order, 0, len(ob.bids)),
			Asks:            make([]*pb.Order, 0, len(ob.asks)),
			TradesPartition: uint32(tradesPartition),
		}

		for _, o := range ob.bids {
			if o == nil {
				continue
			}
			snap.Bids = append(snap.Bids, proto.CloneOf(o))
		}
		for _, o := range ob.asks {
			if o == nil {
				continue
			}
			snap.Asks = append(snap.Asks, proto.CloneOf(o))
		}

		out[symbol] = snap
	}

	return out
}

// onTrade must be called with e.mut acquired.
func (e *engine) onTrade(ev *pb.TradeEvent, orderBookFactory func(string) *orderBook) ([]executed, error) {
	if ev == nil || ev.Trade == nil || ev.Trade.Instrument == nil {
		return nil, fmt.Errorf("invalid TradeEvent: missing trade.instrument")
	}
	symbol := ev.Trade.Instrument.Symbol
	if symbol == "" {
		return nil, fmt.Errorf("invalid TradeEvent: empty symbol")
	}
	if ev.Trade.Price == 0 || ev.Trade.Size == 0 {
		return nil, fmt.Errorf("invalid TradeEvent: price/size must be > 0")
	}
	if ev.RequestId == "" {
		return nil, fmt.Errorf("invalid TradeEvent: missing request_id")
	}

	ob := e.books[symbol]
	if ob == nil {
		ob = orderBookFactory(symbol)
		e.books[symbol] = ob
	}

	ob.seq++
	order := &pb.Order{
		Id:    ev.RequestId,
		Price: ev.Trade.Price,
		Size:  ev.Trade.Size,
		Seq:   ob.seq,
	}

	switch ev.Trade.Side {
	case pb.Side_BUY:
		return matchBuy(symbol, ob, order), nil
	case pb.Side_SELL:
		return matchSell(symbol, ob, order), nil
	default:
		return nil, fmt.Errorf("unknown side: %v", ev.Trade.Side)
	}
}

func matchBuy(symbol string, ob *orderBook, in *pb.Order) []executed {
	var out []executed

	for in.Size > 0 {
		bestAsk := ob.asks.Peek()
		if bestAsk == nil {
			break
		}

		if bestAsk.Price > in.Price {
			break
		}

		qty := minU64(in.Size, bestAsk.Size)
		out = append(out, executed{
			symbol:   symbol,
			price:    bestAsk.Price,
			size:     qty,
			buyID:    in.Id,
			sellID:   bestAsk.Id,
			execTime: time.Now().UTC(),
		})

		in.Size -= qty
		bestAsk.Size -= qty

		if bestAsk.Size == 0 {
			heap.Pop(&ob.asks)
		}
	}

	if in.Size > 0 {
		heap.Push(&ob.bids, in)
	}
	return out
}

func matchSell(symbol string, ob *orderBook, in *pb.Order) []executed {
	var out []executed

	for in.Size > 0 {
		bestBid := ob.bids.Peek()
		if bestBid == nil {
			break
		}

		if bestBid.Price < in.Price {
			break
		}

		qty := minU64(in.Size, bestBid.Size)
		out = append(out, executed{
			symbol:   symbol,
			price:    bestBid.Price,
			size:     qty,
			buyID:    bestBid.Id,
			sellID:   in.Id,
			execTime: time.Now().UTC(),
		})

		in.Size -= qty
		bestBid.Size -= qty

		if bestBid.Size == 0 {
			heap.Pop(&ob.bids)
		}
	}

	if in.Size > 0 {
		heap.Push(&ob.asks, in)
	}
	return out
}

func minU64(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
