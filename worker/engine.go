package main

import (
	"container/heap"
	"fmt"
	"time"

	pb "github.com/IRIO-ORG/Trading-System/proto"
)

type executed struct {
	symbol 		string
	price 		uint64
	size 		uint64
	buyID		string
	sellID		string
	execTime	time.Time
}

type engine struct {
	books map[string]*orderBook
}

func newEngine() *engine {
	return &engine{
		books: map[string]*orderBook{}
	}
}

func (e *engine) onTrade(ti *pb.TradeInternal) ([]executed, error) {
	if ti == nil || ti.Trade == nil || ti.Trade.Instrument == nil {
		return nil, fmt.Errorf("invalid TradeInternal: missing trade.instrument")
	}
	symbol := ti.Trade.Instrument.Symbol
	if symbol == "" {
		return nil, fmt.Errorf("invalid TradeInternal: empty symbol")
	}
	if ti.Trade.Price == 0 || ti.Trade.Size == 0 {
		return nil, fmt.Errorf("invalid TradeInternal: price/size must be > 0")
	}
	if ti.RequestId == "" {
		return nil, fmt.Errorf("invalid TradeInternal: missing request_id")
	}

	ob := e.books[symbol]
	if ob == nil {
		ob = newOrderBook()
		e.books[symbol] = ob
	}

	ob.seq++
	in := &order{
		id:        ti.RequestId,
		symbol:	   symbol,
		price:     ti.Trade.Price,
		remaining: ti.Trade.Size,
		seq:       ob.seq,
	}

	switch ti.Trade.Side {
	case pb.Side_BUY:
		in.side = sideBuy
		return matchBuy(symbol, ob, in), nil
	case pb.Side_SELL:
		in.side = sideSell
		return matchSell(symbol, ob, in), nil
	default:
		return nil, fmt.Errorf("unknown side: %v", ti.Trade.Side)
	}
}

func matchBuy(symbol string, ob *orderBook, in *order) []executed {
	var out []executed

	for in.remaining > 0 {
		bestAsk := ob.asks.Peek()
		if bestAsk == nil {
			break
		}
		
		if bestAsk.price > in.price {
			break
		}

		qty := minU64(in.remaining, bestAsk.remaining)
		out = append(out, executed{
			symbol:   symbol,
			price:    bestAsk.price,
			size:     qty,
			buyID:    in.id,
			sellID:   bestAsk.id,
			execTime: time.Now().UTC(),
			// already concat id?
		})

		in.remaining -= qty
		bestAsk.remaining -= qty

		if bestAsk.remaining == 0 {
			heap.Pop(&ob.asks)
		}
	}

	if in.remaining > 0 {
		heap.Push(&ob.bids, in)
	}
	return out
}

func matchSell(symbol string, ob *orderBook, in *order) []executed {
	var out []executed

	for in.remaining > 0 {
		bestBid := ob.bids.Peek()
		if bestBid == nil {
			break
		}

		if bestBid.price < in.price {
			break
		}

		qty := minU64(in.remaining, bestBid.remaining)
		out = append(out, executed{
			symbol:   symbol,
			price:    bestBid.price,
			size:     qty,
			buyID:    bestBid.id,
			sellID:   in.id,
			execTime: time.Now().UTC(),
			// same
		})

		in.remaining -= qty
		bestBid.remaining -= qty

		if bestBid.remaining == 0 {
			heap.Pop(&ob.bids)
		}
	}

	if in.remaining > 0 {
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