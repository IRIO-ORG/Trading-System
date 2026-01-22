package main

import (
	"container/heap"

	pb "github.com/IRIO-ORG/Trading-System/proto"
)

type buyHeap []*pb.Order

func (h buyHeap) Len() int { return len(h) }
func (h buyHeap) Less(i, j int) bool {
	if h[i].Price != h[j].Price {
		return h[i].Price > h[j].Price
	}
	return h[i].Seq < h[j].Seq
}
func (h buyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *buyHeap) Push(x any)   { *h = append(*h, x.(*pb.Order)) }
func (h *buyHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
func (h buyHeap) Peek() *pb.Order {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type sellHeap []*pb.Order

func (h sellHeap) Len() int { return len(h) }
func (h sellHeap) Less(i, j int) bool {
	if h[i].Price != h[j].Price {
		return h[i].Price < h[j].Price
	}
	return h[i].Seq < h[j].Seq
}
func (h sellHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sellHeap) Push(x any)   { *h = append(*h, x.(*pb.Order)) }
func (h *sellHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
func (h sellHeap) Peek() *pb.Order {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type orderBook struct {
	bids buyHeap
	asks sellHeap
	seq  uint64
}

func newOrderBook() *orderBook {
	ob := &orderBook{}
	heap.Init(&ob.bids)
	heap.Init(&ob.asks)
	return ob
}
