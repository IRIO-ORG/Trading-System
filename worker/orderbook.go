package main

import (
	"container/heap"

	pb "github.com/IRIO-ORG/Trading-System/proto"
)

type order struct {
	id 			string
	side 		pb.Side
	price 		uint64
	remaining	uint64
	seq			uint64
}

type buyHeap []*order

func (h buyHeap) Len() int { return len(h) }
func (h buyHeap) Less(i, j int) bool {
	if h[i].price != h[j].price {
		return h[i].price > h[j].price
	}
	return h[i].seq < h[j].seq
}
func (h buyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *buyHeap) Push(x any) { *h = append(*h, x.(*order)) }
func (h *buyHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
func (h buyHeap) Peek() *order {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type sellHeap []*order

func (h sellHeap) Len() int { return len(h) }
func (h sellHeap) Less(i, j int) bool {
	if h[i].price != h[j].price {
		return h[i].price < h[j].price
	}
	return h[i].seq < h[j].seq
}
func (h sellHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sellHeap) Push(x any) { *h = append(*h, x.(*order)) }
func (h *sellHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
func (h sellHeap) Peek() *order {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}


type orderBook struct {
	bids 	buyHeap
	asks 	sellHeap
	seq		uint64
}

func newOrderBook() *orderBook {
	ob := &orderBook{}
	heap.Init(&ob.bids)
	heap.Init(&ob.asks)
	return ob
}