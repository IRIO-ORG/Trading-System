package model

import "time"

type Side string

const {
	SideBuy Side = "BUY"
	SideSell Side = "SELL"
}

type OrderRequest struct {
	RequestID 	string 		`json:"requestId`
	Ticker 		string		`json:"ticker"`
	Side		Side		`json:"side"`
	Price 		float64		`json:"price`
	Quantity	int64		`json:"quantity"`
	Timestamp	time.Time	`json:"ts"`
}

type ExecutedTrade struct {
	TradeID			string 		`json:"tradeId"`
	Ticker			string		`json:"ticker"`
	Price			float64		`json:"price"`
	Quantity		int64		`json:"quantity"`
	BuyRequestID	string		`json:"buyRequestId"`
	SellRequestID	string		`json:"sellRequestId"`
	Timestamp		time.Time	`json:"ts"`
}

type OrderBookSnapshot struct {
	Ticker		string		`json:"ticker"`
	BidCount	int			`json:bidCount"`
	AskCount	int			`json:askCount"`
	Timestamp	time.Time	`json:"ts"`
	Note		string		`json:"note,omitempty"`
}