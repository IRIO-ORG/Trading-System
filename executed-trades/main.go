package main

import (
	"log/slog"

	"github.com/IRIO-ORG/Trading-System/common/database"
	"github.com/IRIO-ORG/Trading-System/common/kafka"
)

const (
	topic   = "executed-trades"
	groupID = "executed-trades-consumer-group"
)

func main() {
	slog.Info("Starting Executed Trades Consumer...")

	db, err := database.ConnectWithRetries(database.GetConfigFromEnv())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := database.EnsureTableExists(db); err != nil {
		panic(err)
	}

	handler := &ExecutedTradesHandler{db: db}

	// Blocks main() operation until receives CTRL+C/SIGTERM
	err = kafka.RunConsumerGroup(groupID, []string{topic}, handler)
	if err != nil {
		slog.Error("ERROR running consumer group", "error", err)
	}
}
