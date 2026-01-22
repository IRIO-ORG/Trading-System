package database

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/lib/pq"
)

// max retries for connection do DB
// In the future can be extracted to parameters
const maxRetries = 10

// EnsureTableExists ensures existence of the trades table, to store executed trades
func EnsureTableExists(db *sql.DB) error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS trades (
		execution_id VARCHAR(255) PRIMARY KEY,
		symbol VARCHAR(50) NOT NULL,
		price BIGINT NOT NULL,
		size BIGINT NOT NULL,
		bid_request_id VARCHAR(255),
		ask_request_id VARCHAR(255),
		executed_at TIMESTAMP
	);`
	_, err := db.Exec(createTableSQL)
	if err == nil {
		slog.Info("Table 'trades' is ready")
	}
	return err
}

func ConnectWithRetries(cfg Config) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName,
	)

	var db *sql.DB
	var err error

	// Retry loop
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
		}

		if err == nil {
			slog.Info("Successfully connected to the database")
			return db, nil
		}

		slog.Warn("Waiting for database...", "attempt", i+1, "error", err)
		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("could not connect to database after %d attempts: %w", maxRetries, err)
}
