package database

import (
	"log/slog"

	"github.com/IRIO-ORG/Trading-System/common"
)

// Config holds connection parameters
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

func GetConfigFromEnv() Config {
	host, err := common.GetEnv("DB_HOST", "my-postgres-postgresql")
	if err != nil {
		slog.Warn("Failed to get DB_HOST from env, using default", "error", err)
	}

	port, err := common.GetEnv("DB_PORT", "5432")
	if err != nil {
		slog.Warn("Failed to get DB_PORT from env, using default", "error", err)
	}

	user, err := common.GetEnv("DB_USER", "postgres")
	if err != nil {
		slog.Warn("Failed to get DB_USER from env, using default", "error", err)
	}

	password, err := common.GetEnv("DB_PASSWORD", "postgres")
	if err != nil {
		slog.Warn("Failed to get DB_PASSWORD from env, using default", "error", err)
	}

	dbName, err := common.GetEnv("DB_NAME", "trading_db")
	if err != nil {
		slog.Warn("Failed to get DB_NAME from env, using default", "error", err)
	}

	return Config{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		DBName:   dbName,
	}
}
