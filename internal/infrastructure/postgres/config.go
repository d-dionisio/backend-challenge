package postgres

import (
	"errors"
	"os"
)

var ErrDatabaseURLRequired = errors.New("DATABASE_URL is required")

type Config struct {
	DatabaseURL string
}

func NewConfig() (Config, error) {
	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		return Config{}, ErrDatabaseURLRequired
	}

	return Config{
		DatabaseURL: databaseURL,
	}, nil
}
