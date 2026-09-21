package httpapi

import (
	"errors"
	"net"
	"os"
)

type Config struct{ Address string }

func NewConfig() (Config, error) {
	address := os.Getenv("HTTP_ADDRESS")

	if address == "" {
		address = ":8080"
	}

	if _, _, err := net.SplitHostPort(address); err != nil {
		return Config{}, errors.New("HTTP_ADDRESS must contain host and port")
	}

	return Config{Address: address}, nil
}
