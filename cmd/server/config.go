package main

import (
	"os"
	"strconv"
)

type config struct {
	grpcAddr      string
	healthAddr    string
	dbPath        string
	grpcMaxRecvMB int
	grpcMaxSendMB int
}

func loadConfig() config {
	return config{
		grpcAddr:      getEnv("GRPC_ADDR", ":8080"),
		healthAddr:    getEnv("HEALTH_ADDR", ":8081"),
		dbPath:        getEnv("DB_PATH", "saga-conductor.db"),
		grpcMaxRecvMB: getEnvInt("GRPC_MAX_RECV_MB", 4),
		grpcMaxSendMB: getEnvInt("GRPC_MAX_SEND_MB", 16),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
