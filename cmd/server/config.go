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

	// Keepalive tuning.
	grpcMaxConnIdleMinutes   int // MaxConnectionIdle
	grpcKeepaliveTimeMinutes int // Time between server pings
	grpcKeepaliveTimeoutSecs int // Timeout waiting for ping ack
	grpcKeepaliveMinTimeSecs int // EnforcementPolicy: minimum client ping interval
}

func loadConfig() config {
	return config{
		grpcAddr:      getEnv("GRPC_ADDR", ":8080"),
		healthAddr:    getEnv("HEALTH_ADDR", ":8081"),
		dbPath:        getEnv("DB_PATH", "saga-conductor.db"),
		grpcMaxRecvMB: getEnvInt("GRPC_MAX_RECV_MB", 4),
		grpcMaxSendMB: getEnvInt("GRPC_MAX_SEND_MB", 16),

		grpcMaxConnIdleMinutes:   getEnvInt("GRPC_MAX_CONN_IDLE_MINUTES", 5),
		grpcKeepaliveTimeMinutes: getEnvInt("GRPC_KEEPALIVE_TIME_MINUTES", 2),
		grpcKeepaliveTimeoutSecs: getEnvInt("GRPC_KEEPALIVE_TIMEOUT_SECONDS", 20),
		grpcKeepaliveMinTimeSecs: getEnvInt("GRPC_KEEPALIVE_MIN_TIME_SECONDS", 30),
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
