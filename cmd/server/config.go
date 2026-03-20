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

	// Health HTTP server timeouts.
	healthReadTimeoutSecs  int
	healthWriteTimeoutSecs int
	healthIdleTimeoutSecs  int

	// gRPC handler timeout (applied to all RPCs except StartSaga).
	grpcHandlerTimeoutSecs int

	// Graceful-stop fallback: if GracefulStop does not complete within this
	// duration, Stop() is called to force-close remaining connections.
	grpcStopTimeoutSecs int

	// Idempotency key TTL: how long CreateSaga idempotency keys are retained.
	idempotencyKeyTTLHours int
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

		healthReadTimeoutSecs:  getEnvInt("HEALTH_READ_TIMEOUT_SECONDS", 5),
		healthWriteTimeoutSecs: getEnvInt("HEALTH_WRITE_TIMEOUT_SECONDS", 5),
		healthIdleTimeoutSecs:  getEnvInt("HEALTH_IDLE_TIMEOUT_SECONDS", 60),

		grpcHandlerTimeoutSecs: getEnvInt("GRPC_HANDLER_TIMEOUT_SECONDS", 60),
		grpcStopTimeoutSecs:    getEnvInt("GRPC_STOP_TIMEOUT_SECONDS", 30),

		idempotencyKeyTTLHours: getEnvInt("IDEMPOTENCY_KEY_TTL_HOURS", 24),
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
