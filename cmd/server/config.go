package main

import "os"

type config struct {
	grpcAddr   string
	healthAddr string
	dbPath     string
}

func loadConfig() config {
	return config{
		grpcAddr:   getEnv("GRPC_ADDR", ":8080"),
		healthAddr: getEnv("HEALTH_ADDR", ":8081"),
		dbPath:     getEnv("DB_PATH", "saga-conductor.db"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
