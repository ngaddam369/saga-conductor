package main

import (
	"testing"
)

func TestGetEnvInt(t *testing.T) {
	tests := []struct {
		name     string
		envVal   string
		fallback int
		want     int
	}{
		{"unset uses fallback", "", 4, 4},
		{"valid value", "16", 4, 16},
		{"zero uses fallback", "0", 4, 4},
		{"negative uses fallback", "-1", 4, 4},
		{"non-numeric uses fallback", "abc", 4, 4},
		{"float uses fallback", "1.5", 4, 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envVal != "" {
				t.Setenv("TEST_ENV_INT", tc.envVal)
			}
			got := getEnvInt("TEST_ENV_INT", tc.fallback)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name           string
		env            map[string]string
		wantRecvMB     int
		wantSendMB     int
		wantGRPCAddr   string
		wantHealthAddr string
		wantDBPath     string
	}{
		{
			name:           "defaults when no env vars set",
			env:            map[string]string{},
			wantRecvMB:     4,
			wantSendMB:     16,
			wantGRPCAddr:   ":8080",
			wantHealthAddr: ":8081",
			wantDBPath:     "saga-conductor.db",
		},
		{
			name: "all env vars overridden",
			env: map[string]string{
				"GRPC_MAX_RECV_MB": "8",
				"GRPC_MAX_SEND_MB": "32",
				"GRPC_ADDR":        ":9090",
				"HEALTH_ADDR":      ":9091",
				"DB_PATH":          "/data/saga.db",
			},
			wantRecvMB:     8,
			wantSendMB:     32,
			wantGRPCAddr:   ":9090",
			wantHealthAddr: ":9091",
			wantDBPath:     "/data/saga.db",
		},
		{
			name: "invalid GRPC_MAX_RECV_MB falls back to default",
			env: map[string]string{
				"GRPC_MAX_RECV_MB": "bad",
			},
			wantRecvMB:     4,
			wantSendMB:     16,
			wantGRPCAddr:   ":8080",
			wantHealthAddr: ":8081",
			wantDBPath:     "saga-conductor.db",
		},
		{
			name: "zero GRPC_MAX_SEND_MB falls back to default",
			env: map[string]string{
				"GRPC_MAX_SEND_MB": "0",
			},
			wantRecvMB:     4,
			wantSendMB:     16,
			wantGRPCAddr:   ":8080",
			wantHealthAddr: ":8081",
			wantDBPath:     "saga-conductor.db",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg := loadConfig()

			if cfg.grpcMaxRecvMB != tc.wantRecvMB {
				t.Errorf("grpcMaxRecvMB: got %d, want %d", cfg.grpcMaxRecvMB, tc.wantRecvMB)
			}
			if cfg.grpcMaxSendMB != tc.wantSendMB {
				t.Errorf("grpcMaxSendMB: got %d, want %d", cfg.grpcMaxSendMB, tc.wantSendMB)
			}
			if cfg.grpcAddr != tc.wantGRPCAddr {
				t.Errorf("grpcAddr: got %q, want %q", cfg.grpcAddr, tc.wantGRPCAddr)
			}
			if cfg.healthAddr != tc.wantHealthAddr {
				t.Errorf("healthAddr: got %q, want %q", cfg.healthAddr, tc.wantHealthAddr)
			}
			if cfg.dbPath != tc.wantDBPath {
				t.Errorf("dbPath: got %q, want %q", cfg.dbPath, tc.wantDBPath)
			}
		})
	}
}
