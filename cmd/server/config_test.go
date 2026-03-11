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
		name                     string
		env                      map[string]string
		wantGRPCAddr             string
		wantHealthAddr           string
		wantDBPath               string
		wantRecvMB               int
		wantSendMB               int
		wantMaxConnIdleMinutes   int
		wantKeepaliveTimeMinutes int
		wantKeepaliveTimeoutSecs int
		wantKeepaliveMinTimeSecs int
	}{
		{
			name:                     "defaults when no env vars set",
			env:                      map[string]string{},
			wantGRPCAddr:             ":8080",
			wantHealthAddr:           ":8081",
			wantDBPath:               "saga-conductor.db",
			wantRecvMB:               4,
			wantSendMB:               16,
			wantMaxConnIdleMinutes:   5,
			wantKeepaliveTimeMinutes: 2,
			wantKeepaliveTimeoutSecs: 20,
			wantKeepaliveMinTimeSecs: 30,
		},
		{
			name: "all env vars overridden",
			env: map[string]string{
				"GRPC_ADDR":                       ":9090",
				"HEALTH_ADDR":                     ":9091",
				"DB_PATH":                         "/data/saga.db",
				"GRPC_MAX_RECV_MB":                "8",
				"GRPC_MAX_SEND_MB":                "32",
				"GRPC_MAX_CONN_IDLE_MINUTES":      "10",
				"GRPC_KEEPALIVE_TIME_MINUTES":     "3",
				"GRPC_KEEPALIVE_TIMEOUT_SECONDS":  "30",
				"GRPC_KEEPALIVE_MIN_TIME_SECONDS": "60",
			},
			wantGRPCAddr:             ":9090",
			wantHealthAddr:           ":9091",
			wantDBPath:               "/data/saga.db",
			wantRecvMB:               8,
			wantSendMB:               32,
			wantMaxConnIdleMinutes:   10,
			wantKeepaliveTimeMinutes: 3,
			wantKeepaliveTimeoutSecs: 30,
			wantKeepaliveMinTimeSecs: 60,
		},
		{
			name: "invalid GRPC_MAX_RECV_MB falls back to default",
			env: map[string]string{
				"GRPC_MAX_RECV_MB": "bad",
			},
			wantGRPCAddr:             ":8080",
			wantHealthAddr:           ":8081",
			wantDBPath:               "saga-conductor.db",
			wantRecvMB:               4,
			wantSendMB:               16,
			wantMaxConnIdleMinutes:   5,
			wantKeepaliveTimeMinutes: 2,
			wantKeepaliveTimeoutSecs: 20,
			wantKeepaliveMinTimeSecs: 30,
		},
		{
			name: "zero values fall back to defaults",
			env: map[string]string{
				"GRPC_MAX_SEND_MB":                "0",
				"GRPC_MAX_CONN_IDLE_MINUTES":      "0",
				"GRPC_KEEPALIVE_TIME_MINUTES":     "0",
				"GRPC_KEEPALIVE_TIMEOUT_SECONDS":  "0",
				"GRPC_KEEPALIVE_MIN_TIME_SECONDS": "0",
			},
			wantGRPCAddr:             ":8080",
			wantHealthAddr:           ":8081",
			wantDBPath:               "saga-conductor.db",
			wantRecvMB:               4,
			wantSendMB:               16,
			wantMaxConnIdleMinutes:   5,
			wantKeepaliveTimeMinutes: 2,
			wantKeepaliveTimeoutSecs: 20,
			wantKeepaliveMinTimeSecs: 30,
		},
		{
			name: "invalid keepalive values fall back to defaults",
			env: map[string]string{
				"GRPC_MAX_CONN_IDLE_MINUTES":      "abc",
				"GRPC_KEEPALIVE_TIME_MINUTES":     "-1",
				"GRPC_KEEPALIVE_TIMEOUT_SECONDS":  "xyz",
				"GRPC_KEEPALIVE_MIN_TIME_SECONDS": "0",
			},
			wantGRPCAddr:             ":8080",
			wantHealthAddr:           ":8081",
			wantDBPath:               "saga-conductor.db",
			wantRecvMB:               4,
			wantSendMB:               16,
			wantMaxConnIdleMinutes:   5,
			wantKeepaliveTimeMinutes: 2,
			wantKeepaliveTimeoutSecs: 20,
			wantKeepaliveMinTimeSecs: 30,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			cfg := loadConfig()

			if cfg.grpcAddr != tc.wantGRPCAddr {
				t.Errorf("grpcAddr: got %q, want %q", cfg.grpcAddr, tc.wantGRPCAddr)
			}
			if cfg.healthAddr != tc.wantHealthAddr {
				t.Errorf("healthAddr: got %q, want %q", cfg.healthAddr, tc.wantHealthAddr)
			}
			if cfg.dbPath != tc.wantDBPath {
				t.Errorf("dbPath: got %q, want %q", cfg.dbPath, tc.wantDBPath)
			}
			if cfg.grpcMaxRecvMB != tc.wantRecvMB {
				t.Errorf("grpcMaxRecvMB: got %d, want %d", cfg.grpcMaxRecvMB, tc.wantRecvMB)
			}
			if cfg.grpcMaxSendMB != tc.wantSendMB {
				t.Errorf("grpcMaxSendMB: got %d, want %d", cfg.grpcMaxSendMB, tc.wantSendMB)
			}
			if cfg.grpcMaxConnIdleMinutes != tc.wantMaxConnIdleMinutes {
				t.Errorf("grpcMaxConnIdleMinutes: got %d, want %d", cfg.grpcMaxConnIdleMinutes, tc.wantMaxConnIdleMinutes)
			}
			if cfg.grpcKeepaliveTimeMinutes != tc.wantKeepaliveTimeMinutes {
				t.Errorf("grpcKeepaliveTimeMinutes: got %d, want %d", cfg.grpcKeepaliveTimeMinutes, tc.wantKeepaliveTimeMinutes)
			}
			if cfg.grpcKeepaliveTimeoutSecs != tc.wantKeepaliveTimeoutSecs {
				t.Errorf("grpcKeepaliveTimeoutSecs: got %d, want %d", cfg.grpcKeepaliveTimeoutSecs, tc.wantKeepaliveTimeoutSecs)
			}
			if cfg.grpcKeepaliveMinTimeSecs != tc.wantKeepaliveMinTimeSecs {
				t.Errorf("grpcKeepaliveMinTimeSecs: got %d, want %d", cfg.grpcKeepaliveMinTimeSecs, tc.wantKeepaliveMinTimeSecs)
			}
		})
	}
}
