package main

import (
	"sync/atomic"
	"testing"
	"time"
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
		name                       string
		env                        map[string]string
		wantGRPCAddr               string
		wantHealthAddr             string
		wantDBPath                 string
		wantRecvMB                 int
		wantSendMB                 int
		wantMaxConnIdleMinutes     int
		wantKeepaliveTimeMinutes   int
		wantKeepaliveTimeoutSecs   int
		wantKeepaliveMinTimeSecs   int
		wantHealthReadSecs         int
		wantHealthWriteSecs        int
		wantHealthIdleSecs         int
		wantGRPCStopTimeoutSecs    int
		wantIdempotencyKeyTTLHours int
	}{
		{
			name:                       "defaults when no env vars set",
			env:                        map[string]string{},
			wantGRPCAddr:               ":8080",
			wantHealthAddr:             ":8081",
			wantDBPath:                 "saga-conductor.db",
			wantRecvMB:                 4,
			wantSendMB:                 16,
			wantMaxConnIdleMinutes:     5,
			wantKeepaliveTimeMinutes:   2,
			wantKeepaliveTimeoutSecs:   20,
			wantKeepaliveMinTimeSecs:   30,
			wantHealthReadSecs:         5,
			wantHealthWriteSecs:        5,
			wantHealthIdleSecs:         60,
			wantGRPCStopTimeoutSecs:    30,
			wantIdempotencyKeyTTLHours: 24,
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
				"HEALTH_READ_TIMEOUT_SECONDS":     "10",
				"HEALTH_WRITE_TIMEOUT_SECONDS":    "10",
				"HEALTH_IDLE_TIMEOUT_SECONDS":     "120",
				"GRPC_STOP_TIMEOUT_SECONDS":       "60",
				"IDEMPOTENCY_KEY_TTL_HOURS":       "48",
			},
			wantGRPCAddr:               ":9090",
			wantHealthAddr:             ":9091",
			wantDBPath:                 "/data/saga.db",
			wantRecvMB:                 8,
			wantSendMB:                 32,
			wantMaxConnIdleMinutes:     10,
			wantKeepaliveTimeMinutes:   3,
			wantKeepaliveTimeoutSecs:   30,
			wantKeepaliveMinTimeSecs:   60,
			wantHealthReadSecs:         10,
			wantHealthWriteSecs:        10,
			wantHealthIdleSecs:         120,
			wantGRPCStopTimeoutSecs:    60,
			wantIdempotencyKeyTTLHours: 48,
		},
		{
			name: "invalid GRPC_MAX_RECV_MB falls back to default",
			env: map[string]string{
				"GRPC_MAX_RECV_MB": "bad",
			},
			wantGRPCAddr:               ":8080",
			wantHealthAddr:             ":8081",
			wantDBPath:                 "saga-conductor.db",
			wantRecvMB:                 4,
			wantSendMB:                 16,
			wantMaxConnIdleMinutes:     5,
			wantKeepaliveTimeMinutes:   2,
			wantKeepaliveTimeoutSecs:   20,
			wantKeepaliveMinTimeSecs:   30,
			wantHealthReadSecs:         5,
			wantHealthWriteSecs:        5,
			wantHealthIdleSecs:         60,
			wantGRPCStopTimeoutSecs:    30,
			wantIdempotencyKeyTTLHours: 24,
		},
		{
			name: "zero values fall back to defaults",
			env: map[string]string{
				"GRPC_MAX_SEND_MB":                "0",
				"GRPC_MAX_CONN_IDLE_MINUTES":      "0",
				"GRPC_KEEPALIVE_TIME_MINUTES":     "0",
				"GRPC_KEEPALIVE_TIMEOUT_SECONDS":  "0",
				"GRPC_KEEPALIVE_MIN_TIME_SECONDS": "0",
				"HEALTH_READ_TIMEOUT_SECONDS":     "0",
				"HEALTH_WRITE_TIMEOUT_SECONDS":    "0",
				"HEALTH_IDLE_TIMEOUT_SECONDS":     "0",
				"GRPC_STOP_TIMEOUT_SECONDS":       "0",
			},
			wantGRPCAddr:               ":8080",
			wantHealthAddr:             ":8081",
			wantDBPath:                 "saga-conductor.db",
			wantRecvMB:                 4,
			wantSendMB:                 16,
			wantMaxConnIdleMinutes:     5,
			wantKeepaliveTimeMinutes:   2,
			wantKeepaliveTimeoutSecs:   20,
			wantKeepaliveMinTimeSecs:   30,
			wantHealthReadSecs:         5,
			wantHealthWriteSecs:        5,
			wantHealthIdleSecs:         60,
			wantGRPCStopTimeoutSecs:    30,
			wantIdempotencyKeyTTLHours: 24,
		},
		{
			name: "invalid keepalive and health timeout values fall back to defaults",
			env: map[string]string{
				"GRPC_MAX_CONN_IDLE_MINUTES":      "abc",
				"GRPC_KEEPALIVE_TIME_MINUTES":     "-1",
				"GRPC_KEEPALIVE_TIMEOUT_SECONDS":  "xyz",
				"GRPC_KEEPALIVE_MIN_TIME_SECONDS": "0",
				"HEALTH_READ_TIMEOUT_SECONDS":     "bad",
				"HEALTH_WRITE_TIMEOUT_SECONDS":    "-1",
				"HEALTH_IDLE_TIMEOUT_SECONDS":     "abc",
				"GRPC_STOP_TIMEOUT_SECONDS":       "bad",
			},
			wantGRPCAddr:               ":8080",
			wantHealthAddr:             ":8081",
			wantDBPath:                 "saga-conductor.db",
			wantRecvMB:                 4,
			wantSendMB:                 16,
			wantMaxConnIdleMinutes:     5,
			wantKeepaliveTimeMinutes:   2,
			wantKeepaliveTimeoutSecs:   20,
			wantKeepaliveMinTimeSecs:   30,
			wantHealthReadSecs:         5,
			wantHealthWriteSecs:        5,
			wantHealthIdleSecs:         60,
			wantGRPCStopTimeoutSecs:    30,
			wantIdempotencyKeyTTLHours: 24,
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
			if cfg.healthReadTimeoutSecs != tc.wantHealthReadSecs {
				t.Errorf("healthReadTimeoutSecs: got %d, want %d", cfg.healthReadTimeoutSecs, tc.wantHealthReadSecs)
			}
			if cfg.healthWriteTimeoutSecs != tc.wantHealthWriteSecs {
				t.Errorf("healthWriteTimeoutSecs: got %d, want %d", cfg.healthWriteTimeoutSecs, tc.wantHealthWriteSecs)
			}
			if cfg.healthIdleTimeoutSecs != tc.wantHealthIdleSecs {
				t.Errorf("healthIdleTimeoutSecs: got %d, want %d", cfg.healthIdleTimeoutSecs, tc.wantHealthIdleSecs)
			}
			if cfg.grpcStopTimeoutSecs != tc.wantGRPCStopTimeoutSecs {
				t.Errorf("grpcStopTimeoutSecs: got %d, want %d", cfg.grpcStopTimeoutSecs, tc.wantGRPCStopTimeoutSecs)
			}
			if cfg.idempotencyKeyTTLHours != tc.wantIdempotencyKeyTTLHours {
				t.Errorf("idempotencyKeyTTLHours: got %d, want %d", cfg.idempotencyKeyTTLHours, tc.wantIdempotencyKeyTTLHours)
			}
		})
	}
}

// hangingStopper is a mock grpcStopper whose GracefulStop blocks until Stop is
// called. It lets us test the force-stop fallback without needing a real gRPC
// server with stuck connections.
type hangingStopper struct {
	stopCalled atomic.Bool
	stopped    chan struct{}
}

func newHangingStopper() *hangingStopper {
	return &hangingStopper{stopped: make(chan struct{})}
}

func (h *hangingStopper) GracefulStop() { <-h.stopped }
func (h *hangingStopper) Stop() {
	h.stopCalled.Store(true)
	close(h.stopped)
}

// immediateStopper is a mock grpcStopper whose GracefulStop returns immediately.
type immediateStopper struct{}

func (immediateStopper) GracefulStop() {}
func (immediateStopper) Stop()         {}

func TestGRPCStopWithTimeout(t *testing.T) {
	t.Run("graceful stop completes before timeout", func(t *testing.T) {
		start := time.Now()
		grpcStopWithTimeout(immediateStopper{}, 5*time.Second)
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Errorf("took %v; expected immediate return when GracefulStop is fast", elapsed)
		}
	})

	t.Run("force stop fires when graceful stop hangs", func(t *testing.T) {
		h := newHangingStopper()

		const stopTimeout = 50 * time.Millisecond
		start := time.Now()
		grpcStopWithTimeout(h, stopTimeout)
		elapsed := time.Since(start)

		if !h.stopCalled.Load() {
			t.Error("Stop() was not called; expected force stop after timeout")
		}
		// Should have waited at least the timeout before forcing.
		if elapsed < stopTimeout {
			t.Errorf("returned after %v; expected to wait at least %s", elapsed, stopTimeout)
		}
		// Should not hang much beyond the timeout.
		if elapsed > 3*time.Second {
			t.Errorf("took %v; expected force stop to fire promptly after %s", elapsed, stopTimeout)
		}
	})
}
