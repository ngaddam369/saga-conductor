package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/ngaddam369/saga-conductor/internal/server"
)

func TestTimeoutInterceptor(t *testing.T) {
	t.Parallel()

	// handler returns the context's deadline presence and sleeps for dur.
	hangHandler := func(dur time.Duration) grpc.UnaryHandler {
		return func(ctx context.Context, _ any) (any, error) {
			select {
			case <-time.After(dur):
				return "ok", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	t.Run("applies deadline to non-StartSaga methods", func(t *testing.T) {
		t.Parallel()
		interceptor := server.TimeoutInterceptor(100 * time.Millisecond)
		info := &grpc.UnaryServerInfo{FullMethod: "/saga.v1.SagaOrchestrator/ListSagas"}

		_, err := interceptor(context.Background(), nil, info, hangHandler(10*time.Second))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("got %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("StartSaga is exempt from deadline", func(t *testing.T) {
		t.Parallel()
		interceptor := server.TimeoutInterceptor(50 * time.Millisecond)
		info := &grpc.UnaryServerInfo{FullMethod: "/saga.v1.SagaOrchestrator/StartSaga"}

		// Handler completes in 200ms — longer than the interceptor timeout.
		// StartSaga must not be cancelled.
		_, err := interceptor(context.Background(), nil, info, hangHandler(200*time.Millisecond))
		if err != nil {
			t.Errorf("StartSaga: got %v, want nil", err)
		}
	})

	t.Run("zero timeout is a noop", func(t *testing.T) {
		t.Parallel()
		interceptor := server.TimeoutInterceptor(0)
		info := &grpc.UnaryServerInfo{FullMethod: "/saga.v1.SagaOrchestrator/ListSagas"}

		_, err := interceptor(context.Background(), nil, info, hangHandler(200*time.Millisecond))
		if err != nil {
			t.Errorf("zero timeout: got %v, want nil", err)
		}
	})

	t.Run("fast handler completes before deadline", func(t *testing.T) {
		t.Parallel()
		interceptor := server.TimeoutInterceptor(5 * time.Second)
		info := &grpc.UnaryServerInfo{FullMethod: "/saga.v1.SagaOrchestrator/GetSaga"}

		res, err := interceptor(context.Background(), nil, info, hangHandler(0))
		if err != nil {
			t.Errorf("fast handler: got %v, want nil", err)
		}
		if res != "ok" {
			t.Errorf("result: got %v, want ok", res)
		}
	})
}
