package server_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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

// rejectValidator rejects all tokens.
type rejectValidator struct{ err error }

func (r rejectValidator) Validate(_ context.Context, _ string) error { return r.err }

// captureValidator records the token it received.
type captureValidator struct{ got string }

func (c *captureValidator) Validate(_ context.Context, token string) error {
	c.got = token
	return nil
}

func infoAny() *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{FullMethod: "/saga.v1.SagaOrchestrator/GetSaga"}
}

func okHandler(_ context.Context, _ any) (any, error) { return "ok", nil }

func TestAuthInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("nil validator is a noop", func(t *testing.T) {
		t.Parallel()
		interceptor := server.AuthInterceptor(nil)
		_, err := interceptor(context.Background(), nil, infoAny(), okHandler)
		if err != nil {
			t.Errorf("nil validator: got %v, want nil", err)
		}
	})

	t.Run("valid token passes through", func(t *testing.T) {
		t.Parallel()
		cap := &captureValidator{}
		interceptor := server.AuthInterceptor(cap)
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "Bearer my-secret"))

		res, err := interceptor(ctx, nil, infoAny(), okHandler)
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
		if res != "ok" {
			t.Errorf("result: got %v, want ok", res)
		}
		if cap.got != "my-secret" {
			t.Errorf("captured token: got %q, want %q", cap.got, "my-secret")
		}
	})

	t.Run("missing Authorization header passes empty string to validator", func(t *testing.T) {
		t.Parallel()
		cap := &captureValidator{}
		interceptor := server.AuthInterceptor(cap)

		_, err := interceptor(context.Background(), nil, infoAny(), okHandler)
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
		if cap.got != "" {
			t.Errorf("captured token: got %q, want empty", cap.got)
		}
	})

	t.Run("rejected token returns Unauthenticated", func(t *testing.T) {
		t.Parallel()
		interceptor := server.AuthInterceptor(rejectValidator{err: errors.New("bad token")})
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "Bearer bad"))

		_, err := interceptor(ctx, nil, infoAny(), okHandler)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("expected gRPC status error, got %T: %v", err, err)
		}
		if st.Code() != codes.Unauthenticated {
			t.Errorf("code: want Unauthenticated, got %v", st.Code())
		}
	})

	t.Run("non-Bearer Authorization header passes empty string to validator", func(t *testing.T) {
		t.Parallel()
		cap := &captureValidator{}
		interceptor := server.AuthInterceptor(cap)
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "Basic dXNlcjpwYXNz"))

		_, _ = interceptor(ctx, nil, infoAny(), okHandler)
		if cap.got != "" {
			t.Errorf("non-Bearer scheme: expected empty token, got %q", cap.got)
		}
	})
}
