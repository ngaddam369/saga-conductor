package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// startSagaMethod is the fully-qualified gRPC method name for StartSaga.
// It is exempt from the handler timeout because it intentionally blocks for
// the full saga execution duration; the caller's own deadline governs it.
const startSagaMethod = "/saga.v1.SagaOrchestrator/StartSaga"

// TimeoutInterceptor returns a gRPC unary server interceptor that wraps every
// RPC with a context deadline of d. StartSaga is exempt and always uses the
// caller's context as-is. If d is zero the interceptor is a no-op pass-through.
func TimeoutInterceptor(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if d == 0 || info.FullMethod == startSagaMethod {
			return handler(ctx, req)
		}
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return handler(ctx, req)
	}
}
