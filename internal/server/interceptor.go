package server

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// startSagaMethod is the fully-qualified gRPC method name for StartSaga.
// It is exempt from the handler timeout because it intentionally blocks for
// the full saga execution duration; the caller's own deadline governs it.
const startSagaMethod = "/saga.v1.SagaOrchestrator/StartSaga"

// AuthInterceptor returns a gRPC unary server interceptor that extracts the
// Authorization: Bearer <token> header from incoming metadata and calls
// v.Validate(ctx, token). A non-nil error from Validate rejects the call with
// codes.Unauthenticated. If v is nil the interceptor is a no-op pass-through
// (open server — same behaviour as before this feature was added).
func AuthInterceptor(v TokenValidator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if v == nil {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		var token string
		if vals := md.Get("authorization"); len(vals) > 0 {
			raw := vals[0]
			if after, ok := strings.CutPrefix(raw, "Bearer "); ok {
				token = after
			}
		}
		if err := v.Validate(ctx, token); err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "unauthorized: %v", err)
		}
		return handler(ctx, req)
	}
}

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
