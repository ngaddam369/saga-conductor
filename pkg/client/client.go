// Package client provides a Go client for the saga-conductor gRPC API.
// It handles connection management and exposes a clean API over the five
// SagaOrchestrator RPCs so callers do not need to import or manage gRPC
// plumbing directly.
package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

// Client is a connected saga-conductor client. It is safe for concurrent use.
// Call Close when done to release the underlying gRPC connection.
type Client struct {
	cc   *grpc.ClientConn
	stub pb.SagaOrchestratorClient
}

type options struct {
	grpcOpts []grpc.DialOption
}

// Option configures the Client.
type Option func(*options)

// WithInsecure disables transport security. Use this for local development and
// testing only — do not use in production.
func WithInsecure() Option {
	return func(o *options) {
		o.grpcOpts = append(o.grpcOpts,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
}

// WithTransportCredentials sets custom transport credentials (e.g. TLS or
// mTLS). Use this instead of WithInsecure for production deployments.
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return func(o *options) {
		o.grpcOpts = append(o.grpcOpts, grpc.WithTransportCredentials(creds))
	}
}

// WithToken injects a static Bearer token into the Authorization metadata
// header of every outbound RPC. Suitable for use with AUTH_TYPE=static on the
// server side.
func WithToken(token string) Option {
	return func(o *options) {
		interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		o.grpcOpts = append(o.grpcOpts, grpc.WithUnaryInterceptor(interceptor))
	}
}

// WithDialOptions appends raw gRPC dial options. Use this as an escape hatch
// for options not covered by the other helpers (e.g. keepalive parameters,
// custom interceptors).
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.grpcOpts = append(o.grpcOpts, opts...)
	}
}

// New creates a new Client connected to target. The target format follows the
// gRPC name-resolution syntax (e.g. "host:port"). gRPC establishes the
// connection lazily so New itself rarely fails; errors are typically surfaced
// on the first RPC call.
func New(target string, opts ...Option) (*Client, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	cc, err := grpc.NewClient(target, o.grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("saga client dial %s: %w", target, err)
	}
	return &Client{cc: cc, stub: pb.NewSagaOrchestratorClient(cc)}, nil
}

// Close releases the underlying gRPC connection. No further calls should be
// made on the Client after Close returns.
func (c *Client) Close() error {
	return c.cc.Close()
}

// CreateSaga registers a new saga and returns it in the PENDING state.
// The saga is not executed until StartSaga is called.
func (c *Client) CreateSaga(ctx context.Context, req *pb.CreateSagaRequest) (*pb.SagaExecution, error) {
	resp, err := c.stub.CreateSaga(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Saga, nil
}

// StartSaga begins executing the saga identified by sagaID. It blocks until
// the saga reaches a terminal state (COMPLETED, FAILED, COMPENSATION_FAILED,
// or ABORTED) and returns the final execution snapshot.
func (c *Client) StartSaga(ctx context.Context, sagaID string) (*pb.SagaExecution, error) {
	resp, err := c.stub.StartSaga(ctx, &pb.StartSagaRequest{SagaId: sagaID})
	if err != nil {
		return nil, err
	}
	return resp.Saga, nil
}

// GetSaga retrieves the current state of the saga identified by sagaID.
func (c *Client) GetSaga(ctx context.Context, sagaID string) (*pb.SagaExecution, error) {
	resp, err := c.stub.GetSaga(ctx, &pb.GetSagaRequest{SagaId: sagaID})
	if err != nil {
		return nil, err
	}
	return resp.Saga, nil
}

// ListSagas returns a page of saga executions matching the request filters.
// The second return value is the next-page token; it is empty when there are
// no more pages.
func (c *Client) ListSagas(ctx context.Context, req *pb.ListSagasRequest) ([]*pb.SagaExecution, string, error) {
	resp, err := c.stub.ListSagas(ctx, req)
	if err != nil {
		return nil, "", err
	}
	return resp.Sagas, resp.NextPageToken, nil
}

// AbortSaga forcibly moves the saga identified by sagaID to the ABORTED state
// without triggering compensation. Returns an error if the saga does not exist
// or is already in a terminal state.
func (c *Client) AbortSaga(ctx context.Context, sagaID string) (*pb.SagaExecution, error) {
	resp, err := c.stub.AbortSaga(ctx, &pb.AbortSagaRequest{SagaId: sagaID})
	if err != nil {
		return nil, err
	}
	return resp.Saga, nil
}
