package dashboard_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/dashboard"
	"github.com/ngaddam369/saga-conductor/internal/saga"
)

// openSSE starts an SSE request against b.SSEHandler and returns a channel
// that emits each raw "data: ..." line received from the stream.
func openSSE(t *testing.T, b *dashboard.Broadcaster) <-chan string {
	t.Helper()
	srv := httptest.NewServer(b.SSEHandler())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("build SSE request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("SSE GET: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	lines := make(chan string, 16)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				lines <- data
			}
		}
	}()
	return lines
}

func TestBroadcasterPublishesUpdateToSSEClient(t *testing.T) {
	t.Parallel()
	b := dashboard.NewBroadcaster()
	lines := openSSE(t, b)

	b.OnUpdate(&saga.Execution{
		ID:     "saga-1",
		Name:   "order-saga",
		Status: saga.SagaStatusRunning,
		Steps:  []saga.StepExecution{{Name: "reserve", Status: saga.StepStatusRunning}},
	})

	select {
	case data := <-lines:
		if !strings.Contains(data, `"saga-1"`) {
			t.Errorf("expected saga ID in SSE payload, got: %s", data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE event")
	}
}

func TestBroadcasterFansOutToMultipleClients(t *testing.T) {
	t.Parallel()
	b := dashboard.NewBroadcaster()

	const n = 3
	clients := make([]<-chan string, n)
	for i := range n {
		clients[i] = openSSE(t, b)
	}

	// Give all clients time to subscribe before publishing.
	time.Sleep(50 * time.Millisecond)

	b.OnUpdate(&saga.Execution{ID: "s", Name: "n", Status: saga.SagaStatusCompleted})

	for i, ch := range clients {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("client %d did not receive the event", i)
		}
	}
}

func TestBroadcasterOnUpdateWithNoSubscribersDoesNotBlock(t *testing.T) {
	t.Parallel()
	b := dashboard.NewBroadcaster()

	done := make(chan struct{})
	go func() {
		b.OnUpdate(&saga.Execution{ID: "s", Name: "n", Status: saga.SagaStatusRunning})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("OnUpdate blocked with no subscribers")
	}
}
