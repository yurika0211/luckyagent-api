package websocket

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestClientReliableReplayPreservesMoreThanBufferAndOrdersLiveEvents(t *testing.T) {
	client := &Client{Send: make(chan *Message, clientSendBuffer)}
	defer client.Close()
	if err := client.BeginReplay(); err != nil {
		t.Fatal(err)
	}
	if err := client.BeginReplay(); !errors.Is(err, ErrReplayActive) {
		t.Fatalf("duplicate BeginReplay=%v", err)
	}
	// One live event will also appear in the snapshot; the next belongs after it.
	if !client.TrySend(&Message{ID: "event-999"}) || !client.TrySend(&Message{ID: "live-after"}) {
		t.Fatal("live events not buffered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delivered := make(map[string]bool)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 1000; i++ {
			id := fmt.Sprintf("event-%d", i)
			if err := client.SendReliable(ctx, &Message{ID: id}); err != nil {
				done <- err
				return
			}
			delivered[id] = true
		}
		done <- client.EndReplay(ctx, delivered)
	}()
	for i := 0; i <= 1000; i++ {
		want := fmt.Sprintf("event-%d", i)
		if i == 1000 {
			want = "live-after"
		}
		select {
		case msg := <-client.Send:
			if msg == nil || msg.ID != want {
				t.Fatalf("message %d=%#v want=%s", i, msg, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(client.Send) != 0 {
		t.Fatal("duplicate snapshot/live event delivered")
	}
	if !client.TrySend(&Message{ID: "normal"}) || (<-client.Send).ID != "normal" {
		t.Fatal("live delivery did not resume")
	}
}

func TestClientReliableSendIsCancelledAndCloseDoesNotDeadlock(t *testing.T) {
	client := &Client{Send: make(chan *Message, 1)}
	client.Send <- &Message{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.SendReliable(ctx, &Message{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled send=%v", err)
	}
	done := make(chan error, 1)
	go func() { done <- client.SendReliable(context.Background(), &Message{}) }()
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on full Send")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("closed send=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send not interrupted by Close")
	}
	client.Close()
	if client.TrySend(&Message{}) {
		t.Fatal("closed client accepted event")
	}
}

func TestClientReplayOverflowAndFlushCancellation(t *testing.T) {
	client := &Client{Send: make(chan *Message, 1)}
	defer client.Close()
	if err := client.BeginReplay(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < clientSendBuffer; i++ {
		if !client.TrySend(&Message{ID: fmt.Sprint(i)}) {
			t.Fatalf("replay queue full at %d", i)
		}
	}
	if client.TrySend(&Message{ID: "overflow"}) {
		t.Fatal("unbounded replay queue")
	}
	client.Send <- &Message{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.EndReplay(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("EndReplay=%v", err)
	}
}

func TestHubStopInterruptsBlockedSessionAndReliableSends(t *testing.T) {
	hub := NewHub(nil, DefaultHubConfig())
	for i := 0; i < cap(hub.broadcast); i++ {
		hub.SendToSession("s", &Message{})
	}
	blocked := make(chan struct{})
	go func() { hub.SendToSession("s", &Message{}); close(blocked) }()
	client := &Client{Hub: hub, Send: make(chan *Message)}
	defer client.Close()
	reliable := make(chan error, 1)
	go func() { reliable <- client.SendReliable(context.Background(), &Message{}) }()
	hub.Stop()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("SendToSession blocked after Stop")
	}
	select {
	case err := <-reliable:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reliable send=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SendReliable blocked after Stop")
	}
}

func TestHubConfigReadLimitAndSessionEnvelopeDoNotMutateCaller(t *testing.T) {
	hub := NewHub(nil, HubConfig{MaxMessageSize: 128 * 1024})
	defer hub.Stop()
	if limit := (&Client{maxMessageSize: hub.maxMessageSize}).readLimit(); limit != 128*1024 {
		t.Fatalf("read limit=%d", limit)
	}
	message := &Message{SessionID: "original"}
	hub.SendToSession("target", message)
	if message.SessionID != "original" || (<-hub.broadcast).SessionID != "target" {
		t.Fatal("SendToSession mutated caller envelope")
	}
	hub.Broadcast(message)
	if message.SessionID != "original" || (<-hub.broadcast).SessionID != "" {
		t.Fatal("Broadcast mutated caller envelope")
	}
}
