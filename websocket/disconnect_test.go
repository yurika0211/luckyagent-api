package websocket

import "testing"

func TestHubDisconnectDoesNotCancelSession(t *testing.T) {
	handler := &cancelTrackingHandler{}
	hub := NewHub(handler, DefaultHubConfig())
	client := &Client{
		ID:        "client-1",
		SessionID: "disconnect-session",
		Send:      make(chan *Message, 1),
		Conn:      nil,
	}
	hub.clients[client.ID] = client
	hub.sessions[client.SessionID] = map[string]bool{client.ID: true}
	hub.unregisterClient(client)
	if handler.cancelled != 0 {
		t.Fatalf("disconnect cancelled %d runs", handler.cancelled)
	}
}

type cancelTrackingHandler struct {
	cancelled int
}

func (h *cancelTrackingHandler) HandleMessage(*Client, *Message) {}

func (h *cancelTrackingHandler) CancelSession(string) {
	h.cancelled++
}
