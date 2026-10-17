package websocket

import "sync"

type mockHandler struct {
	mu      sync.Mutex
	msgs    []*Message
	clients []*Client
}

func (h *mockHandler) HandleMessage(client *Client, msg *Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, msg)
	h.clients = append(h.clients, client)
}
