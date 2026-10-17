package websocket

// NewID allocates a wire-event ID for host-persisted event streams.
func NewID() string { return generateID() }
