//go:build windows

package websocket

// Windows does not support os.File.Sync on directory handles. Individual log
// and snapshot files are still synced before acknowledgment/publication, but
// directory-entry persistence across power loss has the platform's guarantee.
func syncDirectory(string) error { return nil }
