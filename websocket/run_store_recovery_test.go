package websocket

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yurika0211/luckyagent-gateway"
)

func TestRecoveryScanLeavesEventHistoryLazy(t *testing.T) {
	dir := t.TempDir()
	store := NewRunStore(dir)
	run := RunRecord{ID: "active", SessionID: "session", State: "running", TaskID: "task", MaxIter: 7, Attachments: []gateway.Attachment{{}}}
	if err := store.UpsertRun(run); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEvent("session", "active", &Message{ID: "event"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertRun(RunRecord{ID: "done", SessionID: "completed", State: "completed"}); err != nil {
		t.Fatal(err)
	}
	restored := NewRunStore(dir)
	for i := 0; i < 2; i++ {
		runs := restored.Restoreable()
		if len(runs) != 1 || runs[0].TaskID != "task" || runs[0].MaxIter != 7 || len(runs[0].Attachments) != 1 {
			t.Fatalf("recovery = %#v", runs)
		}
		if len(restored.sessions) != 0 {
			t.Fatal("scan retained session histories")
		}
	}
	if events := restored.ReplayForReconnect("session", ""); len(events) != 1 || events[0].ID != "event" {
		t.Fatalf("lazy replay = %#v", events)
	}
}

func TestRecoveryScanSupportsFieldOrderAndSkipsTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	fixtures := map[string]string{
		"active.json":     `{"events":[{"data":{"nested":[{"value":true},null,42]}}],"runs":[{"id":"r","state":"queued"},{"id":"done","state":"completed"}],"session_id":"s"}`,
		".hidden.json":    `{"session_id":"hidden","runs":[{"id":"bad","state":"running"}]}`,
		"active.tmp.json": `{"session_id":"temporary","runs":[{"id":"bad","state":"running"}]}`,
		"invalid.json":    `{"session_id":true}`,
	}
	for name, data := range fixtures {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	runs := NewRunStore(dir).Restoreable()
	if len(runs) != 1 || runs[0].ID != "r" || runs[0].SessionID != "s" {
		t.Fatalf("recovery = %#v", runs)
	}
	// The current writer's metadata precedes events, so even an unread event
	// tail is not decoded by the startup metadata scanner.
	path := filepath.Join(dir, "metadata-only.json")
	if err := os.WriteFile(path, []byte(`{"session_id":"s","runs":[{"id":"r","state":"queued"}],"events":[`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, records, err := scanActiveRunsFromFile(path); err != nil || len(records) != 1 {
		t.Fatalf("metadata scan = %#v, %v", records, err)
	}
}

func TestRecoveryUsesCachedAndMemoryOnlyRuns(t *testing.T) {
	for _, root := range []string{"", t.TempDir()} {
		store := NewRunStore(root)
		if err := store.UpsertRun(RunRecord{ID: "r", SessionID: "s", State: "running", Attachments: []gateway.Attachment{{FileName: "original.png"}}}); err != nil {
			t.Fatal(err)
		}
		if root != "" {
			data, _ := json.Marshal(persistedSession{SessionID: "s", Runs: []*RunRecord{{ID: "stale", State: "queued"}}})
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%x.json", stableSessionHash("s"))), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
		runs := store.Restoreable()
		if len(runs) != 1 || runs[0].ID != "r" {
			t.Fatalf("cached recovery = %#v", runs)
		}
		runs[0].Attachments[0].FileName = "changed.png"
		if store.sessions["s"].Runs[0].Attachments[0].FileName != "original.png" {
			t.Fatal("attachments changed")
		}
	}
}
