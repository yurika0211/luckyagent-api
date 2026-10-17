package websocket

import (
	"testing"
	"time"
)

func TestRunStoreSurvivesRestartAndPreservesReconnectHistory(t *testing.T) {
	dir := t.TempDir()
	store := NewRunStore(dir)
	now := time.Now()
	if err := store.UpsertRun(RunRecord{ID: "run1", SessionID: "s1", State: "running", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"e1", "e2"} {
		if err := store.AppendEvent("s1", "run1", &Message{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	restored := NewRunStore(dir)
	if runs := restored.Restoreable(); len(runs) != 1 || runs[0].ID != "run1" {
		t.Fatalf("runs = %#v", runs)
	}
	events := restored.ReplayForReconnect("s1", "e1")
	if len(events) != 1 || events[0].ID != "e2" {
		t.Fatalf("events = %#v", events)
	}
	events[0].ID = "mutated"
	if again := restored.ReplayForReconnect("s1", "e1"); len(again) != 1 || again[0].ID != "e2" {
		t.Fatal("caller mutated persisted event")
	}
}
