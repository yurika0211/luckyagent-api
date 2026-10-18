package websocket

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecoveryJournalKeepsCompletedHistoriesLazy(t *testing.T) {
	store := NewRunStore(t.TempDir())
	for _, id := range []string{"active", "completed"} {
		if _, _, err := store.CreateRun(testRun(id, "r")); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			message, _ := NewMessage(TypeStreamChunk, id, StreamChunkData{Content: strings.Repeat("history", 4096)})
			if err := store.AppendEvent(id, "r", message); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.UpdateRun(id, "r", func(run *RunRecord) {
			run.TaskID = "latest-task"
			if id == "completed" {
				run.State = "completed"
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	restored := NewRunStore(store.root)
	for i := 0; i < 2; i++ {
		runs := restored.Restoreable()
		if len(runs) != 1 || runs[0].SessionID != "active" || runs[0].TaskID != "latest-task" {
			t.Fatalf("recovery=%#v", runs)
		}
		if len(restored.sessions) != 0 {
			t.Fatal("metadata scan cached journal event histories")
		}
	}
	if events, err := restored.ReplayWithError("active", ""); err != nil || len(events) != 3 {
		t.Fatalf("lazy events=%d err=%v", len(events), err)
	}
	if len(restored.sessions) != 1 || restored.sessions["completed"] != nil {
		t.Fatal("lazy replay loaded an unrelated completed history")
	}
}

func TestRecoveryJournalOnlySupportsFieldOrderWithoutCachingEvents(t *testing.T) {
	store := NewRunStore(t.TempDir())
	run := testRun("journal-only", "r")
	run.ProfileID, run.TaskID = "profile", "task"
	rawRun, _ := json.Marshal(run)
	// Events and unknown nested fields deliberately precede the headers.
	journal := []byte(`{"run":` + string(rawRun) + `,"sequence":1,"session_id":"journal-only","version":1}` + "\n" +
		`{"event":{"data":{"nested":[{"value":true},null,42]}},"unknown":{"ignored":[1,2]},"session_id":"journal-only","version":1,"sequence":2}` + "\n")
	path := store.sessionPath(run.SessionID, "jsonl")
	if err := os.WriteFile(path, journal, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".hidden.jsonl", "active.tmp.jsonl"} {
		if err := os.WriteFile(filepath.Join(store.root, name), journal, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		runs := store.Restoreable()
		if len(runs) != 1 || runs[0].ProfileID != "profile" || runs[0].TaskID != "task" {
			t.Fatalf("journal recovery=%#v", runs)
		}
		if len(store.sessions) != 0 {
			t.Fatal("journal-only scan cached a session")
		}
	}
}

func TestRecoveryFindsSnapshotSequenceAfterEvents(t *testing.T) {
	store := NewRunStore(t.TempDir())
	active := testRun("s", "r")
	rawRun, _ := json.Marshal(active)
	// The snapshot already contains completed state at seq=3. An old running
	// journal record at seq=2 must not resurrect it even with reordered fields.
	snapshot := []byte(`{"version":1,"session_id":"s","runs":[],"events":[{"data":{"nested":[1,null]}}],"last_sequence":3}`)
	if err := os.WriteFile(store.sessionPath("s", "json"), snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	journal := []byte(`{"version":1,"session_id":"s","sequence":2,"run":` + string(rawRun) + `}` + "\n")
	if err := os.WriteFile(store.sessionPath("s", "jsonl"), journal, 0o600); err != nil {
		t.Fatal(err)
	}
	if runs := store.Restoreable(); len(runs) != 0 {
		t.Fatalf("stale journal resurrected completed run: %#v", runs)
	}
	if len(store.sessions) != 0 {
		t.Fatal("recovery cached snapshot history")
	}
	metadata, err := scanRunSnapshot(store.sessionPath("s", "json"))
	if err != nil || metadata.sequence != 3 {
		t.Fatalf("snapshot sequence=%d err=%v", metadata.sequence, err)
	}
}

func TestRecoveryJournalTailIsLazyAndInteriorCorruptionSkipsSession(t *testing.T) {
	for _, test := range []struct {
		name, tail string
		recover    bool
	}{
		{"partial", `{"version":`, true},
		{"bad-final-line", "{bad}\n", true},
		{"valid-json-no-newline", `{"version":1,"session_id":"s","sequence":3,"run":{"id":"r","session_id":"s","state":"completed"}}`, true},
		{"corrupt-middle", "{bad}\n{\"version\":1,\"session_id\":\"s\",\"sequence\":3,\"run\":{\"id\":\"r\",\"session_id\":\"s\",\"state\":\"completed\"}}\n", false},
		{"unfinished-middle", "{\"event\":\n{\"version\":1,\"session_id\":\"s\",\"sequence\":3,\"run\":{\"id\":\"r\",\"session_id\":\"s\",\"state\":\"completed\"}}\n", false},
		{"sequence-gap", "{\"version\":1,\"session_id\":\"s\",\"sequence\":9,\"run\":{\"id\":\"r\",\"session_id\":\"s\",\"state\":\"completed\"}}\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewRunStore(t.TempDir())
			if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
				t.Fatal(err)
			}
			appendTestEvent(t, store, "s", "r", "event")
			path := store.sessionPath("s", "jsonl")
			valid, _ := os.ReadFile(path)
			damaged := append(append([]byte(nil), valid...), test.tail...)
			if err := os.WriteFile(path, damaged, 0o600); err != nil {
				t.Fatal(err)
			}
			restored := NewRunStore(store.root)
			runs := restored.Restoreable()
			if (len(runs) == 1) != test.recover {
				t.Fatalf("recover=%v runs=%#v", test.recover, runs)
			}
			if len(restored.sessions) != 0 {
				t.Fatal("recovery cached a damaged session")
			}
			if after, _ := os.ReadFile(path); !bytes.Equal(after, damaged) {
				t.Fatal("metadata scan modified journal")
			}
			if test.recover {
				if events, err := restored.ReplayWithError("s", ""); err != nil || len(events) != 1 {
					t.Fatalf("lazy recovery events=%#v err=%v", events, err)
				}
				if repaired, _ := os.ReadFile(path); !bytes.Equal(repaired, valid) {
					t.Fatal("lazy full loader did not repair tail")
				}
			}
		})
	}
}

func TestRecoverySnapshotSequenceKeepsOldJournalFromResurrectingRun(t *testing.T) {
	store := NewRunStore(t.TempDir())
	if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateRun("s", "r", func(run *RunRecord) { run.State = "running" }); err != nil {
		t.Fatal(err)
	}
	oldJournal, _ := os.ReadFile(store.sessionPath("s", "jsonl"))
	if err := store.UpdateRun("s", "r", func(run *RunRecord) { run.State = "completed" }); err != nil {
		t.Fatal(err)
	}
	entry := store.session("s")
	entry.mu.Lock()
	err := store.checkpointLocked(entry)
	entry.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.sessionPath("s", "jsonl"), oldJournal, 0o600); err != nil {
		t.Fatal(err)
	}
	if runs := NewRunStore(store.root).Restoreable(); len(runs) != 0 {
		t.Fatalf("completed run recovered from stale log: %#v", runs)
	}
}

func TestRecoveryJournalKeepsLatestActiveMetadataAndCreationOrder(t *testing.T) {
	store := NewRunStore(t.TempDir())
	first, second := testRun("s", "first"), testRun("s", "second")
	second.CreatedAt = first.CreatedAt.Add(time.Second)
	for _, run := range []RunRecord{first, second} {
		if _, _, err := store.CreateRun(run); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpdateRun("s", "first", func(run *RunRecord) { run.TaskID = "task-first"; run.MaxIter = 7; run.ProfileID = "profile" }); err != nil {
		t.Fatal(err)
	}
	runs := NewRunStore(store.root).Restoreable()
	if len(runs) != 2 || runs[0].ID != "first" || runs[1].ID != "second" || runs[0].TaskID != "task-first" || runs[0].MaxIter != 7 || runs[0].ProfileID != "profile" {
		t.Fatalf("ordered recovery=%#v", runs)
	}
}
