package websocket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yurika0211/luckyagent-gateway"
)

func testRun(sessionID, runID string) RunRecord {
	now := time.Now().UTC()
	return RunRecord{ID: runID, SessionID: sessionID, ParentID: runID, Message: "hello", Stream: true, State: "queued", CreatedAt: now, UpdatedAt: now}
}

func appendTestEvent(t *testing.T, store *RunStore, sessionID, runID, id string) {
	t.Helper()
	msg := &Message{ID: id, EventID: id, Type: TypeStreamChunk, Timestamp: time.Now().UTC(), Data: json.RawMessage(`{"content":"hello"}`)}
	if err := store.AppendEvent(sessionID, runID, msg); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRunIsAtomicAndRejectsConflictingInput(t *testing.T) {
	store := NewRunStore(t.TempDir())
	run := testRun("session", "run")
	run.ProfileID = "profile"
	run.Attachments = []gateway.Attachment{{FileID: "file", Data: []byte("data"), Metadata: map[string]string{"key": "value"}}}
	var created atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, fresh, err := store.CreateRun(run)
			if err != nil {
				t.Error(err)
			}
			if fresh {
				created.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatalf("created=%d", created.Load())
	}
	if err := store.UpdateRun(run.SessionID, run.ID, func(record *RunRecord) { record.State = "completed"; record.TaskID = "task" }); err != nil {
		t.Fatal(err)
	}
	stored, fresh, err := store.CreateRun(run)
	if err != nil || fresh || stored.State != "completed" || stored.TaskID != "task" {
		t.Fatalf("retry: %#v %v %v", stored, fresh, err)
	}
	for name, change := range map[string]func(*RunRecord){
		"message":    func(r *RunRecord) { r.Message += " changed" },
		"stream":     func(r *RunRecord) { r.Stream = false },
		"iterations": func(r *RunRecord) { r.MaxIter++ },
		"profile":    func(r *RunRecord) { r.ProfileID = "another" },
		"attachment": func(r *RunRecord) { r.Attachments[0].Metadata["key"] = "changed" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := cloneRun(run)
			change(&changed)
			if _, fresh, err := store.CreateRun(changed); !errors.Is(err, ErrRunConflict) || fresh {
				t.Fatalf("fresh=%v err=%v", fresh, err)
			}
		})
	}
	stored.Attachments[0].Metadata["key"] = "mutated"
	run.Attachments[0].Data[0] = 'X'
	inMemory, _, _ := store.GetRun("session", "run")
	if inMemory.Attachments[0].Metadata["key"] != "value" || string(inMemory.Attachments[0].Data) != "data" {
		t.Fatal("input or CreateRun result mutated stored attachments")
	}
	inMemory.Attachments[0].Data[0] = 'Y'
	inMemory.Attachments[0].Metadata["key"] = "changed"
	independent, _, _ := store.GetRun("session", "run")
	if independent.Attachments[0].Metadata["key"] != "value" || string(independent.Attachments[0].Data) != "data" {
		t.Fatal("GetRun returned shared attachment storage")
	}
	restored, found, err := NewRunStore(store.root).GetRun("session", "run")
	if err != nil || !found || restored.ProfileID != "profile" || restored.Attachments[0].Metadata["key"] != "value" || string(restored.Attachments[0].Data) != "data" {
		t.Fatalf("restored=%#v found=%v err=%v", restored, found, err)
	}
}

func TestRunStoreLoadsLegacySnapshotAndKeepsCompletedEndEvent(t *testing.T) {
	store := NewRunStore(t.TempDir())
	run := testRun("legacy", "old")
	run.State = "completed"
	legacy := persistedSession{SessionID: "legacy", Runs: []*RunRecord{&run}, Events: []*Message{{ID: "old-event", RunID: run.ID, Timestamp: time.Now().UTC()}}}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(store.sessionPath("legacy", "json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	newRun := testRun("legacy", "new")
	if _, created, err := store.CreateRun(newRun); err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	end := &Message{ID: "end", EventID: "end-cursor", Type: TypeStreamEnd, Timestamp: time.Now().UTC()}
	if err := store.AppendEvent("legacy", "new", end); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateRun("legacy", "new", func(run *RunRecord) { run.State = "completed" }); err != nil {
		t.Fatal(err)
	}
	restored := NewRunStore(store.root)
	if runs := restored.Restoreable(); len(runs) != 0 {
		t.Fatalf("active runs=%#v", runs)
	}
	if events := restored.ReplayForReconnect("legacy", "old-event"); len(events) != 1 || events[0].ID != "end" || events[0].EventID != "end-cursor" {
		t.Fatalf("events=%#v", events)
	}
	if events := restored.ReplayForReconnect("legacy", ""); len(events) != 0 {
		t.Fatalf("completed replay without cursor=%#v", events)
	}
	if events := restored.ReplayForReconnect("legacy", "end-cursor"); len(events) != 0 {
		t.Fatalf("cursor replay=%#v", events)
	}
}

func TestRunStoreRepairsIncompleteJournalTailBeforeAppending(t *testing.T) {
	for _, tail := range []string{`{"version":`, "{broken}\n", `{"version":1,"session_id":"s","sequence":3}`} {
		t.Run(fmt.Sprintf("tail-%x", tail), func(t *testing.T) {
			store := NewRunStore(t.TempDir())
			if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
				t.Fatal(err)
			}
			appendTestEvent(t, store, "s", "r", "before")
			path := store.sessionPath("s", "jsonl")
			valid, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(valid, tail...), 0o600); err != nil {
				t.Fatal(err)
			}
			restored := NewRunStore(store.root)
			if _, found, err := restored.GetRun("s", "r"); err != nil || !found {
				t.Fatalf("found=%v err=%v", found, err)
			}
			if repaired, _ := os.ReadFile(path); !bytes.Equal(repaired, valid) {
				t.Fatalf("tail not removed: %s", repaired)
			}
			appendTestEvent(t, restored, "s", "r", "after")
			if events := NewRunStore(store.root).Replay("s", ""); len(events) != 2 || events[0].ID != "before" || events[1].ID != "after" {
				t.Fatalf("events=%#v", events)
			}
		})
	}
}

func TestRunStoreDoesNotIgnoreCorruptJournalMiddle(t *testing.T) {
	store := NewRunStore(t.TempDir())
	if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
		t.Fatal(err)
	}
	appendTestEvent(t, store, "s", "r", "first")
	appendTestEvent(t, store, "s", "r", "last")
	path := store.sessionPath("s", "jsonl")
	raw, _ := os.ReadFile(path)
	newline := bytes.IndexByte(raw, '\n') + 1
	corrupt := append(append(append([]byte(nil), raw[:newline]...), []byte("{bad}\n")...), raw[newline:]...)
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	restored := NewRunStore(store.root)
	if _, _, err := restored.GetRun("s", "r"); err == nil {
		t.Fatal("corrupt middle was silently accepted")
	}
	if _, err := restored.ReplayForReconnectWithError("s", ""); err == nil {
		t.Fatal("corrupt replay was reported as successful")
	}
	if _, err := restored.ReplayWithError("s", ""); err == nil {
		t.Fatal("corrupt full replay was reported as successful")
	}
	if _, _, err := restored.CreateRun(testRun("s", "another")); err == nil {
		t.Fatal("accepted mutation after corruption")
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(after, corrupt) {
		t.Fatal("corrupt middle was truncated")
	}
}

func TestCreateRunDefaultsRemainIdempotentAcrossRestart(t *testing.T) {
	store := NewRunStore(t.TempDir())
	run := RunRecord{ID: "r", SessionID: "s", Message: "input"}
	stored, created, err := store.CreateRun(run)
	if err != nil || !created || stored.State != "queued" || stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatalf("stored=%#v created=%v err=%v", stored, created, err)
	}
	if stored, created, err := NewRunStore(store.root).CreateRun(run); err != nil || created || stored.State != "queued" {
		t.Fatalf("retry=%#v created=%v err=%v", stored, created, err)
	}
}

func TestRunStoreReplaysAfterEventIDCursor(t *testing.T) {
	store := NewRunStore("")
	if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		msg := &Message{ID: fmt.Sprint(i), EventID: fmt.Sprintf("cursor-%d", i), Timestamp: time.Now().UTC()}
		if err := store.AppendEvent("s", "r", msg); err != nil {
			t.Fatal(err)
		}
	}
	for _, replay := range []func(string, string) ([]*Message, error){store.ReplayWithError, store.ReplayForReconnectWithError} {
		events, err := replay("s", "cursor-0")
		if err != nil || len(events) != 1 || events[0].ID != "1" {
			t.Fatalf("cursor replay=%#v err=%v", events, err)
		}
	}
}

func TestRunStoreCheckpointDoesNotDuplicateOldJournalOnRestart(t *testing.T) {
	store := NewRunStore(t.TempDir())
	if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
		t.Fatal(err)
	}
	appendTestEvent(t, store, "s", "r", "first")
	path := store.sessionPath("s", "jsonl")
	oldJournal, _ := os.ReadFile(path)
	entry := store.session("s")
	entry.mu.Lock()
	err := store.checkpointLocked(entry)
	entry.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after snapshot publication but before log rotation.
	if err := os.WriteFile(path, oldJournal, 0o600); err != nil {
		t.Fatal(err)
	}
	restored := NewRunStore(store.root)
	if events := restored.Replay("s", ""); len(events) != 1 || events[0].ID != "first" {
		t.Fatalf("events=%#v", events)
	}
	appendTestEvent(t, restored, "s", "r", "second")
	if events := NewRunStore(store.root).Replay("s", ""); len(events) != 2 {
		t.Fatalf("events=%#v", events)
	}
}

func TestRunStoreFailedWritesDoNotChangeAcceptedState(t *testing.T) {
	store := NewRunStore(t.TempDir())
	if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
		t.Fatal(err)
	}
	path := store.sessionPath("s", "jsonl")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateRun(testRun("s", "new")); err == nil {
		t.Fatal("create succeeded with unavailable journal")
	}
	if _, found, err := store.GetRun("s", "new"); err != nil || found {
		t.Fatalf("unaccepted run found=%v err=%v", found, err)
	}
	if err := store.UpdateRun("s", "r", func(run *RunRecord) { run.State = "completed" }); err == nil {
		t.Fatal("update succeeded with unavailable journal")
	}
	if run, _, _ := store.GetRun("s", "r"); run.State != "queued" {
		t.Fatalf("state changed after failure: %s", run.State)
	}
	if err := store.AppendEvent("s", "r", &Message{ID: "e", Timestamp: time.Now().UTC()}); err == nil {
		t.Fatal("event append succeeded with unavailable journal")
	}
	if events := store.Replay("s", ""); len(events) != 0 {
		t.Fatalf("unaccepted events=%#v", events)
	}
}

func TestRunStoreAppendsBoundedRecordsWithoutRewritingSnapshot(t *testing.T) {
	store := NewRunStore(t.TempDir())
	if _, _, err := store.CreateRun(testRun("s", "r")); err != nil {
		t.Fatal(err)
	}
	snapshotPath := store.sessionPath("s", "json")
	before, _ := os.ReadFile(snapshotPath)
	var prefix []byte
	for i := 0; i < 40; i++ {
		msg, _ := NewMessage(TypeStreamChunk, "s", StreamChunkData{Content: strings.Repeat("x", 1024)})
		if err := store.AppendEvent("s", "r", msg); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			prefix, _ = os.ReadFile(store.sessionPath("s", "jsonl"))
		}
	}
	after, _ := os.ReadFile(snapshotPath)
	if !bytes.Equal(before, after) {
		t.Fatal("streaming rewrote the snapshot before a checkpoint was due")
	}
	journal, _ := os.ReadFile(store.sessionPath("s", "jsonl"))
	if !bytes.HasPrefix(journal, prefix) || bytes.Count(journal, []byte{'\n'}) != 40 || len(journal) > 40*2048 {
		t.Fatalf("journal is not bounded append-only: bytes=%d", len(journal))
	}
}

func TestRunStoreRetentionPreservesOldActiveRunsOnly(t *testing.T) {
	store := NewRunStore(t.TempDir())
	old := time.Now().Add(-runStoreRetention - time.Hour)
	active, done := testRun("s", "active"), testRun("s", "done")
	active.CreatedAt, active.UpdatedAt, done.CreatedAt, done.UpdatedAt = old, old, old, old
	done.State = "completed"
	legacy := persistedSession{Version: 1, SessionID: "s", Runs: []*RunRecord{&active, &done}, Events: []*Message{
		{ID: "active-old", RunID: active.ID, Timestamp: old}, {ID: "done-old", RunID: done.ID, Timestamp: old},
	}}
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(store.sessionPath("s", "json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if events := store.Replay("s", ""); len(events) != 1 || events[0].ID != "active-old" {
		t.Fatalf("events=%#v", events)
	}
	if runs := store.Restoreable(); len(runs) != 1 || runs[0].ID != "active" {
		t.Fatalf("runs=%#v", runs)
	}
	if err := store.UpdateRun("s", "active", func(run *RunRecord) { run.State = "completed" }); err != nil {
		t.Fatal(err)
	}
	if events := NewRunStore(store.root).Replay("s", ""); len(events) != 0 {
		t.Fatalf("expired completed events=%#v", events)
	}
}

func TestRunStoreOneSessionDoesNotHoldAnotherSessionLock(t *testing.T) {
	store := NewRunStore("")
	first := store.session("first")
	first.mu.Lock()
	defer first.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, _, err := store.CreateRun(testRun("second", "r")); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("one session blocked another session")
	}
}
