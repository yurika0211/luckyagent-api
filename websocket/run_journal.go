package websocket

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const minJournalCheckpointBytes int64 = 1 << 20

// Each complete newline-terminated record is one durable mutation. A snapshot
// records its last sequence so replay is safe across interrupted checkpoints.
type journalRecord struct {
	Version   int        `json:"version"`
	SessionID string     `json:"session_id"`
	Sequence  uint64     `json:"sequence"`
	Run       *RunRecord `json:"run,omitempty"`
	Event     *Message   `json:"event,omitempty"`
}

func (s *RunStore) sessionPath(sessionID, extension string) string {
	return filepath.Join(s.root, fmt.Sprintf("%x.%s", stableSessionHash(sessionID), extension))
}

func (s *RunStore) loadLocked(entry *runSession) error {
	if entry.loaded {
		return entry.loadErr
	}
	entry.loaded = true
	if s.root != "" {
		path := s.sessionPath(entry.data.SessionID, "json")
		raw, err := os.ReadFile(path)
		if err == nil {
			var snapshot persistedSession
			if err = json.Unmarshal(raw, &snapshot); err != nil {
				entry.loadErr = fmt.Errorf("load websocket snapshot %s: %w", path, err)
			} else if snapshot.SessionID != "" && snapshot.SessionID != entry.data.SessionID {
				entry.loadErr = fmt.Errorf("websocket snapshot session mismatch: %s", path)
			} else if snapshot.Version > 1 {
				entry.loadErr = fmt.Errorf("unsupported websocket snapshot version %d", snapshot.Version)
			} else {
				snapshot.Version, snapshot.SessionID = 1, entry.data.SessionID
				entry.data, entry.snapshotBytes = snapshot, int64(len(raw))
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			entry.loadErr = err
		}
	}
	entry.indexRuns()
	if entry.loadErr == nil && s.root != "" {
		entry.loadErr = s.loadJournalLocked(entry)
	}
	if entry.loadErr == nil {
		s.pruneLocked(entry)
	}
	return entry.loadErr
}

func (s *RunStore) loadJournalLocked(entry *runSession) error {
	path := s.sessionPath(entry.data.SessionID, "jsonl")
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var validBytes int64
	for {
		line, readErr := reader.ReadBytes('\n')
		if errors.Is(readErr, io.EOF) {
			if len(line) > 0 {
				// A crash may leave even syntactically valid JSON without the
				// commit newline. Remove it before a subsequent append.
				if err := truncateJournal(path, validBytes); err != nil {
					return err
				}
			}
			entry.journalBytes = validBytes
			return nil
		}
		if readErr != nil {
			return readErr
		}
		var record journalRecord
		if err := json.Unmarshal(line, &record); err != nil {
			if _, nextErr := reader.Peek(1); errors.Is(nextErr, io.EOF) {
				if err := truncateJournal(path, validBytes); err != nil {
					return err
				}
				entry.journalBytes = validBytes
				return nil
			}
			return fmt.Errorf("invalid websocket journal record at byte %d: %w", validBytes, err)
		}
		if record.Version != 1 || record.SessionID != entry.data.SessionID || record.Sequence == 0 || (record.Run == nil) == (record.Event == nil) {
			return fmt.Errorf("invalid websocket journal record at byte %d", validBytes)
		}
		if record.Run != nil && record.Run.SessionID != record.SessionID {
			return fmt.Errorf("websocket journal run session mismatch at byte %d", validBytes)
		}
		if record.Sequence > entry.data.Sequence {
			if record.Sequence != entry.data.Sequence+1 {
				return fmt.Errorf("websocket journal sequence gap at byte %d", validBytes)
			}
			if record.Run != nil {
				entry.setRun(*record.Run)
			} else {
				entry.data.Events = append(entry.data.Events, cloneMessage(record.Event))
			}
			entry.data.Sequence = record.Sequence
		}
		validBytes += int64(len(line))
	}
}

func truncateJournal(path string, length int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(length); err != nil {
		return err
	}
	return file.Sync()
}

func (s *RunStore) appendLocked(entry *runSession, record journalRecord) error {
	record.Version, record.SessionID, record.Sequence = 1, entry.data.SessionID, entry.data.Sequence+1
	if s.root == "" {
		entry.data.Sequence = record.Sequence
		return nil
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	path := s.sessionPath(entry.data.SessionID, "jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	rollback := func(cause error) error {
		if err := file.Truncate(info.Size()); err != nil {
			entry.loadErr = errors.Join(cause, fmt.Errorf("rollback websocket journal: %w", err))
			return entry.loadErr
		}
		if err := file.Sync(); err != nil {
			entry.loadErr = errors.Join(cause, fmt.Errorf("sync websocket journal rollback: %w", err))
			return entry.loadErr
		}
		return cause
	}
	if count, err := file.Write(raw); err != nil {
		return rollback(err)
	} else if count != len(raw) {
		return rollback(io.ErrShortWrite)
	}
	if err := file.Sync(); err != nil {
		return rollback(err)
	}
	if info.Size() == 0 {
		if err := syncDirectory(s.root); err != nil {
			return rollback(err)
		}
	}
	entry.data.Sequence = record.Sequence
	entry.journalBytes = info.Size() + int64(len(raw))
	return nil
}

func (s *RunStore) maintainLocked(entry *runSession) {
	if s.root == "" {
		return
	}
	// Growing snapshots use a geometric checkpoint interval, so appending N
	// events does not repeatedly encode/write the preceding N events. Hourly
	// checkpoints also discard records removed by the retention policy.
	threshold := max(minJournalCheckpointBytes, entry.snapshotBytes)
	if entry.journalBytes < threshold && time.Since(entry.data.CheckpointAt) < time.Hour {
		return
	}
	if err := s.checkpointLocked(entry); err != nil {
		// The mutation was already committed to the journal. A failed optional
		// checkpoint must not turn an accepted run into an apparent failure.
		slog.Warn("websocket journal checkpoint failed", "session", entry.data.SessionID, "error", err)
	}
}

func (s *RunStore) checkpointLocked(entry *runSession) error {
	s.pruneLocked(entry)
	snapshot := entry.data
	snapshot.CheckpointAt = time.Now().UTC()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := replaceRunFile(s.sessionPath(snapshot.SessionID, "json"), raw); err != nil {
		return err
	}
	entry.data.CheckpointAt, entry.snapshotBytes = snapshot.CheckpointAt, int64(len(raw))
	// Publishing the snapshot first is essential. If rotation is interrupted,
	// the loader skips journal sequences already represented by the snapshot.
	err = replaceRunFile(s.sessionPath(snapshot.SessionID, "jsonl"), nil)
	if info, statErr := os.Stat(s.sessionPath(snapshot.SessionID, "jsonl")); statErr == nil {
		entry.journalBytes = info.Size()
	}
	return err
}

func replaceRunFile(path string, raw []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".runs-*.tmp")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := io.Copy(file, bytes.NewReader(raw)); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (s *RunStore) discoverSessions() []string {
	if s.root == "" {
		return nil
	}
	files, err := os.ReadDir(s.root)
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	for _, entry := range files {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(s.root, entry.Name())
		var raw []byte
		if strings.HasSuffix(entry.Name(), ".json") {
			raw, err = os.ReadFile(path)
		} else if strings.HasSuffix(entry.Name(), ".jsonl") {
			file, openErr := os.Open(path)
			if openErr != nil {
				continue
			}
			raw, err = bufio.NewReader(file).ReadBytes('\n')
			_ = file.Close()
		} else {
			continue
		}
		if err != nil {
			continue
		}
		var header struct {
			SessionID string `json:"session_id"`
		}
		if json.Unmarshal(raw, &header) == nil && header.SessionID != "" {
			seen[header.SessionID] = true
		}
	}
	result := make([]string, 0, len(seen))
	for sessionID := range seen {
		result = append(result, sessionID)
	}
	return result
}
