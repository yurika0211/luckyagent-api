package websocket

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yurika0211/luckyagent-gateway"
)

const runStoreRetention = 7 * 24 * time.Hour

type RunRecord struct {
	ID          string               `json:"id"`
	SessionID   string               `json:"session_id"`
	ParentID    string               `json:"parent_id"`
	Message     string               `json:"message"`
	Stream      bool                 `json:"stream"`
	MaxIter     int                  `json:"max_iterations,omitempty"`
	Attachments []gateway.Attachment `json:"attachments,omitempty"`
	TaskID      string               `json:"task_id,omitempty"`
	State       string               `json:"state"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
}

type persistedSession struct {
	Version   int          `json:"version"`
	SessionID string       `json:"session_id"`
	Runs      []*RunRecord `json:"runs,omitempty"`
	Events    []*Message   `json:"events,omitempty"`
}

type RunStore struct {
	root     string
	mu       sync.Mutex
	sessions map[string]*persistedSession
}

func NewRunStore(root string) *RunStore {
	return &RunStore{root: strings.TrimSpace(root), sessions: make(map[string]*persistedSession)}
}

func (s *RunStore) sessionLocked(sessionID string) *persistedSession {
	if session := s.sessions[sessionID]; session != nil {
		return session
	}
	session := &persistedSession{Version: 1, SessionID: sessionID}
	if s.root != "" {
		path := filepath.Join(s.root, filepath.Base(fmt.Sprintf("%x.json", stableSessionHash(sessionID))))
		if raw, err := os.ReadFile(path); err == nil {
			var loaded persistedSession
			if json.Unmarshal(raw, &loaded) == nil {
				if loaded.Version == 0 {
					loaded.Version = 1
				}
				if loaded.SessionID == "" {
					loaded.SessionID = sessionID
				}
				session = &loaded
			}
		}
	}
	s.sessions[sessionID] = session
	return session
}

func (s *RunStore) saveLocked(session *persistedSession) error {
	if s.root == "" {
		return nil
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.root, filepath.Base(fmt.Sprintf("%x.json", stableSessionHash(session.SessionID))))
	tmp, err := os.CreateTemp(s.root, ".runs-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	encoder := json.NewEncoder(tmp)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(session); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (s *RunStore) pruneLocked(session *persistedSession) {
	cutoff := time.Now().Add(-runStoreRetention)
	active := make(map[string]bool)
	keptRuns := session.Runs[:0]
	for _, run := range session.Runs {
		if run == nil {
			continue
		}
		if run.State == "queued" || run.State == "running" {
			active[run.ID] = true
			keptRuns = append(keptRuns, run)
			continue
		}
		if run.UpdatedAt.After(cutoff) {
			keptRuns = append(keptRuns, run)
		}
	}
	session.Runs = keptRuns
	keptEvents := session.Events[:0]
	for _, event := range session.Events {
		if event == nil || event.Timestamp.Before(cutoff) {
			if event == nil || !active[event.RunID] {
				continue
			}
		}
		keptEvents = append(keptEvents, event)
	}
	session.Events = keptEvents
}

func (s *RunStore) UpsertRun(run RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessionLocked(run.SessionID)
	for i, existing := range session.Runs {
		if existing != nil && existing.ID == run.ID {
			session.Runs[i] = &run
			s.pruneLocked(session)
			return s.saveLocked(session)
		}
	}
	session.Runs = append(session.Runs, &run)
	s.pruneLocked(session)
	return s.saveLocked(session)
}

func (s *RunStore) UpdateRun(sessionID, runID string, update func(*RunRecord)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessionLocked(sessionID)
	for _, run := range session.Runs {
		if run != nil && run.ID == runID {
			update(run)
			run.UpdatedAt = time.Now().UTC()
			s.pruneLocked(session)
			return s.saveLocked(session)
		}
	}
	return nil
}

func (s *RunStore) AppendEvent(sessionID, runID string, msg *Message) error {
	if msg == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessionLocked(sessionID)
	copyMsg := *msg
	copyMsg.Data = append(json.RawMessage(nil), msg.Data...)
	copyMsg.RunID = runID
	session.Events = append(session.Events, &copyMsg)
	s.pruneLocked(session)
	return s.saveLocked(session)
}

func (s *RunStore) Replay(sessionID, lastMessageID string) []*Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessionLocked(sessionID)
	start := 0
	if lastMessageID != "" {
		found := false
		for i, event := range session.Events {
			if event != nil && event.ID == lastMessageID {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			start = 0
		}
	}
	result := make([]*Message, 0, len(session.Events)-start)
	for _, event := range session.Events[start:] {
		if event == nil {
			continue
		}
		copyMsg := *event
		copyMsg.Data = append(json.RawMessage(nil), event.Data...)
		result = append(result, &copyMsg)
	}
	return result
}

func (s *RunStore) ReplayForReconnect(sessionID, lastMessageID string) []*Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessionLocked(sessionID)
	start := 0
	if lastMessageID != "" {
		found := false
		for i, event := range session.Events {
			if event != nil && event.ID == lastMessageID {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return cloneEventsForRuns(session.Events, activeRunIDs(session))
		}
		return cloneEvents(session.Events[start:])
	}
	return cloneEventsForRuns(session.Events, activeRunIDs(session))
}

func activeRunIDs(session *persistedSession) map[string]bool {
	active := make(map[string]bool)
	for _, run := range session.Runs {
		if run != nil && (run.State == "queued" || run.State == "running") {
			active[run.ID] = true
		}
	}
	return active
}

func cloneEvents(events []*Message) []*Message {
	result := make([]*Message, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		copyMsg := *event
		copyMsg.Data = append(json.RawMessage(nil), event.Data...)
		result = append(result, &copyMsg)
	}
	return result
}

func cloneEventsForRuns(events []*Message, runIDs map[string]bool) []*Message {
	result := make([]*Message, 0, len(events))
	for _, event := range events {
		if event == nil || !runIDs[event.RunID] {
			continue
		}
		copyMsg := *event
		copyMsg.Data = append(json.RawMessage(nil), event.Data...)
		result = append(result, &copyMsg)
	}
	return result
}

func (s *RunStore) ActiveRuns(sessionID string) []RunRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.sessionLocked(sessionID)
	result := make([]RunRecord, 0)
	for _, run := range session.Runs {
		if run != nil && (run.State == "queued" || run.State == "running") {
			result = append(result, *run)
		}
	}
	return result
}

func (s *RunStore) Restoreable() []RunRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cached sessions are authoritative, including stores without a disk root.
	// Startup scanning must not turn completed event histories into resident data.
	var result []RunRecord
	loadedFiles := make(map[string]bool, len(s.sessions))
	for id, session := range s.sessions {
		result = append(result, restoreableRunRecords(id, session.Runs)...)
		loadedFiles[fmt.Sprintf("%x.json", stableSessionHash(id))] = true
	}
	if s.root == "" {
		return result
	}
	paths, err := filepath.Glob(filepath.Join(s.root, "*.json"))
	if err != nil {
		return result
	}
	for _, path := range paths {
		if !isWebsocketSessionFile(path) || loadedFiles[filepath.Base(path)] {
			continue
		}
		id, runs, err := scanActiveRunsFromFile(path)
		if err != nil || id == "" || s.sessions[id] != nil {
			continue
		}
		result = append(result, runs...)
	}
	return result
}

func restoreableRunRecords(sessionID string, runs []*RunRecord) []RunRecord {
	var active []RunRecord
	for _, run := range runs {
		if run == nil || (run.State != "queued" && run.State != "running") {
			continue
		}
		copyRun := *run
		if strings.TrimSpace(copyRun.SessionID) == "" {
			copyRun.SessionID = sessionID
		}
		copyRun.Attachments = append([]gateway.Attachment(nil), run.Attachments...)
		active = append(active, copyRun)
	}
	return active
}

// scanActiveRunsFromFile reads recovery metadata without decoding events. The
// writer places session_id and runs before events; older field orders still
// work, but require streaming past intervening values. Event replay remains a
// separate, lazy read through sessionLocked.
func scanActiveRunsFromFile(path string) (string, []RunRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	token, err := decoder.Token()
	if err != nil {
		return "", nil, err
	}
	if token != json.Delim('{') {
		return "", nil, fmt.Errorf("websocket session %s: expected object", filepath.Base(path))
	}
	var sessionID string
	var runs []*RunRecord
	haveRuns := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return "", nil, err
		}
		switch token {
		case "session_id":
			if err := decoder.Decode(&sessionID); err != nil {
				return "", nil, err
			}
		case "runs":
			if err := decoder.Decode(&runs); err != nil {
				return "", nil, err
			}
			haveRuns = true
		default:
			if err := skipJSONValue(decoder); err != nil {
				return "", nil, err
			}
		}
		sessionID = strings.TrimSpace(sessionID)
		if sessionID != "" && haveRuns {
			return sessionID, restoreableRunRecords(sessionID, runs), nil
		}
	}
	if _, err := decoder.Token(); err != nil {
		return "", nil, err
	}
	return sessionID, restoreableRunRecords(sessionID, runs), nil
}

func skipJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	for decoder.More() {
		if delim == '{' {
			if _, err := decoder.Token(); err != nil {
				return err
			}
		}
		if err := skipJSONValue(decoder); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func isWebsocketSessionFile(path string) bool {
	name := filepath.Base(path)
	return !strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".json") && !strings.Contains(name, ".tmp.")
}

func stableSessionHash(sessionID string) uint64 {
	var hash uint64 = 1469598103934665603
	for i := 0; i < len(sessionID); i++ {
		hash ^= uint64(sessionID[i])
		hash *= 1099511628211
	}
	return hash
}
