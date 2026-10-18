package websocket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yurika0211/luckyagent-gateway"
)

const runStoreRetention = 7 * 24 * time.Hour

var ErrRunConflict = errors.New("websocket run id already used with different input")

type RunRecord struct {
	ID          string               `json:"id"`
	SessionID   string               `json:"session_id"`
	ParentID    string               `json:"parent_id"`
	Message     string               `json:"message"`
	Stream      bool                 `json:"stream"`
	MaxIter     int                  `json:"max_iterations,omitempty"`
	ProfileID   string               `json:"profile_id,omitempty"`
	Attachments []gateway.Attachment `json:"attachments,omitempty"`
	TaskID      string               `json:"task_id,omitempty"`
	State       string               `json:"state"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
}

// Sequence makes snapshot replacement followed by journal rotation crash safe.
// Old JSON snapshots without last_sequence are sequence zero.
type persistedSession struct {
	Version      int          `json:"version"`
	SessionID    string       `json:"session_id"`
	Sequence     uint64       `json:"last_sequence,omitempty"`
	CheckpointAt time.Time    `json:"checkpoint_at,omitempty"`
	Runs         []*RunRecord `json:"runs,omitempty"`
	Events       []*Message   `json:"events,omitempty"`
}

type runSession struct {
	mu            sync.Mutex
	loaded        bool
	loadErr       error
	data          persistedSession
	runs          map[string]int
	journalBytes  int64
	snapshotBytes int64
	lastPrune     time.Time
}

type RunStore struct {
	root     string
	mu       sync.Mutex // map only; never held during disk I/O
	sessions map[string]*runSession
}

func NewRunStore(root string) *RunStore {
	return &RunStore{root: strings.TrimSpace(root), sessions: make(map[string]*runSession)}
}

func (s *RunStore) session(sessionID string) *runSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.sessions[sessionID]
	if entry == nil {
		entry = &runSession{data: persistedSession{Version: 1, SessionID: sessionID}}
		s.sessions[sessionID] = entry
	}
	return entry
}

func (entry *runSession) indexRuns() {
	entry.runs = make(map[string]int, len(entry.data.Runs))
	for i, run := range entry.data.Runs {
		if run != nil {
			entry.runs[run.ID] = i
		}
	}
}

func (entry *runSession) setRun(run RunRecord) {
	copyRun := cloneRun(run)
	if index, ok := entry.runs[run.ID]; ok {
		entry.data.Runs[index] = &copyRun
	} else {
		entry.runs[run.ID] = len(entry.data.Runs)
		entry.data.Runs = append(entry.data.Runs, &copyRun)
	}
}

func cloneRun(run RunRecord) RunRecord {
	if run.Attachments != nil {
		run.Attachments = append([]gateway.Attachment(nil), run.Attachments...)
		for i := range run.Attachments {
			a := &run.Attachments[i]
			a.Data = append([]byte(nil), a.Data...)
			if a.Metadata != nil {
				metadata := make(map[string]string, len(a.Metadata))
				for key, value := range a.Metadata {
					metadata[key] = value
				}
				a.Metadata = metadata
			}
		}
	}
	return run
}

func sameRunInput(a, b RunRecord) bool {
	if a.Message != b.Message || a.Stream != b.Stream || a.MaxIter != b.MaxIter || a.ProfileID != b.ProfileID {
		return false
	}
	// Normalize optional empty/nil attachment fields and map key order.
	left, _ := json.Marshal(struct {
		Attachments []gateway.Attachment `json:"attachments,omitempty"`
	}{a.Attachments})
	right, _ := json.Marshal(struct {
		Attachments []gateway.Attachment `json:"attachments,omitempty"`
	}{b.Attachments})
	return bytes.Equal(left, right)
}

// CreateRun atomically creates (session_id, id), durably before returning true.
// An identical retry returns the existing record and created=false, retaining
// its execution state. Different Message, Stream, MaxIter, ProfileID or
// Attachments returns ErrRunConflict without overwriting the original record.
// ParentID, task/state fields and timestamps do not form the input identity.
func (s *RunStore) CreateRun(run RunRecord) (stored RunRecord, created bool, err error) {
	if strings.TrimSpace(run.SessionID) == "" || strings.TrimSpace(run.ID) == "" {
		return RunRecord{}, false, errors.New("websocket run requires session_id and id")
	}
	if run.State == "" {
		run.State = "queued"
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now().UTC()
	}
	if run.UpdatedAt.IsZero() {
		run.UpdatedAt = run.CreatedAt
	}
	entry := s.session(run.SessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return RunRecord{}, false, err
	}
	s.pruneDueLocked(entry)
	if index, ok := entry.runs[run.ID]; ok {
		existing := cloneRun(*entry.data.Runs[index])
		if !sameRunInput(existing, run) {
			return existing, false, fmt.Errorf("%w: session %q run %q", ErrRunConflict, run.SessionID, run.ID)
		}
		return existing, false, nil
	}
	run = cloneRun(run)
	if err := s.appendLocked(entry, journalRecord{Run: &run}); err != nil {
		return RunRecord{}, false, err
	}
	entry.setRun(run)
	s.maintainLocked(entry)
	return cloneRun(run), true, nil
}

// GetRun returns an independent copy of a run and any persistence load error.
func (s *RunStore) GetRun(sessionID, runID string) (RunRecord, bool, error) {
	entry := s.session(sessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return RunRecord{}, false, err
	}
	s.pruneDueLocked(entry)
	if index, ok := entry.runs[runID]; ok {
		return cloneRun(*entry.data.Runs[index]), true, nil
	}
	return RunRecord{}, false, nil
}

// UpsertRun supports legacy callers and explicit replacement. Request admission
// must use CreateRun to obtain idempotent creation semantics.
func (s *RunStore) UpsertRun(run RunRecord) error {
	entry := s.session(run.SessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return err
	}
	run = cloneRun(run)
	if err := s.appendLocked(entry, journalRecord{Run: &run}); err != nil {
		return err
	}
	entry.setRun(run)
	s.pruneLocked(entry)
	s.maintainLocked(entry)
	return nil
}

func (s *RunStore) UpdateRun(sessionID, runID string, update func(*RunRecord)) error {
	entry := s.session(sessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return err
	}
	index, ok := entry.runs[runID]
	if !ok {
		return nil
	}
	run := cloneRun(*entry.data.Runs[index])
	update(&run)
	run.ID, run.SessionID = runID, sessionID
	run.UpdatedAt = time.Now().UTC()
	if err := s.appendLocked(entry, journalRecord{Run: &run}); err != nil {
		return err
	}
	wasActive := isActive(entry.data.Runs[index].State)
	entry.setRun(run)
	if wasActive && !isActive(run.State) {
		s.pruneLocked(entry)
	} else {
		s.pruneDueLocked(entry)
	}
	s.maintainLocked(entry)
	return nil
}

func (s *RunStore) AppendEvent(sessionID, runID string, msg *Message) error {
	if msg == nil {
		return nil
	}
	entry := s.session(sessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return err
	}
	s.pruneDueLocked(entry)
	copyMsg := cloneMessage(msg)
	copyMsg.RunID, copyMsg.SessionID = runID, sessionID
	if copyMsg.Timestamp.Before(time.Now().Add(-runStoreRetention)) {
		index, ok := entry.runs[runID]
		if !ok || !isActive(entry.data.Runs[index].State) {
			return nil
		}
	}
	if err := s.appendLocked(entry, journalRecord{Event: copyMsg}); err != nil {
		return err
	}
	entry.data.Events = append(entry.data.Events, copyMsg)
	s.maintainLocked(entry)
	return nil
}

func isActive(state string) bool { return state == "queued" || state == "running" }

func (s *RunStore) pruneDueLocked(entry *runSession) {
	if time.Since(entry.lastPrune) >= time.Hour {
		s.pruneLocked(entry)
	}
}

func (s *RunStore) pruneLocked(entry *runSession) {
	cutoff := time.Now().Add(-runStoreRetention)
	active := make(map[string]bool)
	runs := entry.data.Runs[:0]
	for _, run := range entry.data.Runs {
		if run == nil {
			continue
		}
		if isActive(run.State) {
			active[run.ID] = true
			runs = append(runs, run)
		} else if run.UpdatedAt.After(cutoff) {
			runs = append(runs, run)
		}
	}
	clear(entry.data.Runs[len(runs):])
	entry.data.Runs = runs
	events := entry.data.Events[:0]
	for _, event := range entry.data.Events {
		if event != nil && (!event.Timestamp.Before(cutoff) || active[event.RunID]) {
			events = append(events, event)
		}
	}
	clear(entry.data.Events[len(events):])
	entry.data.Events = events
	entry.indexRuns()
	entry.lastPrune = time.Now()
}

func (s *RunStore) Replay(sessionID, lastMessageID string) []*Message {
	events, _ := s.ReplayWithError(sessionID, lastMessageID)
	return events
}

// ReplayWithError is Replay with explicit durable-stream recovery errors.
func (s *RunStore) ReplayWithError(sessionID, lastMessageID string) ([]*Message, error) {
	entry := s.session(sessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return nil, err
	}
	s.pruneDueLocked(entry)
	start, _ := replayStart(entry.data.Events, lastMessageID)
	return cloneEvents(entry.data.Events[start:]), nil
}

func replayStart(events []*Message, lastMessageID string) (int, bool) {
	if lastMessageID != "" {
		for i, event := range events {
			if event != nil && (event.ID == lastMessageID || event.EventID == lastMessageID) {
				return i + 1, true
			}
		}
	}
	return 0, false
}

func (s *RunStore) ReplayForReconnect(sessionID, lastMessageID string) []*Message {
	events, _ := s.ReplayForReconnectWithError(sessionID, lastMessageID)
	return events
}

// ReplayForReconnectWithError exposes recovery failures to callers that must not
// report a successful reconnect after a corrupt or unreadable durable stream.
func (s *RunStore) ReplayForReconnectWithError(sessionID, lastMessageID string) ([]*Message, error) {
	entry := s.session(sessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if err := s.loadLocked(entry); err != nil {
		return nil, err
	}
	s.pruneDueLocked(entry)
	if start, found := replayStart(entry.data.Events, lastMessageID); found {
		return cloneEvents(entry.data.Events[start:]), nil
	}
	return cloneEventsForRuns(entry.data.Events, activeRunIDs(&entry.data)), nil
}

func activeRunIDs(session *persistedSession) map[string]bool {
	active := make(map[string]bool)
	for _, run := range session.Runs {
		if run != nil && isActive(run.State) {
			active[run.ID] = true
		}
	}
	return active
}

func cloneMessage(event *Message) *Message {
	copyMsg := *event
	copyMsg.Data = append(json.RawMessage(nil), event.Data...)
	return &copyMsg
}

func cloneEvents(events []*Message) []*Message {
	result := make([]*Message, 0, len(events))
	for _, event := range events {
		if event != nil {
			result = append(result, cloneMessage(event))
		}
	}
	return result
}

func cloneEventsForRuns(events []*Message, runIDs map[string]bool) []*Message {
	result := make([]*Message, 0, len(events))
	for _, event := range events {
		if event != nil && runIDs[event.RunID] {
			result = append(result, cloneMessage(event))
		}
	}
	return result
}

func (s *RunStore) ActiveRuns(sessionID string) []RunRecord {
	entry := s.session(sessionID)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if s.loadLocked(entry) != nil {
		return nil
	}
	s.pruneDueLocked(entry)
	var result []RunRecord
	for _, run := range entry.data.Runs {
		if run != nil && isActive(run.State) {
			result = append(result, cloneRun(*run))
		}
	}
	return result
}

func (s *RunStore) Restoreable() []RunRecord {
	for _, sessionID := range s.discoverSessions() {
		s.session(sessionID)
	}
	s.mu.Lock()
	entries := make([]*runSession, 0, len(s.sessions))
	for _, entry := range s.sessions {
		entries = append(entries, entry)
	}
	s.mu.Unlock()
	var result []RunRecord
	for _, entry := range entries {
		entry.mu.Lock()
		if s.loadLocked(entry) == nil {
			s.pruneDueLocked(entry)
			for _, run := range entry.data.Runs {
				if run != nil && isActive(run.State) {
					result = append(result, cloneRun(*run))
				}
			}
		}
		entry.mu.Unlock()
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func stableSessionHash(sessionID string) uint64 {
	var hash uint64 = 1469598103934665603
	for i := 0; i < len(sessionID); i++ {
		hash ^= uint64(sessionID[i])
		hash *= 1099511628211
	}
	return hash
}
