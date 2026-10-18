package websocket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type runRecoveryMetadata struct {
	sessionID string
	sequence  uint64
	runs      []*RunRecord
}

// scanRunSnapshot reads metadata up to the event field in the canonical format.
// Older field orders are streamed past; only active run records are retained.
func scanRunSnapshot(path string) (runRecoveryMetadata, error) {
	var metadata runRecoveryMetadata
	file, err := os.Open(path)
	if err != nil {
		return metadata, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if token, err := decoder.Token(); err != nil {
		return metadata, err
	} else if token != json.Delim('{') {
		return metadata, errors.New("expected websocket snapshot object")
	}
	haveRuns, haveSequence, haveVersion := false, false, false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return metadata, err
		}
		switch key {
		case "session_id":
			err = decoder.Decode(&metadata.sessionID)
		case "last_sequence":
			err = decoder.Decode(&metadata.sequence)
			haveSequence = err == nil
		case "version":
			haveVersion = true
			var version int
			err = decoder.Decode(&version)
			if err == nil && version > 1 {
				err = fmt.Errorf("unsupported websocket snapshot version %d", version)
			}
		case "runs":
			metadata.runs, err = scanRecoveryRunArray(decoder)
			haveRuns = true
		case "events":
			if metadata.sessionID != "" && haveRuns && haveSequence {
				return metadata, nil
			}
			err = skipJSONValue(decoder)
			// Legacy metadata-only records have no format version/sequence.
			// Keep main's tolerance of their unread event tail. Versioned
			// files must instead find a sequence after a reordered events field.
			if err != nil && !haveVersion && metadata.sessionID != "" && haveRuns {
				return metadata, nil
			}
		default:
			err = skipJSONValue(decoder)
		}
		if err != nil {
			return metadata, err
		}
	}
	_, err = decoder.Token()
	return metadata, err
}

func scanRecoveryRunArray(decoder *json.Decoder) ([]*RunRecord, error) {
	token, err := decoder.Token()
	if err != nil || token == nil {
		return nil, err
	}
	if token != json.Delim('[') {
		return nil, errors.New("expected websocket runs array")
	}
	var active []*RunRecord
	for decoder.More() {
		var run *RunRecord
		if err := decoder.Decode(&run); err != nil {
			return nil, err
		}
		if run != nil && isActive(run.State) {
			active = append(active, run)
		}
	}
	_, err = decoder.Token()
	return active, err
}

func (s *RunStore) scanRestoreableDiskRuns(cached map[string]bool) []RunRecord {
	if s.root == "" {
		return nil
	}
	files, err := os.ReadDir(s.root)
	if err != nil {
		return nil
	}
	type recoveryFiles struct{ snapshot, journal string }
	groups := make(map[string]*recoveryFiles)
	excluded := make(map[string]bool, len(cached))
	for sessionID := range cached {
		excluded[fmt.Sprintf("%x", stableSessionHash(sessionID))] = true
	}
	for _, file := range files {
		name := file.Name()
		if file.IsDir() || strings.HasPrefix(name, ".") || strings.Contains(name, ".tmp.") {
			continue
		}
		extension := filepath.Ext(name)
		if extension != ".json" && extension != ".jsonl" {
			continue
		}
		base := strings.TrimSuffix(name, extension)
		if excluded[base] {
			continue
		}
		group := groups[base]
		if group == nil {
			group = &recoveryFiles{}
			groups[base] = group
		}
		path := filepath.Join(s.root, name)
		if extension == ".json" {
			group.snapshot = path
		} else {
			group.journal = path
		}
	}
	bases := make([]string, 0, len(groups))
	for base := range groups {
		bases = append(bases, base)
	}
	sort.Strings(bases)
	seen := make(map[string]bool, len(cached))
	for sessionID := range cached {
		seen[sessionID] = true
	}
	var result []RunRecord
	for _, base := range bases {
		group := groups[base]
		var metadata runRecoveryMetadata
		if group.snapshot != "" {
			metadata, err = scanRunSnapshot(group.snapshot)
		} else {
			err = nil
		}
		if err != nil {
			slog.Warn("websocket recovery metadata scan failed", "file", base, "error", err)
			continue
		}
		if seen[metadata.sessionID] {
			continue
		}
		if group.journal != "" {
			metadata, err = scanRecoveryJournal(group.journal, metadata)
		}
		if err != nil {
			// Restoreable has no error return. Skip the entire session rather
			// than resume stale snapshot state after a corrupt journal update.
			slog.Warn("websocket recovery metadata scan failed", "file", base, "error", err)
			continue
		}
		if metadata.sessionID == "" || seen[metadata.sessionID] {
			continue
		}
		seen[metadata.sessionID] = true
		result = append(result, restoreableRunRecords(metadata.sessionID, metadata.runs)...)
	}
	return result
}

type journalRecoveryRecord struct {
	version   int
	sessionID string
	sequence  uint64
	run       *RunRecord
	hasEvent  bool
}

// scanRecoveryJournal never decodes an event into Message or retains its Data.
// A decoder is reset at each commit newline, avoiding a whole JSONL record
// allocation and distinguishing a complete JSON object from a committed row.
func scanRecoveryJournal(path string, metadata runRecoveryMetadata) (runRecoveryMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return metadata, err
	}
	defer file.Close()
	active := make(map[string]*RunRecord, len(metadata.runs))
	order := make([]string, 0, len(metadata.runs))
	for _, run := range metadata.runs {
		active[run.ID] = run
		order = append(order, run.ID)
	}
	var offset int64
	for {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return metadata, err
		}
		decoder := json.NewDecoder(file)
		record, readErr := scanJournalRecoveryRecord(decoder)
		if errors.Is(readErr, io.EOF) && decoder.InputOffset() == 0 {
			break
		}
		if readErr != nil {
			if journalTailOnly(file, offset) {
				break
			}
			return metadata, fmt.Errorf("invalid websocket journal metadata at byte %d: %w", offset, readErr)
		}
		nextOffset, err := recoveryRecordEnd(file, offset+decoder.InputOffset())
		if errors.Is(err, io.EOF) {
			break
		} // final row lacks its commit newline
		if err != nil {
			if journalTailOnly(file, offset) {
				break
			}
			return metadata, err
		}
		if metadata.sessionID == "" {
			metadata.sessionID = record.sessionID
		}
		if record.version != 1 || record.sessionID == "" || record.sessionID != metadata.sessionID || record.sequence == 0 || (record.run != nil) == record.hasEvent {
			return metadata, fmt.Errorf("invalid websocket journal metadata at byte %d", offset)
		}
		if record.run != nil && record.run.SessionID != record.sessionID {
			return metadata, errors.New("websocket journal run session mismatch")
		}
		if record.sequence > metadata.sequence {
			if record.sequence != metadata.sequence+1 {
				return metadata, errors.New("websocket journal sequence gap")
			}
			if record.run != nil {
				if isActive(record.run.State) {
					if active[record.run.ID] == nil {
						order = append(order, record.run.ID)
					}
					active[record.run.ID] = record.run
				} else {
					delete(active, record.run.ID)
				}
			}
			metadata.sequence = record.sequence
		}
		offset = nextOffset
	}
	metadata.runs = nil
	for _, id := range order {
		if run := active[id]; run != nil {
			metadata.runs = append(metadata.runs, run)
			delete(active, id)
		}
	}
	return metadata, nil
}

func scanJournalRecoveryRecord(decoder *json.Decoder) (journalRecoveryRecord, error) {
	var record journalRecoveryRecord
	if token, err := decoder.Token(); err != nil {
		return record, err
	} else if token != json.Delim('{') {
		return record, errors.New("expected websocket journal object")
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return record, err
		}
		switch key {
		case "version":
			err = decoder.Decode(&record.version)
		case "session_id":
			err = decoder.Decode(&record.sessionID)
		case "sequence":
			err = decoder.Decode(&record.sequence)
		case "run":
			err = decoder.Decode(&record.run)
		case "event":
			record.hasEvent = true
			err = skipJSONValue(decoder)
		default:
			err = skipJSONValue(decoder)
		}
		if err != nil {
			return record, err
		}
	}
	_, err := decoder.Token()
	return record, err
}

func recoveryRecordEnd(file *os.File, offset int64) (int64, error) {
	var one [1]byte
	for {
		if _, err := file.ReadAt(one[:], offset); err != nil {
			return offset, err
		}
		offset++
		if one[0] == '\n' {
			return offset, nil
		}
		if one[0] != ' ' && one[0] != '\t' && one[0] != '\r' {
			return offset, errors.New("websocket journal row lacks commit newline")
		}
	}
}

// A malformed final line is left untouched for the lazy full loader to repair.
// Any non-whitespace following that line makes corruption an interior failure.
func journalTailOnly(file *os.File, offset int64) bool {
	reader := io.NewSectionReader(file, offset, 1<<63-1-offset)
	var buffer [4096]byte
	haveNewline := false
	for {
		count, err := reader.Read(buffer[:])
		chunk := buffer[:count]
		if !haveNewline {
			if newline := bytes.IndexByte(chunk, '\n'); newline >= 0 {
				haveNewline = true
				chunk = chunk[newline+1:]
			} else {
				chunk = nil
			}
		}
		if haveNewline && len(bytes.TrimSpace(chunk)) != 0 {
			return false
		}
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return false
		}
	}
}
