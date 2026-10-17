package resources

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	taskstore "github.com/yurika0211/luckyagent-tools/task"
)

type taskFeedbackRequest struct {
	Status          taskstore.Status       `json:"status"`
	Verified        bool                   `json:"verified"`
	Verifier        string                 `json:"verifier"`
	UserFeedback    string                 `json:"user_feedback"`
	Score           float64                `json:"score"`
	Cost            taskstore.CostSnapshot `json:"cost"`
	RecommendedNext string                 `json:"recommended_next"`
}

type taskCancelRequest struct {
	Reason string `json:"reason"`
}

type TaskTreeNode struct {
	Task        taskstore.Record               `json:"task"`
	Observation taskstore.MainAgentObservation `json:"observation"`
	Children    []TaskTreeNode                 `json:"children,omitempty"`
	Result      string                         `json:"result,omitempty"`
}

func (s *Controller) HandleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	store := s.agent.TaskStore()
	if store == nil {
		s.sendError(w, "task store not initialized", http.StatusServiceUnavailable, "")
		return
	}
	filter := taskstore.ListFilter{
		Source:   taskstore.Source(strings.TrimSpace(r.URL.Query().Get("source"))),
		Status:   taskstore.Status(strings.TrimSpace(r.URL.Query().Get("status"))),
		ParentID: strings.TrimSpace(r.URL.Query().Get("parent_id")),
		Limit:    boundedTasksLimit(r.URL.Query().Get("limit")),
	}
	records, err := store.List(filter)
	if err != nil {
		s.sendError(w, "list tasks failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.sendJSON(w, http.StatusOK, map[string]any{
		"tasks": records,
		"count": len(records),
	})
}

func (s *Controller) HandleTaskByID(w http.ResponseWriter, r *http.Request) {
	store := s.agent.TaskStore()
	if store == nil {
		s.sendError(w, "task store not initialized", http.StatusServiceUnavailable, "")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/"), "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		s.sendError(w, "task id is required", http.StatusBadRequest, "")
		return
	}
	taskID := strings.TrimSpace(parts[0])
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		record, ok, err := store.Get(taskID)
		if err != nil {
			s.sendError(w, "get task failed", http.StatusInternalServerError, err.Error())
			return
		}
		if !ok {
			s.sendError(w, "task not found", http.StatusNotFound, taskID)
			return
		}
		s.sendJSON(w, http.StatusOK, record)
		return
	}

	switch parts[1] {
	case "tree":
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		s.HandleTaskTree(w, store, taskID)
	case "events":
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		events, err := store.Events(taskID)
		if err != nil {
			s.sendError(w, "get task events failed", http.StatusInternalServerError, err.Error())
			return
		}
		s.sendJSON(w, http.StatusOK, map[string]any{
			"task_id": taskID,
			"events":  events,
			"count":   len(events),
		})
	case "trace":
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		trace, ok, err := store.PlannerTrace(taskID)
		if err != nil {
			s.sendError(w, "get task trace failed", http.StatusInternalServerError, err.Error())
			return
		}
		if !ok {
			s.sendError(w, "task trace not found", http.StatusNotFound, taskID)
			return
		}
		var payload any
		if err := json.Unmarshal(trace, &payload); err != nil {
			s.sendError(w, "task trace is invalid", http.StatusInternalServerError, err.Error())
			return
		}
		s.sendJSON(w, http.StatusOK, payload)
	case "result":
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		result, ok, err := store.Result(taskID)
		if err != nil {
			s.sendError(w, "get task result failed", http.StatusInternalServerError, err.Error())
			return
		}
		if !ok {
			s.sendError(w, "task result not found", http.StatusNotFound, taskID)
			return
		}
		s.sendJSON(w, http.StatusOK, map[string]any{
			"task_id": taskID,
			"result":  result,
		})
	case "observation":
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		record, ok, err := store.Get(taskID)
		if err != nil {
			s.sendError(w, "get task failed", http.StatusInternalServerError, err.Error())
			return
		}
		if !ok {
			s.sendError(w, "task not found", http.StatusNotFound, taskID)
			return
		}
		events, err := store.Events(taskID)
		if err != nil {
			s.sendError(w, "get task events failed", http.StatusInternalServerError, err.Error())
			return
		}
		s.sendJSON(w, http.StatusOK, taskstore.ReduceObservation(record, events))
	case "feedback":
		if r.Method != http.MethodPost {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		s.HandleTaskFeedback(w, r, store, taskID)
	case "cancel":
		if r.Method != http.MethodPost {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		s.HandleTaskCancel(w, r, store, taskID)
	default:
		s.sendError(w, "not found", http.StatusNotFound, "")
	}
}

// HandleTaskTree returns a task and all persisted descendants as a stable tree
// suitable for a dashboard, TUI, or gateway progress card.
func (s *Controller) HandleTaskTree(w http.ResponseWriter, store taskstore.Store, taskID string) {
	root, ok, err := store.Get(taskID)
	if err != nil {
		s.sendError(w, "get task failed", http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		s.sendError(w, "task not found", http.StatusNotFound, taskID)
		return
	}
	// A tree is scoped by its root, not by the global recent-task list. Older
	// descendants must remain visible even when newer unrelated tasks exist.
	records, err := store.List(taskstore.ListFilter{})
	if err != nil {
		s.sendError(w, "list task tree failed", http.StatusInternalServerError, err.Error())
		return
	}
	childrenByParent := make(map[string][]taskstore.Record)
	for _, record := range records {
		if strings.TrimSpace(record.ParentID) != "" {
			childrenByParent[record.ParentID] = append(childrenByParent[record.ParentID], record)
		}
	}
	for parentID := range childrenByParent {
		sort.SliceStable(childrenByParent[parentID], func(i, j int) bool {
			left, right := childrenByParent[parentID][i], childrenByParent[parentID][j]
			if left.CreatedAt.Equal(right.CreatedAt) {
				return left.ID < right.ID
			}
			return left.CreatedAt.Before(right.CreatedAt)
		})
	}
	visiting := make(map[string]bool)
	node, err := buildTaskTreeNode(store, root, childrenByParent, visiting)
	if err != nil {
		s.sendError(w, "build task tree failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.sendJSON(w, http.StatusOK, node)
}

func buildTaskTreeNode(store taskstore.Store, record taskstore.Record, childrenByParent map[string][]taskstore.Record, visiting map[string]bool) (TaskTreeNode, error) {
	if visiting[record.ID] {
		return TaskTreeNode{}, fmt.Errorf("task hierarchy contains a cycle at %s", record.ID)
	}
	visiting[record.ID] = true
	defer delete(visiting, record.ID)

	events, err := store.Events(record.ID)
	if err != nil {
		return TaskTreeNode{}, err
	}
	node := TaskTreeNode{
		Task:        record,
		Observation: taskstore.ReduceObservation(record, events),
	}
	if result, _, err := store.Result(record.ID); err != nil {
		return TaskTreeNode{}, err
	} else {
		node.Result = result
	}
	for _, child := range childrenByParent[record.ID] {
		childNode, err := buildTaskTreeNode(store, child, childrenByParent, visiting)
		if err != nil {
			return TaskTreeNode{}, err
		}
		node.Children = append(node.Children, childNode)
	}
	return node, nil
}

func (s *Controller) HandleTaskFeedback(w http.ResponseWriter, r *http.Request, store taskstore.Store, taskID string) {
	var req taskFeedbackRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			s.sendError(w, "invalid feedback body", http.StatusBadRequest, err.Error())
			return
		}
	}
	record, ok, err := store.Get(taskID)
	if err != nil {
		s.sendError(w, "get task failed", http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		s.sendError(w, "task not found", http.StatusNotFound, taskID)
		return
	}
	record.Outcome = taskstore.Outcome{
		Status:          firstStatus(req.Status, record.Status),
		Verified:        req.Verified,
		Verifier:        strings.TrimSpace(req.Verifier),
		UserFeedback:    strings.TrimSpace(req.UserFeedback),
		Score:           req.Score,
		Cost:            req.Cost,
		RecommendedNext: strings.TrimSpace(req.RecommendedNext),
	}
	if err := store.Update(record); err != nil {
		s.sendError(w, "record task feedback failed", http.StatusInternalServerError, err.Error())
		return
	}
	if err := store.AppendEvent(taskstore.Event{
		Type:               taskstore.EventOutcomeRecorded,
		TaskID:             record.ID,
		ExecutionSessionID: record.ExecutionSessionID,
		ParentSessionID:    record.ParentSessionID,
		Role:               record.Role,
		Time:               time.Now(),
		Status:             record.Outcome.Status,
		Message:            record.Outcome.UserFeedback,
		Cost:               record.Outcome.Cost,
		Metadata: map[string]string{
			"verified":         fmt.Sprintf("%t", record.Outcome.Verified),
			"verifier":         record.Outcome.Verifier,
			"score":            fmt.Sprintf("%.4f", record.Outcome.Score),
			"recommended_next": record.Outcome.RecommendedNext,
		},
	}); err != nil {
		s.sendError(w, "append task feedback event failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.sendJSON(w, http.StatusOK, record)
}

func (s *Controller) HandleTaskCancel(w http.ResponseWriter, r *http.Request, store taskstore.Store, taskID string) {
	var req taskCancelRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			s.sendError(w, "invalid cancel body", http.StatusBadRequest, err.Error())
			return
		}
	}
	record, ok, err := store.Get(taskID)
	if err != nil {
		s.sendError(w, "get task failed", http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		s.sendError(w, "task not found", http.StatusNotFound, taskID)
		return
	}
	switch record.Status {
	case taskstore.StatusCompleted, taskstore.StatusFailed, taskstore.StatusCancelled:
		s.sendError(w, "task cannot be cancelled from current status", http.StatusConflict, string(record.Status))
		return
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "cancelled by user"
	}
	if s.delegateManager != nil && record.Source == taskstore.SourceHTTP {
		_ = s.delegateManager.CancelTask(record.ID)
	}
	if record.Source == taskstore.SourceTool && s.agent != nil && s.agent.Delegate() != nil {
		cancelErr := s.agent.Delegate().CancelTask(record.ID, reason)
		if cancelErr != nil {
			// A task restored from disk may no longer have an in-memory
			// goroutine. Keep the durable API useful by marking that record below;
			// only reject errors for a task that is still resident but terminal.
			if !strings.Contains(cancelErr.Error(), "task not found") {
				s.sendError(w, "cancel delegated task failed", http.StatusConflict, cancelErr.Error())
				return
			}
		} else {
			// The delegate manager writes the terminal record and event itself.
			if updated, found, getErr := store.Get(taskID); getErr == nil && found {
				s.sendJSON(w, http.StatusOK, updated)
				return
			}
		}
	}
	record.Status = taskstore.StatusCancelled
	now := time.Now()
	record.CompletedAt = now
	record.Outcome.Status = taskstore.StatusCancelled
	record.Outcome.UserFeedback = reason
	if err := store.Update(record); err != nil {
		s.sendError(w, "cancel task failed", http.StatusInternalServerError, err.Error())
		return
	}
	if err := store.AppendEvent(taskstore.Event{
		Type:               taskstore.EventCancelled,
		TaskID:             record.ID,
		ExecutionSessionID: record.ExecutionSessionID,
		ParentSessionID:    record.ParentSessionID,
		Role:               record.Role,
		Mode:               record.Mode,
		Time:               now,
		Status:             taskstore.StatusCancelled,
		Message:            reason,
	}); err != nil {
		s.sendError(w, "append task cancellation event failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.sendJSON(w, http.StatusOK, record)
}

func firstStatus(primary, fallback taskstore.Status) taskstore.Status {
	if primary != "" {
		return primary
	}
	return fallback
}

func boundedTasksLimit(raw string) int {
	limit := 50
	if strings.TrimSpace(raw) != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			limit = n
		}
	}
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}
