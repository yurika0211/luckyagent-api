package resources

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yurika0211/luckyagent-tools/cron"
)

type cronExecutionResponse struct {
	Execution cron.Execution `json:"execution"`
	Task      cron.Task      `json:"task"`
	Delivery  cron.Delivery  `json:"delivery"`
}

// HandleCronResults exposes the account-owned scheduled-result contract. FCM
// is an optional transport; polling this endpoint is the required fallback.
// The handler never creates an Execution or changes a result while rendering.
func (s *Controller) HandleCronResults(w http.ResponseWriter, r *http.Request) {
	backend, ok := s.agent.(CronResultBackend)
	if !ok || backend.CronResults() == nil {
		s.sendError(w, "cron result store unavailable", http.StatusServiceUnavailable, "")
		return
	}
	store := backend.CronResults()
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/cron/results/"), "/")
	parts := strings.Split(path, "/")
	if path == "tasks" && r.Method == http.MethodGet {
		s.handleCronResultTasks(w, r, store)
		return
	}
	if len(parts) == 3 && parts[0] == "tasks" && parts[2] == "executions" && r.Method == http.MethodGet {
		task, ok := store.GetTask(parts[1])
		if !ok || task.Status == cron.TaskDeleted {
			s.sendError(w, "cron task not found", http.StatusNotFound, "")
			return
		}
		s.sendJSON(w, http.StatusOK, map[string]any{"task": task, "executions": store.ListExecutions(parts[1])})
		return
	}
	if path == "devices" && r.Method == http.MethodPost {
		var device cron.Device
		if err := json.NewDecoder(r.Body).Decode(&device); err != nil {
			s.sendError(w, "invalid device body", http.StatusBadRequest, err.Error())
			return
		}
		registered, err := store.RegisterDevice(device)
		if err != nil {
			s.sendError(w, "device registration failed", http.StatusBadRequest, err.Error())
			return
		}
		s.sendJSON(w, http.StatusOK, registered)
		return
	}
	if path == "pending" && r.Method == http.MethodGet {
		deviceID := strings.TrimSpace(r.URL.Query().Get("device_id"))
		limit := 1
		if value, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && value > 0 && value <= 50 {
			limit = value
		}
		items, err := store.PendingForDevice(deviceID, limit)
		if err != nil {
			s.sendError(w, "cron result query failed", http.StatusBadRequest, err.Error())
			return
		}
		responses := make([]cronExecutionResponse, 0, len(items))
		for _, delivery := range items {
			execution, ok := store.GetExecution(delivery.ExecutionID)
			if !ok {
				continue
			}
			task, ok := store.GetTask(execution.TaskID)
			if !ok || task.Status == cron.TaskDeleted {
				continue
			}
			responses = append(responses, cronExecutionResponse{Execution: execution, Task: task, Delivery: delivery})
		}
		s.sendJSON(w, http.StatusOK, map[string]any{"device_id": deviceID, "items": responses})
		return
	}
	if len(parts) == 3 && parts[0] == "deliveries" && parts[2] == "ack" && r.Method == http.MethodPost {
		var req struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.sendError(w, "invalid ack body", http.StatusBadRequest, err.Error())
			return
		}
		delivery, err := store.Ack(parts[1], req.DeviceID, time.Now().UTC())
		if err != nil {
			code := http.StatusBadRequest
			if errors.Is(err, cron.ErrResultNotFound) {
				code = http.StatusNotFound
			}
			s.sendError(w, "cron delivery ack failed", code, err.Error())
			return
		}
		s.sendJSON(w, http.StatusOK, delivery)
		return
	}
	s.sendError(w, "cron result route not found", http.StatusNotFound, "")
}

func (s *Controller) handleCronResultTasks(w http.ResponseWriter, r *http.Request, store *cron.ResultStore) {
	includeDeleted := r.URL.Query().Get("include_deleted") == "1"
	s.sendJSON(w, http.StatusOK, map[string]any{"tasks": store.ListTasks(includeDeleted)})
}
