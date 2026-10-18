package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/yurika0211/luckyagent-orchestrator/collab"
)

// HandleAgentsList 列出所有注册的 Agent
func (s *Controller) HandleAgentsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	agents := s.collabRegistry.List()

	resp := struct {
		Agents []collab.AgentProfile `json:"agents"`
		Count  int                   `json:"count"`
		Online int                   `json:"online"`
	}{}

	for _, a := range agents {
		resp.Agents = append(resp.Agents, *a)
		resp.Count++
		if a.Status == collab.StatusOnline {
			resp.Online++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HandleAgentsGet 获取单个 Agent 信息
func (s *Controller) HandleAgentsGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	agentID := r.URL.Query().Get("id")
	if agentID == "" {
		http.Error(w, "missing agent id", http.StatusBadRequest)
		return
	}

	profile, ok := s.collabRegistry.Get(agentID)
	if !ok {
		http.Error(w, "agent not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(profile)
}

// HandleAgentsRegister 注册新 Agent
func (s *Controller) HandleAgentsRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID           string            `json:"id"`
		Name         string            `json:"name"`
		Description  string            `json:"description"`
		Capabilities []string          `json:"capabilities"`
		Metadata     map[string]string `json:"metadata"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.ID == "" {
		http.Error(w, "agent id is required", http.StatusBadRequest)
		return
	}

	profile := &collab.AgentProfile{
		ID:           req.ID,
		Name:         req.Name,
		Description:  req.Description,
		Capabilities: req.Capabilities,
		Status:       collab.StatusOnline,
		Metadata:     req.Metadata,
	}

	if err := s.collabRegistry.Register(profile); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "registered",
		"agent_id": req.ID,
	})
}

// HandleAgentsDeregister 注销 Agent
func (s *Controller) HandleAgentsDeregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	agentID := r.URL.Query().Get("id")
	if agentID == "" {
		http.Error(w, "missing agent id", http.StatusBadRequest)
		return
	}

	if err := s.collabRegistry.Deregister(agentID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "deregistered",
		"agent_id": agentID,
	})
}

// HandleAgentsDelegate 创建协作任务
func (s *Controller) HandleAgentsDelegate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Mode            collab.CollabMode `json:"mode"`
		Description     string            `json:"description"`
		Input           string            `json:"input"`
		AgentIDs        []string          `json:"agent_ids"`
		Timeout         time.Duration     `json:"timeout"`
		CostBudget      float64           `json:"cost_budget"`
		ParentSessionID string            `json:"parent_session_id,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if len(req.AgentIDs) == 0 {
		http.Error(w, "at least one agent_id is required", http.StatusBadRequest)
		return
	}

	if req.Timeout == 0 {
		req.Timeout = 60 * time.Second
	}
	if req.CostBudget < 0 || req.CostBudget > 1 {
		http.Error(w, "cost_budget must be between 0 and 1", http.StatusBadRequest)
		return
	}

	// The collaboration goroutines outlive this HTTP request. Preserve request
	// values (including tracing/auth context) without inheriting net/http's
	// cancellation when the handler writes the 202 response.
	backgroundCtx := context.WithoutCancel(r.Context())
	task, err := s.delegateManager.DelegateWithSession(backgroundCtx, req.Mode, req.Description, req.Input, req.AgentIDs, req.Timeout, req.CostBudget, req.ParentSessionID)
	if err != nil {
		if errors.Is(err, collab.ErrInvalidMode) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The orchestrator has its own durable session too. Sub-task execution
	// creates its sessions in the task handler; provisioning this one before
	// returning makes the top-level execution_session_id immediately usable by
	// clients that open the task from the 202 response.
	if s.ensureOrchestrator != nil && task != nil && task.ExecutionSessionID != "" {
		s.ensureOrchestrator(task.ExecutionSessionID)
	}

	// 使用 GetTask 获取深拷贝，避免 data race（后台 goroutine 正在修改 task）
	taskCopy, _ := s.delegateManager.GetTask(task.ID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(taskCopy)
}

// HandleAgentsTask 获取任务状态
func (s *Controller) HandleAgentsTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		http.Error(w, "missing task id", http.StatusBadRequest)
		return
	}

	task, ok := s.delegateManager.GetTask(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(task)
}

// HandleAgentsTasks 列出所有任务
func (s *Controller) HandleAgentsTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasks := s.delegateManager.ListTasks()

	resp := struct {
		Tasks []*collab.CollabTask `json:"tasks"`
		Count int                  `json:"count"`
	}{
		Tasks: tasks,
		Count: len(tasks),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HandleAgentsCancel 取消任务
func (s *Controller) HandleAgentsCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		http.Error(w, "missing task id", http.StatusBadRequest)
		return
	}

	if err := s.delegateManager.CancelTask(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "cancelled",
		"task_id": taskID,
	})
}

// HandleAgentsBlackboard exposes the shared task notes used by collaborative
// workers. GET is read-only for clients; POST/PUT lets an operator or agent
// publish a small value that other workers can consume.
func (s *Controller) HandleAgentsBlackboard(w http.ResponseWriter, r *http.Request) {
	board := (*collab.Blackboard)(nil)
	if s != nil && s.delegateManager != nil {
		board = s.delegateManager.Blackboard()
	}
	if board == nil {
		http.Error(w, "blackboard unavailable", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		taskID := r.URL.Query().Get("task_id")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			TaskID string                   `json:"task_id,omitempty"`
			Items  []collab.BlackboardEntry `json:"items"`
		}{TaskID: taskID, Items: board.List(taskID)})
	case http.MethodPost, http.MethodPut:
		var req struct {
			TaskID string `json:"task_id"`
			Key    string `json:"key"`
			Value  string `json:"value"`
			Author string `json:"author"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		entry, err := board.Set(req.TaskID, req.Key, req.Value, req.Author)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(entry)
	case http.MethodDelete:
		if !board.Delete(r.URL.Query().Get("task_id"), r.URL.Query().Get("key")) {
			http.Error(w, "blackboard entry not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleAgentsMessages provides a small A2A mailbox for agents that are not
// sharing the same in-process delegate goroutine. Messages are ordered by
// timestamp and can be filtered by sender or recipient.
func (s *Controller) HandleAgentsMessages(w http.ResponseWriter, r *http.Request) {
	bus := (*collab.MessageBus)(nil)
	if s != nil && s.delegateManager != nil {
		bus = s.delegateManager.MessageBus()
	}
	if bus == nil {
		http.Error(w, "message bus unavailable", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		limit := 50
		if raw := r.URL.Query().Get("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			}
		}
		messages := bus.List(collab.MessageFilter{From: r.URL.Query().Get("from"), To: r.URL.Query().Get("to"), TaskID: r.URL.Query().Get("task_id"), Limit: limit})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Messages []*collab.Message `json:"messages"`
			Count    int               `json:"count"`
		}{Messages: messages, Count: len(messages)})
	case http.MethodPost:
		var message collab.Message
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if message.ID == "" {
			message.ID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
		}
		if message.Timestamp.IsZero() {
			message.Timestamp = time.Now()
		}
		if err := bus.Publish(&message); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(message)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
