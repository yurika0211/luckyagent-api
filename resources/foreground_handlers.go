package resources

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yurika0211/luckyagent-tools/autonomy"
)

// ForegroundBackend is optional so hosts that do not include the Agent core
// can keep using the shared resource controller.
type ForegroundBackend interface {
	ForegroundTasks() []*autonomy.QueueTask
}

type ForegroundTasksResponse struct {
	GeneratedAt time.Time                   `json:"generated_at"`
	Tasks       []ForegroundTaskSummaryView `json:"tasks"`
	Count       int                         `json:"count"`
}

type ForegroundTaskSummaryView struct {
	ID                  string             `json:"id"`
	State               autonomy.TaskState `json:"state"`
	SessionID           string             `json:"session_id,omitempty"`
	Source              string             `json:"source,omitempty"`
	Attempts            int                `json:"attempts"`
	BudgetStartAttempt  int                `json:"budget_start_attempt"`
	CurrentSlice        int                `json:"current_slice"`
	RemainingSlices     int                `json:"remaining_slices,omitempty"`
	Retries             int                `json:"retries"`
	Continuations       int                `json:"continuations"`
	RemainingSeconds    int                `json:"remaining_seconds,omitempty"`
	BlockReason         string             `json:"block_reason,omitempty"`
	LastFailureCategory string             `json:"last_failure_category,omitempty"`
	CheckpointPresent   bool               `json:"checkpoint_present"`
	FirstStartedAt      *time.Time         `json:"first_started_at,omitempty"`
	UpdatedAt           *time.Time         `json:"updated_at,omitempty"`
	StartedAt           *time.Time         `json:"started_at,omitempty"`
	CompletedAt         *time.Time         `json:"completed_at,omitempty"`
	NextRunAt           *time.Time         `json:"next_run_at,omitempty"`
	CheckpointAt        *time.Time         `json:"checkpoint_at,omitempty"`
}

func (s *Controller) HandleForegroundTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	backend, ok := s.agent.(ForegroundBackend)
	if !ok {
		s.sendJSON(w, http.StatusOK, ForegroundTasksResponse{GeneratedAt: time.Now(), Tasks: []ForegroundTaskSummaryView{}})
		return
	}
	limit, err := boundedForegroundLimit(r.URL.Query().Get("limit"))
	if err != nil {
		s.sendError(w, "invalid limit", http.StatusBadRequest, err.Error())
		return
	}
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	source := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source")))
	all := backend.ForegroundTasks()
	views := make([]ForegroundTaskSummaryView, 0, len(all))
	for _, task := range all {
		if task == nil {
			continue
		}
		if state != "" && string(task.State) != state {
			continue
		}
		view := foregroundTaskSummary(task)
		if source != "" && view.Source != source {
			continue
		}
		views = append(views, view)
		if len(views) == limit {
			break
		}
	}
	s.sendJSON(w, http.StatusOK, ForegroundTasksResponse{GeneratedAt: time.Now(), Tasks: views, Count: len(views)})
}

func foregroundTaskSummary(task *autonomy.QueueTask) ForegroundTaskSummaryView {
	current := task.Attempts - task.BudgetStartAttempt
	if current < 0 {
		current = 0
	}
	maxSlices := parseMetadataInt(task.Metadata, "foreground_max_slices")
	remainingSlices := maxSlices - current
	if remainingSlices < 0 {
		remainingSlices = 0
	}
	maxSeconds := parseMetadataInt(task.Metadata, "foreground_max_total_seconds")
	remainingSeconds := maxSeconds
	if maxSeconds > 0 && !task.FirstStartedAt.IsZero() {
		remainingSeconds = maxSeconds - int(time.Since(task.FirstStartedAt).Seconds())
		if remainingSeconds < 0 {
			remainingSeconds = 0
		}
	}
	return ForegroundTaskSummaryView{
		ID: task.ID, State: task.State, SessionID: task.SessionID,
		Source:   strings.ToLower(strings.TrimSpace(task.Metadata["foreground_source"])),
		Attempts: task.Attempts, BudgetStartAttempt: task.BudgetStartAttempt,
		CurrentSlice: current, RemainingSlices: remainingSlices,
		Retries: task.Retries, Continuations: task.Continuations,
		RemainingSeconds: remainingSeconds, BlockReason: task.BlockReason,
		LastFailureCategory: foregroundFailureCategory(task), CheckpointPresent: len(task.Checkpoint) > 0,
		FirstStartedAt: optionalTime(task.FirstStartedAt), UpdatedAt: optionalTime(task.UpdatedAt),
		StartedAt: optionalTime(task.StartedAt), CompletedAt: optionalTime(task.CompletedAt),
		NextRunAt: optionalTime(task.NextRunAt), CheckpointAt: optionalTime(task.CheckpointAt),
	}
}

func foregroundFailureCategory(task *autonomy.QueueTask) string {
	value := strings.ToLower(strings.TrimSpace(task.BlockReason))
	if value == "" {
		value = strings.ToLower(strings.TrimSpace(task.Error))
	}
	if value == "" {
		return ""
	}
	switch {
	case strings.Contains(value, "deadline"), strings.Contains(value, "timeout"), strings.Contains(value, "timed out"):
		return "timeout"
	case strings.Contains(value, "api error"), strings.Contains(value, "connection"), strings.Contains(value, "upstream"):
		return "upstream"
	case strings.Contains(value, "operation"), strings.Contains(value, "side effect"), strings.Contains(value, "核对"):
		return "operation"
	case strings.Contains(value, "empty"), strings.Contains(value, "no progress"), strings.Contains(value, "stall"):
		return "no_progress"
	default:
		return "blocked"
	}
}

func parseMetadataInt(values map[string]string, key string) int {
	value, err := strconv.Atoi(strings.TrimSpace(values[key]))
	if err != nil || value < 0 {
		return 0
	}
	return value
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}

func boundedForegroundLimit(raw string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return 200, nil
	}
	limit, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || limit <= 0 {
		return 0, strconv.ErrSyntax
	}
	if limit > 200 {
		limit = 200
	}
	return limit, nil
}
