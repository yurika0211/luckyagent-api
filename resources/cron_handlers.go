package resources

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	transport "github.com/yurika0211/luckyagent-api"
	"github.com/yurika0211/luckyagent-tools/cron"
)

type cronJobResponse struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Schedule       string            `json:"schedule"`
	Expression     string            `json:"expression,omitempty"`
	Status         string            `json:"status"`
	LastRun        *string           `json:"last_run,omitempty"`
	NextRun        *string           `json:"next_run,omitempty"`
	RunCount       int               `json:"run_count"`
	ErrorCount     int               `json:"error_count"`
	LastError      string            `json:"last_error,omitempty"`
	CreatedAt      string            `json:"created_at"`
	UpdatedAt      string            `json:"updated_at"`
	DeleteAfterRun bool              `json:"delete_after_run"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type cronCreateRequest struct {
	Name           string            `json:"name"`
	Description    string            `json:"description"`
	Schedule       string            `json:"schedule"`
	Mode           string            `json:"mode"`
	Command        string            `json:"command"`
	Metadata       map[string]string `json:"metadata"`
	DeleteAfterRun bool              `json:"delete_after_run"`
}

type cronPatchRequest struct {
	Action string `json:"action"`
}

func (s *Controller) HandleCron(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listCronJobs(w, r)
	case http.MethodPost:
		s.createCronJob(w, r)
	default:
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
	}
}

func (s *Controller) HandleCronByID(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/cron/"), "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		s.sendError(w, "job id is required", http.StatusBadRequest, "")
		return
	}
	if len(parts) != 1 {
		s.sendError(w, "cron job path not found", http.StatusNotFound, "")
		return
	}
	id := strings.TrimSpace(parts[0])
	switch r.Method {
	case http.MethodGet:
		s.getCronJob(w, id)
	case http.MethodPatch:
		s.patchCronJob(w, r, id)
	case http.MethodDelete:
		s.deleteCronJob(w, id)
	default:
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
	}
}

func (s *Controller) listCronJobs(w http.ResponseWriter, r *http.Request) {
	engine := s.cronEngine()
	if engine == nil {
		s.sendError(w, "cron engine unavailable", http.StatusServiceUnavailable, "")
		return
	}
	jobs := engine.ListJobs()
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].ID < jobs[j].ID
	})
	items := make([]cronJobResponse, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, cronJobJSON(job))
	}
	s.sendJSON(w, http.StatusOK, map[string]any{
		"running": engine.IsRunning(),
		"count":   len(items),
		"jobs":    items,
	})
}

func (s *Controller) createCronJob(w http.ResponseWriter, r *http.Request) {
	engine := s.cronEngine()
	if engine == nil {
		s.sendError(w, "cron engine unavailable", http.StatusServiceUnavailable, "")
		return
	}
	writer := s.cronWriter()
	if writer == nil {
		s.sendError(w, "cron writer unavailable", http.StatusServiceUnavailable, "")
		return
	}
	var req cronCreateRequest
	if err := transport.DecodeJSON(w, r, &req, transport.DefaultJSONBodyLimit); err != nil {
		transport.SendRequestError(w, err)
		return
	}
	scheduleText := strings.TrimSpace(req.Schedule)
	schedule, err := parseCronSchedule(scheduleText)
	if err != nil {
		s.sendError(w, "invalid schedule", http.StatusBadRequest, err.Error())
		return
	}
	mode, err := parseCronMode(req.Mode)
	if err != nil {
		s.sendError(w, "invalid cron mode", http.StatusBadRequest, err.Error())
		return
	}
	command := strings.TrimSpace(req.Command)
	if command == "" {
		s.sendError(w, "command is required", http.StatusBadRequest, "")
		return
	}
	sessionID := strings.TrimSpace(req.Metadata["session_id"])
	platform := strings.TrimSpace(req.Metadata["platform"])
	if sessionID == "" || platform == "" {
		s.sendError(w, "metadata.session_id and metadata.platform are required", http.StatusBadRequest, "")
		return
	}
	id := newCronJobID(mode, command, time.Now())
	meta := map[string]string{
		"session_id":    sessionID,
		"platform":      platform,
		"schedule_text": scheduleText,
	}
	for key, value := range req.Metadata {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" || key == "mode" || key == "command" {
			continue
		}
		meta[key] = value
	}
	if req.DeleteAfterRun {
		meta["delete_after_run"] = "true"
	}
	name := strings.TrimSpace(req.Name)
	description := strings.TrimSpace(req.Description)
	job, err := writer.AddCronJob(id, name, description, mode, command, meta, schedule)
	if err != nil {
		s.writeCronMutationError(w, err)
		return
	}
	if job == nil {
		stored, ok := engine.GetJob(id)
		if !ok {
			s.sendError(w, "created cron job is missing", http.StatusInternalServerError, id)
			return
		}
		job = stored
	}
	s.sendJSON(w, http.StatusCreated, cronJobJSON(job))
}

func (s *Controller) getCronJob(w http.ResponseWriter, id string) {
	engine := s.cronEngine()
	if engine == nil {
		s.sendError(w, "cron engine unavailable", http.StatusServiceUnavailable, "")
		return
	}
	job, ok := engine.GetJob(id)
	if !ok {
		s.sendError(w, "cron job not found", http.StatusNotFound, id)
		return
	}
	s.sendJSON(w, http.StatusOK, cronJobJSON(job))
}

func (s *Controller) patchCronJob(w http.ResponseWriter, r *http.Request, id string) {
	engine := s.cronEngine()
	if engine == nil {
		s.sendError(w, "cron engine unavailable", http.StatusServiceUnavailable, "")
		return
	}
	writer := s.cronWriter()
	if writer == nil {
		s.sendError(w, "cron writer unavailable", http.StatusServiceUnavailable, "")
		return
	}
	var req cronPatchRequest
	if err := transport.DecodeJSON(w, r, &req, 64<<10); err != nil {
		transport.SendRequestError(w, err)
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))
	var err error
	switch action {
	case "pause":
		err = writer.PauseCronJob(id)
	case "resume":
		err = writer.ResumeCronJob(id)
	case "":
		s.sendError(w, "action is required", http.StatusBadRequest, "")
		return
	default:
		s.sendError(w, "invalid cron action", http.StatusBadRequest, action)
		return
	}
	if err != nil {
		s.writeCronMutationError(w, err)
		return
	}
	job, ok := engine.GetJob(id)
	if !ok {
		s.sendError(w, "cron job not found", http.StatusNotFound, id)
		return
	}
	s.sendJSON(w, http.StatusOK, cronJobJSON(job))
}

func (s *Controller) deleteCronJob(w http.ResponseWriter, id string) {
	if s.cronEngine() == nil {
		s.sendError(w, "cron engine unavailable", http.StatusServiceUnavailable, "")
		return
	}
	writer := s.cronWriter()
	if writer == nil {
		s.sendError(w, "cron writer unavailable", http.StatusServiceUnavailable, "")
		return
	}
	if err := writer.RemoveCronJob(id); err != nil {
		s.writeCronMutationError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Controller) cronEngine() *cron.Engine {
	if s == nil || s.agent == nil {
		return nil
	}
	return s.agent.CronEngine()
}

func (s *Controller) cronWriter() CronJobWriter {
	if s == nil || s.agent == nil {
		return nil
	}
	writer, _ := s.agent.(CronJobWriter)
	return writer
}

func (s *Controller) writeCronMutationError(w http.ResponseWriter, err error) {
	message := err.Error()
	switch {
	case cronJobMissing(err):
		s.sendError(w, "cron job not found", http.StatusNotFound, message)
	case strings.Contains(message, "already exists"):
		s.sendError(w, "cron job already exists", http.StatusConflict, message)
	case strings.Contains(message, "not initialized") || strings.Contains(message, "unavailable"):
		s.sendError(w, "cron engine unavailable", http.StatusServiceUnavailable, message)
	default:
		s.sendError(w, "cron mutation failed", http.StatusBadRequest, message)
	}
}

func cronJobMissing(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

func cronJobJSON(job *cron.Job) cronJobResponse {
	if job == nil {
		return cronJobResponse{}
	}
	return cronJobResponse{
		ID:             job.ID,
		Name:           job.Name,
		Description:    job.Description,
		Schedule:       cron.DescribeSchedule(job.Schedule),
		Expression:     cronExpression(job),
		Status:         job.Status.String(),
		LastRun:        formatCronTime(job.LastRun),
		NextRun:        formatCronTime(job.NextRun),
		RunCount:       job.RunCount,
		ErrorCount:     job.ErrorCount,
		LastError:      job.LastError,
		CreatedAt:      job.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      job.UpdatedAt.Format(time.RFC3339),
		DeleteAfterRun: job.DeleteAfterRun,
		Metadata:       job.Metadata,
	}
}

func formatCronTime(value time.Time) *string {
	if value.IsZero() {
		return nil
	}
	formatted := value.Format(time.RFC3339)
	return &formatted
}

func parseCronSchedule(input string) (cron.Schedule, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, fmt.Errorf("schedule is required")
	}
	schedule, err := cron.ParseNaturalLanguage(trimmed)
	if err == nil {
		return schedule, nil
	}
	naturalErr := err
	parsed, cronErr := cron.ParseCronExpr(trimmed)
	if cronErr != nil {
		return nil, fmt.Errorf("%s", naturalErr.Error())
	}
	return parsed, nil
}

func parseCronMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "shell":
		return "shell", nil
	case "agent":
		return "agent", nil
	default:
		return "", fmt.Errorf("invalid cron mode %q (use shell or agent)", strings.TrimSpace(mode))
	}
}

// newCronJobID matches the unexported cron-tool id: a slug of mode and command,
// at most 48 characters, plus the unix timestamp in seconds.
func newCronJobID(mode, command string, now time.Time) string {
	base := strings.ToLower(strings.TrimSpace(mode + "-" + command))
	base = strings.ReplaceAll(base, "_", "-")
	base = strings.ReplaceAll(base, " ", "-")
	var b strings.Builder
	lastDash := false
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	id := strings.Trim(b.String(), "-")
	if id == "" {
		id = "cron-job"
	}
	if len(id) > 48 {
		id = strings.Trim(id[:48], "-")
	}
	return fmt.Sprintf("%s-%d", id, now.Unix())
}

// cronExpression returns the caller's original 5-field expression when the
// stored schedule is cron-backed. ParseCronExpr expands steps such as */15,
// so rebuilding fields would not round-trip. Interval and once schedules omit
// the field. Daily schedules use the minute-hour expression persistence stores
// when the caller did not send a 5-field expression.
func cronExpression(job *cron.Job) string {
	if job == nil {
		return ""
	}
	switch job.Schedule.(type) {
	case cron.CronSchedule, *cron.CronSchedule:
		if text := strings.TrimSpace(job.Metadata["schedule_text"]); text != "" && cronExprFieldCount(text) == 5 {
			return text
		}
		return cronExpressionFromSchedule(job.Schedule)
	case cron.DailySchedule, *cron.DailySchedule:
		if text := strings.TrimSpace(job.Metadata["schedule_text"]); text != "" && cronExprFieldCount(text) == 5 {
			return text
		}
		return cronExpressionFromSchedule(job.Schedule)
	default:
		return ""
	}
}

func cronExprFieldCount(input string) int {
	return len(strings.Fields(strings.TrimSpace(input)))
}

func cronExpressionFromSchedule(schedule cron.Schedule) string {
	switch s := schedule.(type) {
	case cron.CronSchedule:
		return cronFieldExpr(s)
	case *cron.CronSchedule:
		if s == nil {
			return ""
		}
		return cronFieldExpr(*s)
	case cron.DailySchedule:
		return fmt.Sprintf("%d %d * * *", s.Minute, s.Hour)
	case *cron.DailySchedule:
		if s == nil {
			return ""
		}
		return fmt.Sprintf("%d %d * * *", s.Minute, s.Hour)
	default:
		return ""
	}
}

func cronFieldExpr(s cron.CronSchedule) string {
	return strings.Join([]string{
		cronField(s.Minute),
		cronField(s.Hour),
		cronField(s.Day),
		cronField(s.Month),
		cronField(s.Weekday),
	}, " ")
}

func cronField(values []int) string {
	if len(values) == 0 {
		return "*"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}
