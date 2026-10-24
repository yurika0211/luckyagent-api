package resources

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	transport "github.com/yurika0211/luckyagent-api"
	"github.com/yurika0211/luckyagent-tools/cron"
)

type cronTestRuntime struct {
	Runtime
	engine *cron.Engine
	saved  int
	err    error
}

func (r *cronTestRuntime) CronEngine() *cron.Engine { return r.engine }

func (r *cronTestRuntime) AddCronJob(id, name, description, mode, command string, metadata map[string]string, schedule cron.Schedule) (*cron.Job, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.engine == nil {
		return nil, fmt.Errorf("cron engine is not initialized")
	}
	meta := map[string]string{"mode": mode, "command": command}
	for key, value := range metadata {
		meta[key] = value
	}
	if strings.TrimSpace(name) == "" {
		name = "Cron: " + id
	}
	if strings.TrimSpace(description) == "" {
		description = command
	}
	if err := r.engine.AddJobWithMeta(id, name, description, schedule, func() error { return nil }, meta); err != nil {
		return nil, err
	}
	job, ok := r.engine.GetJob(id)
	if !ok {
		return nil, fmt.Errorf("job %s not found", id)
	}
	if meta["delete_after_run"] == "true" || meta["delete_after_run"] == "1" || meta["delete_after_run"] == "yes" {
		job.DeleteAfterRun = true
	}
	r.saved++
	return job, nil
}

func (r *cronTestRuntime) PauseCronJob(id string) error {
	if err := r.engine.PauseJob(id); err != nil {
		return err
	}
	r.saved++
	return nil
}

func (r *cronTestRuntime) ResumeCronJob(id string) error {
	if err := r.engine.ResumeJob(id); err != nil {
		return err
	}
	r.saved++
	return nil
}

func (r *cronTestRuntime) RemoveCronJob(id string) error {
	if err := r.engine.RemoveJob(id); err != nil {
		return err
	}
	r.saved++
	return nil
}

func newCronController(t *testing.T) (*Controller, *cronTestRuntime) {
	t.Helper()
	backend := &cronTestRuntime{engine: cron.NewEngine()}
	return New(Options{Runtime: backend}), backend
}

func decodeCronError(t *testing.T, w *httptest.ResponseRecorder) transport.ErrorResponse {
	t.Helper()
	var response transport.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("error is not JSON: %v body=%s", err, w.Body.String())
	}
	return response
}

type cronReadRuntime struct {
	Runtime
	engine *cron.Engine
}

func (r *cronReadRuntime) CronEngine() *cron.Engine { return r.engine }

func TestCronRejectsUnsupportedMethods(t *testing.T) {
	controller, _ := newCronController(t)
	w := httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodPut, "/api/v1/cron", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("collection status=%d body=%s", w.Code, w.Body.String())
	}
	if decodeCronError(t, w).ErrorCode != "method_not_allowed" {
		t.Fatalf("collection error=%s", w.Body.String())
	}

	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodPost, "/api/v1/cron/job-1", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("item status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestCronCreateRejectsBadScheduleAndMissingEngine(t *testing.T) {
	controller, _ := newCronController(t)
	body := `{"schedule":"not a schedule","command":"echo hi","metadata":{"session_id":"s1","platform":"android"}}`
	w := httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodPost, "/api/v1/cron", strings.NewReader(body)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	response := decodeCronError(t, w)
	if response.ErrorCode != "invalid_request" || response.Details == "" {
		t.Fatalf("expected parser detail: %s", w.Body.String())
	}

	missing := New(Options{})
	w = httptest.NewRecorder()
	missing.HandleCron(w, httptest.NewRequest(http.MethodGet, "/api/v1/cron", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing engine status=%d", w.Code)
	}
	w = httptest.NewRecorder()
	missing.HandleCron(w, httptest.NewRequest(http.MethodPost, "/api/v1/cron", strings.NewReader(body)))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing writer status=%d", w.Code)
	}

	readOnly := New(Options{Runtime: &cronReadRuntime{engine: cron.NewEngine()}})
	w = httptest.NewRecorder()
	readOnly.HandleCronByID(w, httptest.NewRequest(http.MethodDelete, "/api/v1/cron/job-1", nil))
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "cron writer unavailable") {
		t.Fatalf("read-only delete=%d %s", w.Code, w.Body.String())
	}
}

func TestCronCreatePauseResumeDelete(t *testing.T) {
	controller, backend := newCronController(t)
	body := `{"name":"晨报","description":"汇总","schedule":"每天9点","mode":"agent","command":"汇总昨日","metadata":{"session_id":"sess-1","platform":"android","chat_id":"c1"},"delete_after_run":true}`
	w := httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodPost, "/api/v1/cron", strings.NewReader(body)))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", w.Code, w.Body.String())
	}
	var created cronJobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Schedule != "每天09:00" {
		t.Fatalf("schedule human text changed: %+v", created)
	}
	if created.Expression != "0 9 * * *" {
		t.Fatalf("expression=%q", created.Expression)
	}
	if created.Status != "idle" || !created.DeleteAfterRun || created.Metadata["session_id"] != "sess-1" || created.Metadata["platform"] != "android" || created.Metadata["command"] != "汇总昨日" {
		t.Fatalf("created job=%+v", created)
	}
	if created.Metadata["schedule_text"] != "每天9点" {
		t.Fatalf("schedule_text=%q", created.Metadata["schedule_text"])
	}
	if !strings.HasPrefix(created.ID, "agent-") || !strings.HasSuffix(created.ID, fmt.Sprintf("-%d", time.Now().Unix())) {
		t.Fatalf("id shape=%s", created.ID)
	}
	if backend.saved != 1 {
		t.Fatalf("saved=%d", backend.saved)
	}

	w = httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodGet, "/api/v1/cron", nil))
	var listed struct {
		Running bool              `json:"running"`
		Count   int               `json:"count"`
		Jobs    []cronJobResponse `json:"jobs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || listed.Count != 1 || listed.Jobs[0].ID != created.ID || listed.Jobs[0].Expression != "0 9 * * *" || listed.Jobs[0].Schedule != "每天09:00" {
		t.Fatalf("list=%s", w.Body.String())
	}

	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodPatch, "/api/v1/cron/"+created.ID, strings.NewReader(`{"action":"pause"}`)))
	var paused cronJobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &paused); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || paused.Status != "paused" {
		t.Fatalf("pause=%d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodPatch, "/api/v1/cron/"+created.ID, strings.NewReader(`{"action":"resume"}`)))
	var resumed cronJobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resumed); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || resumed.Status != "idle" {
		t.Fatalf("resume=%d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodDelete, "/api/v1/cron/"+created.ID, nil))
	if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
		t.Fatalf("delete=%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodGet, "/api/v1/cron", nil))
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if listed.Count != 0 || len(listed.Jobs) != 0 {
		t.Fatalf("job remained: %s", w.Body.String())
	}
	if backend.saved != 4 {
		t.Fatalf("persist calls=%d", backend.saved)
	}
}

func TestCronMissingJobAndCronExpression(t *testing.T) {
	controller, backend := newCronController(t)
	w := httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodGet, "/api/v1/cron/missing", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("get status=%d", w.Code)
	}
	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodPatch, "/api/v1/cron/missing", strings.NewReader(`{"action":"pause"}`)))
	if w.Code != http.StatusNotFound {
		t.Fatalf("pause status=%d body=%s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodDelete, "/api/v1/cron/missing", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete status=%d", w.Code)
	}

	body := `{"schedule":"*/15 * * * *","mode":"shell","command":"echo hi","metadata":{"session_id":"s1","platform":"android"}}`
	w = httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodPost, "/api/v1/cron", strings.NewReader(body)))
	if w.Code != http.StatusCreated {
		t.Fatalf("cron expr create=%d %s", w.Code, w.Body.String())
	}
	var created cronJobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Expression != "*/15 * * * *" || created.Schedule != "0,15,30,45 * * * *" {
		t.Fatalf("expression contract=%+v", created)
	}

	if err := backend.engine.AddJob("every", "Every", "tick", cron.IntervalSchedule{Interval: time.Hour}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	controller.HandleCronByID(w, httptest.NewRequest(http.MethodGet, "/api/v1/cron/every", nil))
	var interval cronJobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &interval); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || interval.Schedule != "每小时" || interval.Expression != "" || strings.Contains(w.Body.String(), `"expression"`) {
		t.Fatalf("interval expression should be omitted: %s", w.Body.String())
	}
}

func TestCronCreateRequiresSessionDeliveryMetadata(t *testing.T) {
	controller, backend := newCronController(t)
	body := `{"schedule":"每天9点","command":"echo hi","metadata":{"platform":"android"}}`
	w := httptest.NewRecorder()
	controller.HandleCron(w, httptest.NewRequest(http.MethodPost, "/api/v1/cron", strings.NewReader(body)))
	if w.Code != http.StatusBadRequest || backend.saved != 0 {
		t.Fatalf("status=%d saved=%d body=%s", w.Code, backend.saved, w.Body.String())
	}
}
