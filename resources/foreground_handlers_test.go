package resources

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yurika0211/luckyagent-tools/autonomy"
)

type foregroundRuntime struct {
	Runtime
	tasks []*autonomy.QueueTask
}

func (r *foregroundRuntime) ForegroundTasks() []*autonomy.QueueTask {
	return r.tasks
}

func TestForegroundTasksExposeBoundedProgressWithoutPrompt(t *testing.T) {
	started := time.Now().Add(-30 * time.Second)
	backend := &foregroundRuntime{tasks: []*autonomy.QueueTask{{
		ID: "tq-7", Title: "review", State: autonomy.TaskReady, SessionID: "s1",
		Metadata: map[string]string{"foreground_source": "API", "foreground_max_slices": "8", "foreground_max_total_seconds": "120"},
		Attempts: 3, BudgetStartAttempt: 1, Continuations: 2, FirstStartedAt: started,
		Checkpoint: []byte(`{"summary":"private prompt should not be returned"}`), CheckpointAt: started,
		UpdatedAt: started,
	}}}
	controller := New(Options{Runtime: backend})
	w := httptest.NewRecorder()
	controller.HandleForegroundTasks(w, httptest.NewRequest(http.MethodGet, "/api/v1/foreground/tasks?source=api", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response ForegroundTasksResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Count != 1 || len(response.Tasks) != 1 {
		t.Fatalf("response=%s", w.Body.String())
	}
	view := response.Tasks[0]
	if view.Source != "api" || view.CurrentSlice != 2 || view.RemainingSlices != 6 || !view.CheckpointPresent {
		t.Fatalf("progress=%+v", view)
	}
	if bytes.Contains(w.Body.Bytes(), []byte("private prompt")) {
		t.Fatal("foreground progress leaked checkpoint content")
	}
}
