package resources

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tool "github.com/yurika0211/luckyagent-tools"
)

type approvalRuntime struct {
	Runtime
	items  []tool.PendingApproval
	called bool
}

func (r *approvalRuntime) PendingApprovals() []tool.PendingApproval {
	return append([]tool.PendingApproval(nil), r.items...)
}
func (r *approvalRuntime) RespondApprovalWithInput(ctx context.Context, provider, id, decision, input string) (tool.PendingApproval, error) {
	r.called = true
	if _, ok := ctx.Deadline(); !ok {
		panic("approval deadline missing")
	}
	return tool.PendingApproval{Provider: provider, ID: id, SessionID: input}, nil
}

func TestApprovalControllerUsesInjectedBackend(t *testing.T) {
	backend := &approvalRuntime{items: []tool.PendingApproval{{Provider: "codex", SessionID: "s1"}, {Provider: "grok", SessionID: "s2"}}}
	controller := New(Options{Runtime: backend})
	w := httptest.NewRecorder()
	controller.HandleApprovals(w, httptest.NewRequest(http.MethodGet, "/api/v1/approvals?provider=CODEX&session_id=s1", nil))
	var response approvalsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || response.Count != 1 || response.Approvals[0].Provider != "codex" {
		t.Fatalf("unexpected filtered response: %s", w.Body.String())
	}
	w = httptest.NewRecorder()
	controller.HandleApprovals(w, httptest.NewRequest(http.MethodPost, "/api/v1/approvals", strings.NewReader(`{"provider":" CODEX ","approval_id":"a1","decision":"allow","input":"s1"}`)))
	if w.Code != http.StatusOK || !backend.called {
		t.Fatalf("resolution failed: %s", w.Body.String())
	}
	backend.called = false
	w = httptest.NewRecorder()
	controller.HandleApprovals(w, httptest.NewRequest(http.MethodPost, "/api/v1/approvals", strings.NewReader(`{"provider":"codex"}`)))
	if w.Code != http.StatusBadRequest || backend.called {
		t.Fatal("invalid request reached approval backend")
	}
}

func TestUnavailableApprovalBackend(t *testing.T) {
	w := httptest.NewRecorder()
	New(Options{}).HandleApprovals(w, httptest.NewRequest(http.MethodGet, "/api/v1/approvals", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", w.Code)
	}
}
