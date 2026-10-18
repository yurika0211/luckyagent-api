package resources

import (
	"context"
	"encoding/json"
	transport "github.com/yurika0211/luckyagent-api"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tool "github.com/yurika0211/luckyagent-tools"
	"github.com/yurika0211/luckyagent-tools/collab"
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

func TestAgentRegistrationRejectsInvalidBodiesBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		want                    int
	}{
		{"malformed", `{`, "application/json", 400},
		{"multiple values", `{"id":"blocked"} {}`, "application/json", 400},
		{"wrong media type", `{"id":"blocked"}`, "text/plain", 415},
		{"four MiB body", `{"id":"blocked","description":"` + strings.Repeat("x", 4<<20) + `"}`, "application/json", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := collab.NewRegistry()
			controller := New(Options{Registry: registry})
			r := httptest.NewRequest(http.MethodPost, "/api/v1/agents/register", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			controller.HandleAgentsRegister(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
			var response transport.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("error is not JSON: %v: %s", err, w.Body.String())
			}
			if response.Code != tc.want || response.ErrorCode == "" {
				t.Fatalf("incomplete error envelope: %s", w.Body.String())
			}
			if _, exists := registry.Get("blocked"); exists {
				t.Fatal("invalid request registered an agent")
			}
		})
	}
}

func TestCollaborationErrorsUseCommonJSONEnvelope(t *testing.T) {
	controller := New(Options{Registry: collab.NewRegistry()})
	w := httptest.NewRecorder()
	controller.HandleAgentsGet(w, httptest.NewRequest(http.MethodGet, "/api/v1/agents?id=absent", nil))
	var response transport.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != 404 || response.Code != 404 || response.ErrorCode != "not_found" || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("unexpected common error: %s", w.Body.String())
	}
}
