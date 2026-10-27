package transport

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChatCompleteRouteIsSeparateFromSessionChat(t *testing.T) {
	var complete, sync int
	noop := func(http.ResponseWriter, *http.Request) {}
	handlers := Handlers{
		HealthLiveness: noop, HealthReadiness: noop, HealthDetail: noop,
		Metrics: noop, ConfigReload: noop, ConfigSchema: noop, Config: noop,
		Models: noop, ModelEffort: noop, ModelSwitch: noop, ModelProfiles: noop,
		Approvals:    noop,
		Chat:         noop,
		ChatSync:     func(http.ResponseWriter, *http.Request) { sync++ },
		ChatComplete: func(http.ResponseWriter, *http.Request) { complete++ },
		Sessions:     noop, SessionByID: noop, Uploads: noop, Artifact: noop,
		Commands: noop, Cron: noop, CronByID: noop, Tasks: noop, TaskByID: noop,
		AutonomyDashboard: noop, AutonomyTaskByID: noop,
		Memory: noop, MemoryRecall: noop, MemoryRecallTrace: noop, MemoryStats: noop, MemoryGraph: noop,
		ProactiveStatus: noop, Tools: noop, Skills: noop, SkillRoutes: noop,
		Stats: noop, Soul: noop, Context: noop, ContextFit: noop,
		RAGIndex: noop, RAGSearch: noop, RAGStats: noop, RAGStore: noop,
		RAGStreamWatch: noop, RAGStreamScan: noop, RAGStreamStart: noop, RAGStreamStop: noop,
		RAGStreamStatus: noop, RAGStreamIndex: noop, RAGStreamQueue: noop, RAGStreamProcess: noop,
		FunctionCalling: noop, FCTools: noop, FCHistory: noop,
		WebSocket: noop, WSStats: noop,
		SoulTemplates: noop, SoulTemplateByID: noop,
		EmbedderList: noop, EmbedderRegister: noop, EmbedderSwitch: noop, EmbedderRoutes: noop,
		AgentsList: noop, AgentsRegister: noop, AgentsDeregister: noop, AgentsDelegate: noop,
		AgentsTask: noop, AgentsTasks: noop, AgentsCancel: noop, AgentsBlackboard: noop, AgentsMessages: noop,
		Workflows: noop, WorkflowByID: noop, WorkflowInstances: noop, WorkflowInstanceByID: noop,
		GatewaysList: noop, GatewayTelegramStart: noop, GatewayByName: noop,
		Root: noop,
	}
	mux := http.NewServeMux()
	RegisterRoutes(mux, handlers)

	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/chat/complete", nil))
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/v1/chat/sync", nil))
	if complete != 1 || sync != 1 {
		t.Fatalf("complete=%d sync=%d", complete, sync)
	}
}
