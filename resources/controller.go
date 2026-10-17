// Package resources serves optional tool-runtime HTTP resources through host-supplied ports.
package resources

import (
	"context"
	transport "github.com/yurika0211/luckyagent-api"
	tool "github.com/yurika0211/luckyagent-tools"
	"github.com/yurika0211/luckyagent-tools/autonomy"
	"github.com/yurika0211/luckyagent-tools/collab"
	"github.com/yurika0211/luckyagent-tools/cron"
	"github.com/yurika0211/luckyagent-tools/task"
	"net/http"
)

// TaskBackend supplies durable task storage and the active delegate executor.
type TaskBackend interface {
	TaskStore() task.Store
	Delegate() *tool.DelegateManager
}
type ApprovalBackend interface {
	PendingApprovals() []tool.PendingApproval
	RespondApprovalWithInput(context.Context, string, string, string, string) (tool.PendingApproval, error)
}
type CronBackend interface{ CronEngine() *cron.Engine }
type AutonomyBackend interface{ Autonomy() *autonomy.AutonomyKit }
type SkillBackend interface {
	SkillRegistry() *tool.SkillRegistry
	Tools() *tool.Registry
	Skills() []*tool.SkillInfo
	SkillInstaller() *tool.InstallManager
	ReloadSkills() (int, error)
	SkillsDir() string
}

// Runtime composes the optional capabilities exposed by this controller.
type Runtime interface {
	TaskBackend
	ApprovalBackend
	CronBackend
	AutonomyBackend
	SkillBackend
}
type Options struct {
	Runtime            Runtime
	Registry           *collab.Registry
	Delegates          *collab.DelegateManager
	InstallDisabled    func() bool
	EnsureOrchestrator func(string)
	SendJSON           func(http.ResponseWriter, int, any)
	SendError          func(http.ResponseWriter, string, int, string)
}
type Controller struct {
	agent              Runtime
	collabRegistry     *collab.Registry
	delegateManager    *collab.DelegateManager
	installDisabled    func() bool
	ensureOrchestrator func(string)
	sendJSON           func(http.ResponseWriter, int, any)
	sendError          func(http.ResponseWriter, string, int, string)
}

func New(options Options) *Controller {
	if options.SendJSON == nil {
		options.SendJSON = transport.SendJSON
	}
	if options.SendError == nil {
		options.SendError = transport.SendError
	}
	return &Controller{agent: options.Runtime, collabRegistry: options.Registry, delegateManager: options.Delegates, installDisabled: options.InstallDisabled, ensureOrchestrator: options.EnsureOrchestrator, sendJSON: options.SendJSON, sendError: options.SendError}
}
