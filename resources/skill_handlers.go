package resources

import (
	"context"
	"errors"
	transport "github.com/yurika0211/luckyagent-api"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	tool "github.com/yurika0211/luckyagent-tools"
)

// errNoSkillArchive is returned when a request names neither an uploaded file
// nor a local path.
var errNoSkillArchive = errors.New(`expected a multipart field named "file", or a JSON body with a "path"`)

// maxSkillArchiveBytes bounds a skill upload. It is separate from
// maxUploadBytes (chat attachments) because the two have different shapes.
const maxSkillArchiveBytes = 32 << 20
const maxUploadMemory = 8 << 20

// SkillToolDTO is the per-tool view. Registry state is reported alongside the
// declaration so the UI can show "declared but not callable".
type SkillToolDTO struct {
	Name          string `json:"name"`
	FullName      string `json:"full_name"`
	Description   string `json:"description,omitempty"`
	ExposeToModel bool   `json:"expose_to_model"`
	Registered    bool   `json:"registered"`
	Enabled       bool   `json:"enabled"`
}

// SkillDTO mirrors tool.SkillMetadata plus the ledger view. SkillState is an int
// enum with no MarshalJSON, so State is always rendered through String().
type SkillDTO struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Summary     string         `json:"summary,omitempty"`
	State       string         `json:"state"`
	Dir         string         `json:"dir,omitempty"`
	Aliases     []string       `json:"aliases,omitempty"`
	Tools       []SkillToolDTO `json:"tools"`
	ToolCount   int            `json:"tool_count"`
	Available   bool           `json:"available"`
	Version     string         `json:"version,omitempty"`
	Author      string         `json:"author,omitempty"`
	LoadedAt    string         `json:"loaded_at,omitempty"`
	Error       string         `json:"error,omitempty"`
	Unhealthy   []string       `json:"unhealthy_tools,omitempty"`

	// Managed is false for skills copied into skills/ by hand. Those have no
	// ledger entry, so there is nothing to roll back to.
	Managed     bool                `json:"managed"`
	InstallID   string              `json:"install_id,omitempty"`
	Digest      string              `json:"digest,omitempty"`
	Source      *tool.InstallSource `json:"source,omitempty"`
	InstalledAt string              `json:"installed_at,omitempty"`
	Versions    int                 `json:"versions,omitempty"`
}

// HandleSkills serves GET /api/v1/skills.
func (s *Controller) HandleSkills(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	if s.agent == nil {
		s.sendError(w, "agent runtime unavailable", http.StatusServiceUnavailable, "")
		return
	}

	skills := s.BuildSkillDTOs()
	if state := strings.TrimSpace(r.URL.Query().Get("state")); state != "" {
		filtered := skills[:0]
		for _, sk := range skills {
			if sk.State == state {
				filtered = append(filtered, sk)
			}
		}
		skills = filtered
	}
	if r.URL.Query().Get("managed") == "true" {
		filtered := skills[:0]
		for _, sk := range skills {
			if sk.Managed {
				filtered = append(filtered, sk)
			}
		}
		skills = filtered
	}

	s.sendJSON(w, http.StatusOK, map[string]interface{}{
		"skills":     skills,
		"count":      len(skills),
		"skills_dir": s.agent.SkillsDir(),
	})
}

func (s *Controller) BuildSkillDTOs() []SkillDTO {
	sr := s.agent.SkillRegistry()
	if sr == nil {
		return []SkillDTO{}
	}
	unhealthy := sr.HealthCheck()
	tools := s.agent.Tools()

	// Skill definitions carry aliases and per-tool descriptions; metadata carries
	// lifecycle state. Index the former by name to join them.
	infoByName := map[string]*tool.SkillInfo{}
	for _, info := range s.agent.Skills() {
		infoByName[info.Name] = info
	}

	var installer *tool.InstallManager
	if s.agent != nil {
		installer = s.agent.SkillInstaller()
	}

	metas := sr.List()
	out := make([]SkillDTO, 0, len(metas))
	for _, meta := range metas {
		dto := SkillDTO{
			Name:        meta.Name,
			Description: meta.Description,
			State:       meta.State.String(),
			Dir:         meta.Dir,
			Version:     meta.Version,
			Author:      meta.Author,
			Error:       meta.Error,
			Unhealthy:   unhealthy[meta.Name],
			ToolCount:   len(meta.Tools),
			Tools:       []SkillToolDTO{},
		}
		if !meta.LoadedAt.IsZero() {
			dto.LoadedAt = meta.LoadedAt.Format(time.RFC3339)
		}

		if info, ok := infoByName[meta.Name]; ok {
			dto.Aliases = info.Aliases
			dto.Summary = info.Summary
			dto.Available = info.Available
			for _, td := range info.Tools {
				full := "skill_" + info.Name + "_" + td.Name
				entry := SkillToolDTO{
					Name:          td.Name,
					FullName:      full,
					Description:   td.Description,
					ExposeToModel: td.ExposeToModel,
				}
				if tools != nil {
					if registered, ok := tools.Get(full); ok {
						entry.Registered = true
						entry.Enabled = registered.Enabled
					}
				}
				dto.Tools = append(dto.Tools, entry)
			}
		}

		if installer != nil {
			if rec, ok := installer.Store().Get(meta.Name); ok && rec != nil {
				dto.Managed = true
				dto.InstallID = rec.InstallID
				dto.Digest = rec.Digest
				src := rec.Source
				dto.Source = &src
				if !rec.InstalledAt.IsZero() {
					dto.InstalledAt = rec.InstalledAt.Format(time.RFC3339)
				}
			}
			dto.Versions = len(installer.Versions(meta.Name))
		}

		out = append(out, dto)
	}
	return out
}

// HandleSkillRoutes dispatches everything under /api/v1/skills/. ServeMux
// matches on the longest prefix, so this one handler owns the whole subtree.
func (s *Controller) HandleSkillRoutes(w http.ResponseWriter, r *http.Request) {
	if s.agent == nil {
		s.sendError(w, "agent runtime unavailable", http.StatusServiceUnavailable, "")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/skills/"), "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		s.sendError(w, "skill name is required", http.StatusBadRequest, "")
		return
	}

	// Reserved sub-collection words are matched first. A pre-existing skill could
	// be named "install" — installs are refused for that reason, but skills that
	// predate the pipeline never went through it.
	switch parts[0] {
	case "install":
		s.HandleSkillInstallPrepare(w, r)
		return
	case "installs":
		s.HandleSkillInstalls(w, r, parts[1:])
		return
	case "reload":
		s.HandleSkillReloadAll(w, r)
		return
	}

	name := strings.TrimSpace(parts[0])
	// Never build a path from a URL segment: validate, then resolve the directory
	// from the registry or the ledger.
	if !tool.ValidSkillName(name) {
		s.sendError(w, "invalid skill name", http.StatusBadRequest, "")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.HandleSkillDetail(w, name)
		case http.MethodDelete:
			s.HandleSkillUninstall(w, r, name)
		default:
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		}
		return
	}

	switch parts[1] {
	case "enable", "disable", "reload":
		s.HandleSkillLifecycle(w, r, name, parts[1])
	case "uninstall":
		s.HandleSkillUninstall(w, r, name)
	case "rollback":
		s.HandleSkillRollback(w, r, name)
	case "versions":
		s.HandleSkillVersions(w, r, name)
	default:
		s.sendError(w, "unknown skill subresource", http.StatusNotFound, parts[1])
	}
}

func (s *Controller) HandleSkillDetail(w http.ResponseWriter, name string) {
	for _, dto := range s.BuildSkillDTOs() {
		if dto.Name == name {
			s.sendJSON(w, http.StatusOK, dto)
			return
		}
	}
	s.sendError(w, "skill not found", http.StatusNotFound, name)
}

// HandleSkillLifecycle handles enable/disable/reload. The registry's own state
// machine is the source of truth; its rejection becomes a 409.
func (s *Controller) HandleSkillLifecycle(w http.ResponseWriter, r *http.Request, name, action string) {
	if r.Method != http.MethodPost {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	sr := s.agent.SkillRegistry()
	if sr == nil {
		s.sendError(w, "no skills are loaded", http.StatusServiceUnavailable, "")
		return
	}
	if _, ok := sr.Get(name); !ok {
		s.sendError(w, "skill not found", http.StatusNotFound, name)
		return
	}

	var err error
	switch action {
	case "enable":
		err = sr.Enable(name)
	case "disable":
		err = sr.Disable(name)
	case "reload":
		err = sr.Reload(name)
	}
	if err != nil {
		// These are state-machine refusals ("must be registered before enabling"),
		// not bad requests.
		s.sendError(w, "skill "+action+" failed", http.StatusConflict, err.Error())
		return
	}

	meta, _ := sr.Get(name)
	state := ""
	toolCount := 0
	if meta != nil {
		state = meta.State.String()
		toolCount = len(meta.Tools)
	}
	s.sendJSON(w, http.StatusOK, map[string]interface{}{
		"name":           name,
		"state":          state,
		"tools_affected": toolCount,
	})
}

func (s *Controller) HandleSkillReloadAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	count, err := s.agent.ReloadSkills()
	if err != nil {
		s.sendError(w, "reload skills failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.sendJSON(w, http.StatusOK, map[string]interface{}{
		"loaded":     count,
		"skills_dir": s.agent.SkillsDir(),
	})
}

func (s *Controller) HandleSkillUninstall(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	installer := s.agent.SkillInstaller()
	if installer == nil {
		s.sendError(w, "skill installer unavailable", http.StatusServiceUnavailable, "")
		return
	}

	var req struct {
		Purge bool `json:"purge"`
	}
	if r.Body != nil {
		if err := transport.DecodeJSON(w, r, &req, 64<<10); err != nil && !errors.Is(err, io.EOF) {
			transport.SendRequestError(w, err)
			return
		}
	}

	versionID, err := installer.Uninstall(name, req.Purge)
	if err != nil {
		if strings.Contains(err.Error(), "not installed") {
			s.sendError(w, "skill not found", http.StatusNotFound, name)
			return
		}
		s.sendError(w, "uninstall failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.sendJSON(w, http.StatusOK, map[string]interface{}{
		"name":       name,
		"removed":    true,
		"purged":     req.Purge,
		"version_id": versionID,
	})
}

func (s *Controller) HandleSkillVersions(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	installer := s.agent.SkillInstaller()
	if installer == nil {
		s.sendError(w, "skill installer unavailable", http.StatusServiceUnavailable, "")
		return
	}
	versions := installer.Versions(name)
	s.sendJSON(w, http.StatusOK, map[string]interface{}{
		"name":     name,
		"versions": versions,
		"count":    len(versions),
	})
}

func (s *Controller) HandleSkillRollback(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	installer := s.agent.SkillInstaller()
	if installer == nil {
		s.sendError(w, "skill installer unavailable", http.StatusServiceUnavailable, "")
		return
	}
	var req struct {
		VersionID string `json:"version_id"`
	}
	if err := transport.DecodeJSON(w, r, &req, 64<<10); err != nil && !errors.Is(err, io.EOF) {
		transport.SendRequestError(w, err)
		return
	}
	if strings.TrimSpace(req.VersionID) == "" {
		s.sendError(w, "version_id is required", http.StatusBadRequest, "")
		return
	}
	if err := installer.Rollback(name, req.VersionID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, "version not found", http.StatusNotFound, err.Error())
			return
		}
		s.sendError(w, "rollback failed", http.StatusInternalServerError, err.Error())
		return
	}
	s.HandleSkillDetail(w, name)
}

// ---- install ----

// HandleSkillInstallPrepare accepts an upload or a local path and returns 202.
//
// Prepare runs in the background because the probe stage spawns one subprocess
// per discovered subcommand — at the default per-probe ceiling that alone can
// exceed the server's 120s WriteTimeout.
func (s *Controller) HandleSkillInstallPrepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	installer := s.agent.SkillInstaller()
	if installer == nil {
		s.sendError(w, "skill installer unavailable", http.StatusServiceUnavailable, "")
		return
	}
	if s.installDisabled != nil && s.installDisabled() {
		s.sendError(w, "skill installation is disabled", http.StatusForbidden, "set skills.install.disabled to false to enable it")
		return
	}

	var (
		id  string
		err error
	)
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		id, err = s.StageSkillUpload(w, r, installer)
	} else {
		id, err = s.StageSkillLocalPath(r, installer)
	}
	if err != nil {
		var requestError *transport.RequestError
		var tooLarge *http.MaxBytesError
		if errors.As(err, &requestError) || errors.As(err, &tooLarge) {
			transport.SendRequestError(w, err)
			return
		}
		s.sendError(w, "stage skill failed", http.StatusBadRequest, err.Error())
		return
	}

	// r.Context() is cancelled when the response completes, so the background
	// pass gets its own.
	go func(installID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = installer.Prepare(installID)
		}()
		select {
		case <-done:
		case <-ctx.Done():
		}
	}(id)

	s.sendJSON(w, http.StatusAccepted, map[string]interface{}{
		"install_id": id,
		"status":     tool.InstallStaging,
		"poll":       "/api/v1/skills/installs/" + id,
	})
}

// StageSkillUpload copies the multipart bytes into staging synchronously. It has
// to be synchronous: ParseMultipartForm's temp files are removed by the deferred
// RemoveAll the instant this handler returns.
func (s *Controller) StageSkillUpload(w http.ResponseWriter, r *http.Request, installer *tool.InstallManager) (string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSkillArchiveBytes)
	if err := r.ParseMultipartForm(maxUploadMemory); err != nil {
		return "", err
	}
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()

	files := r.MultipartForm.File["file"]
	if len(files) == 0 {
		files = r.MultipartForm.File["files"]
	}
	if len(files) == 0 {
		return "", errNoSkillArchive
	}
	header := files[0]

	id, _, err := installer.StageArchive(header.Filename, func(dst string) error {
		return copyMultipartTo(header, dst)
	})
	return id, err
}

func copyMultipartTo(header *multipart.FileHeader, dst string) error {
	src, err := header.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, io.LimitReader(src, maxSkillArchiveBytes)); err != nil {
		_ = os.Remove(dst)
		return err
	}
	return nil
}

func (s *Controller) StageSkillLocalPath(r *http.Request, installer *tool.InstallManager) (string, error) {
	var req struct {
		Source string `json:"source"`
		Path   string `json:"path"`
	}
	if err := transport.DecodeJSON(nil, r, &req, 64<<10); err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if strings.TrimSpace(req.Path) == "" {
		return "", errNoSkillArchive
	}
	id, _, err := installer.StageLocalDir(filepath.Clean(req.Path))
	return id, err
}

// HandleSkillInstalls serves the installs sub-collection: list, poll, confirm,
// abort.
func (s *Controller) HandleSkillInstalls(w http.ResponseWriter, r *http.Request, rest []string) {
	installer := s.agent.SkillInstaller()
	if installer == nil {
		s.sendError(w, "skill installer unavailable", http.StatusServiceUnavailable, "")
		return
	}

	if len(rest) == 0 {
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		records := installer.ListStaged()
		s.sendJSON(w, http.StatusOK, map[string]interface{}{
			"installs": installRecordDTOs(records),
			"count":    len(records),
		})
		return
	}

	id := strings.TrimSpace(rest[0])
	if len(rest) == 1 {
		if r.Method != http.MethodGet {
			s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
			return
		}
		rec, err := installer.Staged(id)
		if err != nil {
			s.sendError(w, "install not found", http.StatusNotFound, id)
			return
		}
		s.sendJSON(w, http.StatusOK, installRecordDTO(rec))
		return
	}

	if r.Method != http.MethodPost {
		s.sendError(w, "method not allowed", http.StatusMethodNotAllowed, "")
		return
	}
	switch rest[1] {
	case "confirm":
		var req struct {
			AcceptWarnings bool `json:"accept_warnings"`
		}
		if r.Body != nil {
			if err := transport.DecodeJSON(w, r, &req, 64<<10); err != nil && !errors.Is(err, io.EOF) {
				transport.SendRequestError(w, err)
				return
			}
		}
		rec, err := installer.Confirm(id, req.AcceptWarnings)
		if err != nil {
			status := http.StatusConflict
			errorCode := "conflict"
			if rec == nil {
				status = http.StatusNotFound
				errorCode = "not_found"
			}
			s.sendJSON(w, status, map[string]interface{}{
				"error":      "confirm failed",
				"code":       status,
				"error_code": errorCode,
				"request_id": w.Header().Get("X-Request-ID"),
				"details":    err.Error(),
				"install":    installRecordDTO(rec),
			})
			return
		}
		s.sendJSON(w, http.StatusOK, installRecordDTO(rec))
	case "abort":
		if err := installer.Abort(id); err != nil {
			s.sendError(w, "abort failed", http.StatusInternalServerError, err.Error())
			return
		}
		s.sendJSON(w, http.StatusOK, map[string]interface{}{
			"install_id": id,
			"status":     tool.InstallAborted,
		})
	default:
		s.sendError(w, "unknown install action", http.StatusNotFound, rest[1])
	}
}

// installRecordDTO adds the derived can_confirm flag so the UI does not have to
// re-implement the rule.
func installRecordDTO(rec *tool.InstallRecord) map[string]interface{} {
	if rec == nil {
		return nil
	}
	return map[string]interface{}{
		"install_id":  rec.InstallID,
		"name":        rec.Name,
		"status":      rec.State,
		"mode":        rec.Mode,
		"error":       rec.Error,
		"digest":      rec.Digest,
		"source":      rec.Source,
		"scan":        rec.Scan,
		"capability":  rec.Capability,
		"created_at":  rec.CreatedAt.Format(time.RFC3339),
		"updated_at":  rec.UpdatedAt.Format(time.RFC3339),
		"can_confirm": rec.CanConfirm(),
	}
}

func installRecordDTOs(records []*tool.InstallRecord) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
		out = append(out, installRecordDTO(rec))
	}
	return out
}
