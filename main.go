package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	extName    = "zot-loop"
	extVersion = "0.1.0"
	maxLoops   = 25
	maxMon     = 25
	loopMaxAge = 7 * 24 * time.Hour
)

type Frame map[string]any

type HostInfo struct {
	CWD          string `json:"cwd"`
	ExtensionDir string `json:"extension_dir"`
	DataDir      string `json:"data_dir"`
}

type App struct {
	mu       sync.Mutex
	outMu    sync.Mutex
	host     HostInfo
	dataPath string

	loops    []Loop
	monitors map[string]*Monitor
	tasks    []Task
	nextLoop int
	nextTask int
	nextMon  int
	panels   map[string]int
}

type Loop struct {
	ID          string    `json:"id"`
	Trigger     string    `json:"trigger"`
	Prompt      string    `json:"prompt"`
	Status      string    `json:"status"`
	Recurring   bool      `json:"recurring"`
	ReadOnly    bool      `json:"readOnly,omitempty"`
	MaxFires    int       `json:"maxFires,omitempty"`
	FireCount   int       `json:"fireCount"`
	TriggerType string    `json:"triggerType"`
	EventSource string    `json:"eventSource,omitempty"`
	DebounceMs  int       `json:"debounceMs,omitempty"`
	TaskBacklog bool      `json:"taskBacklog,omitempty"`
	LastFire    time.Time `json:"lastFire,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	NextFire    time.Time `json:"nextFire,omitempty"`
}

type Task struct {
	ID          string    `json:"id"`
	Subject     string    `json:"subject"`
	Description string    `json:"description,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Monitor struct {
	ID          string             `json:"id"`
	Command     string             `json:"command"`
	Description string             `json:"description,omitempty"`
	OnDone      string             `json:"onDone,omitempty"`
	Status      string             `json:"status"`
	StartedAt   time.Time          `json:"startedAt"`
	EndedAt     time.Time          `json:"endedAt,omitempty"`
	Exit        string             `json:"exit,omitempty"`
	Lines       []string           `json:"lines"`
	Cancel      context.CancelFunc `json:"-"`
	Cmd         *exec.Cmd          `json:"-"`
}

type State struct {
	Loops    []Loop `json:"loops"`
	Tasks    []Task `json:"tasks"`
	NextLoop int    `json:"nextLoop"`
	NextTask int    `json:"nextTask"`
}

func main() {
	app := &App{monitors: map[string]*Monitor{}, nextLoop: 1, nextTask: 1, nextMon: 1, panels: map[string]int{}}
	app.send(Frame{"type": "hello", "name": extName, "version": extVersion, "capabilities": []string{"commands", "tools", "events", "panels", "submit"}})

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var msg Frame
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			logf("bad frame: %v", err)
			continue
		}
		app.handle(msg)
	}
}

func (a *App) handle(msg Frame) {
	switch str(msg["type"]) {
	case "hello_ack":
		a.host = HostInfo{CWD: str(msg["cwd"]), ExtensionDir: str(msg["extension_dir"]), DataDir: str(msg["data_dir"])}
		if a.host.DataDir == "" {
			a.host.DataDir = a.host.ExtensionDir
		}
		if a.host.DataDir == "" {
			a.host.DataDir = "."
		}
		a.dataPath = filepath.Join(a.host.DataDir, "state.json")
		a.load()
		a.register()
		go a.scheduler()
	case "command_invoked":
		a.command(str(msg["id"]), str(msg["name"]), str(msg["args"]))
	case "tool_call":
		a.tool(str(msg["id"]), str(msg["name"]), msg["args"])
	case "event":
		a.handleEvent(str(msg["event"]))
	case "panel_key":
		a.panelKey(str(msg["panel_id"]), str(msg["key"]), str(msg["text"]))
	case "panel_close":
		a.panelClose(str(msg["panel_id"]))
	case "shutdown":
		a.shutdown()
		a.send(Frame{"type": "shutdown_ack"})
		os.Exit(0)
	}
}

func (a *App) register() {
	a.send(Frame{"type": "register_command", "name": "loop", "description": "create or inspect repeating reminders: /loop [interval] [prompt]"})
	a.send(Frame{"type": "register_command", "name": "tasks", "description": "show or quick-create lightweight tasks"})
	a.send(Frame{"type": "subscribe", "events": []string{"turn_start", "turn_end", "tool_call", "assistant_message"}})
	for _, t := range []Frame{
		tool("LoopCreate", "Schedule a recurring reminder on an interval, an event, or both with debounce. Use instead of raw shell sleep loops.", schema(map[string]any{"trigger": prop("string", "Interval such as 30s, 5m, 2h, 1d, event source, or hybrid spec"), "prompt": prop("string", "Prompt or reminder text"), "recurring": prop("boolean", "Whether the reminder repeats, default true"), "triggerType": enumProp([]string{"cron", "event", "hybrid"}, "cron, event, or hybrid"), "eventSource": prop("string", "Event source for event or hybrid loops"), "debounceMs": prop("number", "Debounce for hybrid event fires, default 30000"), "taskBacklog": prop("boolean", "Auto-delete this loop when no pending or in-progress tasks remain"), "readOnly": prop("boolean", "Mark the reminder as read-only guidance"), "maxFires": prop("number", "Stop after this many fires")}, []string{"trigger", "prompt"})),
		tool("LoopList", "List active reminders with IDs and next-fire times.", schema(map[string]any{}, nil)),
		tool("LoopDelete", "Delete or pause a reminder by ID.", schema(map[string]any{"id": prop("string", "Loop ID"), "action": enumProp([]string{"delete", "pause", "resume"}, "Action, default delete")}, []string{"id"})),
		tool("MonitorCreate", "Run a background command and keep recent output. Use onDone to notify when it exits.", schema(map[string]any{"command": prop("string", "Command to run"), "description": prop("string", "Short label"), "onDone": prop("string", "Message to show when the command exits")}, []string{"command"})),
		tool("MonitorList", "Show background monitors with status, uptime, and recent output count.", schema(map[string]any{}, nil)),
		tool("MonitorOutput", "Read recent output from a monitor.", schema(map[string]any{"monitorId": prop("string", "Monitor ID"), "lines": prop("number", "Number of recent lines, default 50")}, []string{"monitorId"})),
		tool("MonitorStop", "Stop a running monitor gracefully, then force-stop it if needed.", schema(map[string]any{"monitorId": prop("string", "Monitor ID")}, []string{"monitorId"})),
		tool("TaskCreate", "Create a lightweight task for this project/session.", schema(map[string]any{"subject": prop("string", "Task subject"), "description": prop("string", "Task details")}, []string{"subject"})),
		tool("TaskList", "List lightweight tasks.", schema(map[string]any{"status": prop("string", "Optional status filter")}, nil)),
		tool("TaskUpdate", "Update a lightweight task.", schema(map[string]any{"id": prop("string", "Task ID"), "status": enumProp([]string{"pending", "in_progress", "done"}, "New status"), "subject": prop("string", "New subject"), "description": prop("string", "New description")}, []string{"id"})),
		tool("TaskDelete", "Delete a lightweight task.", schema(map[string]any{"id": prop("string", "Task ID")}, []string{"id"})),
		tool("TaskPrune", "Delete completed lightweight tasks.", schema(map[string]any{}, nil)),
	} {
		a.send(t)
	}
	a.send(Frame{"type": "ready"})
}

func tool(name, desc string, sch Frame) Frame {
	return Frame{"type": "register_tool", "name": name, "description": desc, "schema": sch}
}
func schema(props map[string]any, req []string) Frame {
	s := Frame{"type": "object", "properties": props}
	if len(req) > 0 {
		s["required"] = req
	}
	return s
}
func prop(typ, desc string) Frame { return Frame{"type": typ, "description": desc} }
func enumProp(vals []string, desc string) Frame {
	return Frame{"type": "string", "enum": vals, "description": desc}
}

func (a *App) command(id, name, args string) {
	switch name {
	case "loop":
		parts := strings.Fields(args)
		if len(parts) >= 2 {
			entry, err := a.createLoop(parts[0], strings.TrimSpace(strings.TrimPrefix(args, parts[0])), true, false, 0)
			if err != nil {
				a.cmdDisplay(id, err.Error())
				return
			}
			a.cmdDisplay(id, fmt.Sprintf("Loop #%s created: every %s\n%s", entry.ID, entry.Trigger, entry.Prompt))
			return
		}
		a.openLoopsPanel(id)
	case "tasks":
		if strings.TrimSpace(args) != "" {
			t := a.createTask(args, "")
			a.cmdDisplay(id, fmt.Sprintf("Task #%s created: %s", t.ID, t.Subject))
			return
		}
		a.openTasksPanel(id)
	default:
		a.send(Frame{"type": "command_response", "id": id, "action": "noop"})
	}
}

func (a *App) tool(id, name string, raw any) {
	args := asMap(raw)
	var out string
	var err error
	switch name {
	case "LoopCreate":
		recurring := !has(args, "recurring") || boolv(args["recurring"])
		entry, e := a.createLoopWithOptions(LoopOptions{Trigger: str(args["trigger"]), Prompt: str(args["prompt"]), Recurring: recurring, ReadOnly: boolv(args["readOnly"]), MaxFires: intv(args["maxFires"]), TriggerType: str(args["triggerType"]), EventSource: str(args["eventSource"]), DebounceMs: intv(args["debounceMs"]), TaskBacklog: boolv(args["taskBacklog"])})
		err = e
		if err == nil {
			out = fmt.Sprintf("Loop #%s created: %s\nTrigger: %s\nRecurring: %v\nID: %s (use LoopDelete to cancel)", entry.ID, entry.Prompt, loopTriggerDesc(entry), entry.Recurring, entry.ID)
		}
	case "LoopList":
		out = a.loopList()
	case "LoopDelete":
		out, err = a.loopDelete(str(args["id"]), str(args["action"]))
	case "MonitorCreate":
		out, err = a.monitorCreate(str(args["command"]), str(args["description"]), str(args["onDone"]))
	case "MonitorList":
		out = a.monitorList()
	case "MonitorOutput":
		out, err = a.monitorOutput(str(args["monitorId"]), intv(args["lines"]))
	case "MonitorStop":
		out, err = a.monitorStop(str(args["monitorId"]))
	case "TaskCreate":
		t := a.createTask(str(args["subject"]), str(args["description"]))
		out = fmt.Sprintf("Task #%s created: %s", t.ID, t.Subject)
	case "TaskList":
		out = a.taskList(str(args["status"]))
	case "TaskUpdate":
		out, err = a.taskUpdate(str(args["id"]), args)
	case "TaskDelete":
		out, err = a.taskDelete(str(args["id"]))
	case "TaskPrune":
		out = a.taskPrune()
	default:
		err = fmt.Errorf("unknown tool %s", name)
	}
	if err != nil {
		a.toolResult(id, err.Error(), true)
		return
	}
	a.toolResult(id, out, false)
}

type LoopOptions struct {
	Trigger     string
	Prompt      string
	Recurring   bool
	ReadOnly    bool
	MaxFires    int
	TriggerType string
	EventSource string
	DebounceMs  int
	TaskBacklog bool
}

func (a *App) createLoop(trigger, prompt string, recurring, readOnly bool, maxFires int) (Loop, error) {
	return a.createLoopWithOptions(LoopOptions{Trigger: trigger, Prompt: prompt, Recurring: recurring, ReadOnly: readOnly, MaxFires: maxFires})
}

func (a *App) createLoopWithOptions(opts LoopOptions) (Loop, error) {
	if strings.TrimSpace(opts.Prompt) == "" {
		return Loop{}, errors.New("prompt is required")
	}
	triggerType := opts.TriggerType
	if triggerType == "" {
		triggerType = inferTriggerType(opts.Trigger)
	}
	if triggerType != "cron" && triggerType != "event" && triggerType != "hybrid" {
		return Loop{}, errors.New("triggerType must be cron, event, or hybrid")
	}
	var nextFire time.Time
	if triggerType == "cron" || triggerType == "hybrid" {
		d, err := parseSchedule(opts.Trigger, time.Now())
		if err != nil {
			return Loop{}, err
		}
		nextFire = time.Now().Add(d)
	}
	eventSource := opts.EventSource
	if eventSource == "" && triggerType == "event" {
		eventSource = strings.TrimPrefix(opts.Trigger, "event:")
	}
	if triggerType == "event" && eventSource == "" {
		return Loop{}, errors.New("eventSource is required for event loops")
	}
	if triggerType == "hybrid" && eventSource == "" {
		eventSource = "tool_call"
	}
	debounceMs := opts.DebounceMs
	if debounceMs <= 0 {
		debounceMs = 30000
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.loops) >= maxLoops {
		return Loop{}, fmt.Errorf("limit reached: %d active loops", maxLoops)
	}
	entry := Loop{ID: strconv.Itoa(a.nextLoop), Trigger: opts.Trigger, Prompt: opts.Prompt, Status: "active", Recurring: opts.Recurring, ReadOnly: opts.ReadOnly, MaxFires: opts.MaxFires, TriggerType: triggerType, EventSource: eventSource, DebounceMs: debounceMs, TaskBacklog: opts.TaskBacklog, CreatedAt: time.Now(), NextFire: nextFire}
	a.nextLoop++
	a.loops = append(a.loops, entry)
	a.saveLocked()
	return entry, nil
}

func (a *App) loopList() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.loops) == 0 {
		return "No loops configured. Use LoopCreate to set up a schedule."
	}
	lines := []string{}
	for _, l := range a.loops {
		remain := time.Until(l.NextFire).Round(time.Second)
		if remain < 0 {
			remain = 0
		}
		parts := []string{fmt.Sprintf("#%s [%s] %s", l.ID, l.Status, trim(l.Prompt, 70)), loopTriggerDesc(l), fmt.Sprintf("fires: %d", l.FireCount)}
		if !l.NextFire.IsZero() {
			parts = append(parts, "next: "+remain.String())
		}
		lines = append(lines, strings.Join(parts, " | "))
	}
	return strings.Join(lines, "\n")
}

func (a *App) loopDelete(id, action string) (string, error) {
	if action == "" {
		action = "delete"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.loops {
		if a.loops[i].ID == id {
			switch action {
			case "pause":
				a.loops[i].Status = "paused"
			case "resume":
				a.loops[i].Status = "active"
				if a.loops[i].TriggerType == "" || a.loops[i].TriggerType == "cron" || a.loops[i].TriggerType == "hybrid" {
					if d, err := parseSchedule(a.loops[i].Trigger, time.Now()); err == nil {
						a.loops[i].NextFire = time.Now().Add(d)
					}
				}
			case "delete":
				a.loops = append(a.loops[:i], a.loops[i+1:]...)
			default:
				return "", errors.New("action must be delete, pause, or resume")
			}
			a.saveLocked()
			return fmt.Sprintf("Loop #%s %sd", id, action), nil
		}
	}
	return "", fmt.Errorf("Loop #%s not found", id)
}

func (a *App) handleEvent(event string) {
	if event == "" {
		return
	}
	for _, prompt := range a.fireEventLoops(event, time.Now()) {
		a.submit(prompt)
	}
}

func (a *App) fireEventLoops(event string, now time.Time) []string {
	var prompts []string
	a.mu.Lock()
	changed := false
	for i := 0; i < len(a.loops); i++ {
		l := &a.loops[i]
		if l.Status != "active" || (l.TriggerType != "event" && l.TriggerType != "hybrid") || l.EventSource != event {
			continue
		}
		if l.TriggerType == "hybrid" && !l.LastFire.IsZero() && now.Sub(l.LastFire) < time.Duration(l.DebounceMs)*time.Millisecond {
			continue
		}
		l.FireCount++
		l.LastFire = now
		changed = true
		prompts = append(prompts, loopPrompt(*l, "event "+event))
		stop := !l.Recurring || (l.MaxFires > 0 && l.FireCount >= l.MaxFires)
		if stop {
			a.loops = append(a.loops[:i], a.loops[i+1:]...)
			i--
		}
	}
	if changed {
		a.saveLocked()
	}
	a.mu.Unlock()
	return prompts
}

func (a *App) scheduler() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for range tick.C {
		now := time.Now()
		var prompts []string
		a.mu.Lock()
		changed := false
		for i := 0; i < len(a.loops); i++ {
			l := &a.loops[i]
			if !l.CreatedAt.IsZero() && now.Sub(l.CreatedAt) > loopMaxAge {
				a.loops = append(a.loops[:i], a.loops[i+1:]...)
				i--
				changed = true
				continue
			}
			if l.TaskBacklog && a.pendingTaskCountLocked() == 0 {
				prompts = append(prompts, fmt.Sprintf("Loop #%s auto-deleted: task backlog is empty", l.ID))
				a.loops = append(a.loops[:i], a.loops[i+1:]...)
				i--
				changed = true
				continue
			}
			if l.Status != "active" || (l.TriggerType != "" && l.TriggerType != "cron" && l.TriggerType != "hybrid") || now.Before(l.NextFire) {
				continue
			}
			l.FireCount++
			changed = true
			prompts = append(prompts, loopPrompt(*l, "schedule"))
			stop := !l.Recurring || (l.MaxFires > 0 && l.FireCount >= l.MaxFires)
			if stop {
				a.loops = append(a.loops[:i], a.loops[i+1:]...)
				i--
				continue
			}
			if l.TriggerType == "cron" || l.TriggerType == "hybrid" {
				if d, err := parseSchedule(l.Trigger, now); err == nil {
					l.NextFire = now.Add(d)
				}
			}
		}
		if changed {
			a.saveLocked()
		}
		a.mu.Unlock()
		for _, prompt := range prompts {
			a.submit(prompt)
		}
	}
}

func (a *App) monitorCreate(command, desc, onDone string) (string, error) {
	if strings.TrimSpace(command) == "" {
		return "", errors.New("command is required")
	}
	a.mu.Lock()
	if len(a.monitors) >= maxMon {
		a.mu.Unlock()
		return "", fmt.Errorf("limit reached: %d monitors", maxMon)
	}
	id := strconv.Itoa(a.nextMon)
	a.nextMon++
	ctx, cancel := context.WithCancel(context.Background())
	m := &Monitor{ID: id, Command: command, Description: desc, OnDone: onDone, Status: "running", StartedAt: time.Now(), Cancel: cancel}
	a.monitors[id] = m
	a.mu.Unlock()
	go a.runMonitor(ctx, m)
	return fmt.Sprintf("Monitor #%s started: %s", id, command), nil
}

func (a *App) runMonitor(ctx context.Context, m *Monitor) {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", m.Command)
	cmd.Dir = a.host.CWD
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	a.mu.Lock()
	if current := a.monitors[m.ID]; current != nil {
		current.Cmd = cmd
	}
	a.mu.Unlock()
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		a.finishMonitor(m.ID, "error", err.Error())
		return
	}
	go a.readLines(m.ID, stdout)
	go a.readLines(m.ID, stderr)
	err := cmd.Wait()
	status := "done"
	exit := "exit 0"
	if err != nil {
		status = "failed"
		exit = err.Error()
	}
	if ctx.Err() != nil {
		status = "stopped"
		exit = "stopped"
	}
	a.finishMonitor(m.ID, status, exit)
}

func (a *App) readLines(id string, r io.Reader) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		a.mu.Lock()
		if m := a.monitors[id]; m != nil {
			m.Lines = append(m.Lines, s.Text())
			if len(m.Lines) > 200 {
				m.Lines = m.Lines[len(m.Lines)-200:]
			}
		}
		a.mu.Unlock()
	}
}

func (a *App) finishMonitor(id, status, exit string) {
	a.mu.Lock()
	m := a.monitors[id]
	if m != nil {
		m.Status = status
		m.Exit = exit
		m.EndedAt = time.Now()
	}
	onDone := ""
	if m != nil {
		onDone = m.OnDone
	}
	a.mu.Unlock()
	msg := fmt.Sprintf("Monitor #%s %s: %s", id, status, exit)
	if onDone != "" {
		msg += "\n" + onDone
	}
	a.notify("info", msg)
	for _, prompt := range a.fireEventLoops("monitor:done", time.Now()) {
		a.submit(prompt)
	}
}

func (a *App) monitorList() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.monitors) == 0 {
		return "No monitors running."
	}
	ids := []string{}
	for id := range a.monitors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	lines := []string{}
	for _, id := range ids {
		m := a.monitors[id]
		lines = append(lines, fmt.Sprintf("#%s [%s] %s (uptime: %s, lines: %d)", id, m.Status, trim(m.Command, 70), time.Since(m.StartedAt).Round(time.Second), len(m.Lines)))
	}
	return strings.Join(lines, "\n")
}

func (a *App) monitorOutput(id string, n int) (string, error) {
	if n <= 0 {
		n = 50
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	m := a.monitors[id]
	if m == nil {
		return "", fmt.Errorf("Monitor #%s not found", id)
	}
	start := len(m.Lines) - n
	if start < 0 {
		start = 0
	}
	if len(m.Lines[start:]) == 0 {
		return fmt.Sprintf("Monitor #%s has no captured output.", id), nil
	}
	return strings.Join(m.Lines[start:], "\n"), nil
}

func (a *App) monitorStop(id string) (string, error) {
	a.mu.Lock()
	m := a.monitors[id]
	var cmd *exec.Cmd
	if m != nil {
		cmd = m.Cmd
	}
	a.mu.Unlock()
	if m == nil {
		return "", fmt.Errorf("Monitor #%s not found", id)
	}
	if cmd != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		go func() {
			time.Sleep(5 * time.Second)
			a.mu.Lock()
			stillRunning := a.monitors[id] != nil && a.monitors[id].Status == "running"
			a.mu.Unlock()
			if stillRunning {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}()
	} else if m.Cancel != nil {
		m.Cancel()
	}
	return fmt.Sprintf("Monitor #%s stop requested", id), nil
}

func (a *App) createTask(subject, desc string) Task {
	a.mu.Lock()
	defer a.mu.Unlock()
	t := Task{ID: strconv.Itoa(a.nextTask), Subject: subject, Description: desc, Status: "pending", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	a.nextTask++
	a.tasks = append(a.tasks, t)
	a.saveLocked()
	go a.notify("info", fmt.Sprintf("Task #%s created: %s", t.ID, t.Subject))
	return t
}

func (a *App) taskList(filter string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	lines := []string{}
	for _, t := range a.tasks {
		if filter == "" || t.Status == filter {
			lines = append(lines, fmt.Sprintf("#%s [%s] %s", t.ID, t.Status, t.Subject))
		}
	}
	if len(lines) == 0 {
		return "No tasks found."
	}
	return strings.Join(lines, "\n")
}

func (a *App) taskUpdate(id string, args map[string]any) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.tasks {
		if a.tasks[i].ID == id {
			if v := str(args["status"]); v != "" {
				a.tasks[i].Status = v
			}
			if v := str(args["subject"]); v != "" {
				a.tasks[i].Subject = v
			}
			if v := str(args["description"]); v != "" {
				a.tasks[i].Description = v
			}
			a.tasks[i].UpdatedAt = time.Now()
			a.saveLocked()
			go a.notify("info", fmt.Sprintf("Task #%s updated: %s", id, a.tasks[i].Status))
			return fmt.Sprintf("Task #%s updated", id), nil
		}
	}
	return "", fmt.Errorf("Task #%s not found", id)
}

func (a *App) taskDelete(id string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.tasks {
		if a.tasks[i].ID == id {
			a.tasks = append(a.tasks[:i], a.tasks[i+1:]...)
			a.saveLocked()
			go a.notify("info", fmt.Sprintf("Task #%s deleted", id))
			return fmt.Sprintf("Task #%s deleted", id), nil
		}
	}
	return "", fmt.Errorf("Task #%s not found", id)
}

func (a *App) taskPrune() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	kept := a.tasks[:0]
	pruned := 0
	for _, t := range a.tasks {
		if t.Status == "done" {
			pruned++
			continue
		}
		kept = append(kept, t)
	}
	a.tasks = kept
	a.saveLocked()
	go a.notify("info", fmt.Sprintf("TaskPrune removed %d completed tasks", pruned))
	return fmt.Sprintf("Pruned %d completed tasks", pruned)
}

func (a *App) pendingTaskCountLocked() int {
	count := 0
	for _, t := range a.tasks {
		if t.Status == "pending" || t.Status == "in_progress" {
			count++
		}
	}
	return count
}

func (a *App) load() {
	b, err := os.ReadFile(a.dataPath)
	if err != nil {
		return
	}
	var s State
	if json.Unmarshal(b, &s) != nil {
		return
	}
	a.loops = s.Loops
	a.tasks = s.Tasks
	if s.NextLoop > 0 {
		a.nextLoop = s.NextLoop
	}
	if s.NextTask > 0 {
		a.nextTask = s.NextTask
	}
}
func (a *App) saveLocked() {
	_ = os.MkdirAll(filepath.Dir(a.dataPath), 0755)
	b, _ := json.MarshalIndent(State{Loops: a.loops, Tasks: a.tasks, NextLoop: a.nextLoop, NextTask: a.nextTask}, "", "  ")
	_ = os.WriteFile(a.dataPath, b, 0644)
}
func (a *App) shutdown() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, m := range a.monitors {
		if m.Cancel != nil {
			m.Cancel()
		}
	}
	a.saveLocked()
}

func (a *App) send(f Frame) {
	a.outMu.Lock()
	defer a.outMu.Unlock()
	b, _ := json.Marshal(f)
	fmt.Fprintln(os.Stdout, string(b))
}
func (a *App) notify(level, msg string) {
	a.send(Frame{"type": "notify", "level": level, "message": msg})
}
func (a *App) submit(text string) {
	a.send(Frame{"type": "submit", "text": text})
}
func (a *App) toolResult(id, text string, isErr bool) {
	f := Frame{"type": "tool_result", "id": id, "content": []Frame{{"type": "text", "text": text}}}
	if isErr {
		f["is_error"] = true
	}
	a.send(f)
}
func (a *App) cmdDisplay(id, text string) {
	a.send(Frame{"type": "command_response", "id": id, "action": "display", "display": text})
}
func (a *App) cmdPanel(id, panelID, title string, lines []string, footer string) {
	a.send(Frame{"type": "command_response", "id": id, "action": "open_panel", "open_panel": Frame{"id": panelID, "title": title, "lines": lines, "footer": footer}})
}

func (a *App) openLoopsPanel(cmdID string) {
	a.ensurePanel("zot-loop-loops")
	lines, footer := a.loopPanelLines()
	a.cmdPanel(cmdID, "zot-loop-loops", "Loops", lines, footer)
}

func (a *App) openTasksPanel(cmdID string) {
	a.ensurePanel("zot-loop-tasks")
	lines, footer := a.taskPanelLines()
	a.cmdPanel(cmdID, "zot-loop-tasks", "Tasks", lines, footer)
}

func (a *App) ensurePanel(panelID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.panels == nil {
		a.panels = map[string]int{}
	}
}

func (a *App) panelKey(panelID, key, text string) {
	switch panelID {
	case "zot-loop-loops":
		a.loopPanelKey(key, text)
	case "zot-loop-tasks":
		a.taskPanelKey(key, text)
	}
}

func (a *App) panelClose(panelID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.panels, panelID)
}

func (a *App) renderPanel(panelID, title string, lines []string, footer string) {
	a.send(Frame{"type": "panel_render", "panel_id": panelID, "title": title, "lines": lines, "footer": footer})
}

func (a *App) loopPanelLines() ([]string, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.panels == nil {
		a.panels = map[string]int{}
	}
	if len(a.loops) == 0 {
		return []string{"No loops configured.", "", "Type /loop 5m check status to add one."}, "esc close"
	}
	sel := clamp(a.panels["zot-loop-loops"], len(a.loops))
	a.panels["zot-loop-loops"] = sel
	lines := make([]string, 0, len(a.loops)+4)
	for i, l := range a.loops {
		cursor := "  "
		if i == sel {
			cursor = "> "
		}
		remain := "event"
		if !l.NextFire.IsZero() {
			d := time.Until(l.NextFire).Round(time.Second)
			if d < 0 {
				d = 0
			}
			remain = d.String()
		}
		lines = append(lines, fmt.Sprintf("%s#%s [%s] %s", cursor, l.ID, l.Status, trim(l.Prompt, 82)))
		lines = append(lines, fmt.Sprintf("    %s | fires %d | next %s", loopTriggerDesc(l), l.FireCount, remain))
	}
	return lines, "↑/↓ select  enter/r run now  p pause/resume  d delete  esc close"
}

func (a *App) taskPanelLines() ([]string, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.panels == nil {
		a.panels = map[string]int{}
	}
	if len(a.tasks) == 0 {
		return []string{"No tasks configured.", "", "Type /tasks New task to add one."}, "esc close"
	}
	sel := clamp(a.panels["zot-loop-tasks"], len(a.tasks))
	a.panels["zot-loop-tasks"] = sel
	lines := make([]string, 0, len(a.tasks))
	for i, t := range a.tasks {
		cursor := "  "
		if i == sel {
			cursor = "> "
		}
		lines = append(lines, fmt.Sprintf("%s#%s [%s] %s", cursor, t.ID, t.Status, trim(t.Subject, 90)))
		if t.Description != "" {
			lines = append(lines, "    "+trim(t.Description, 96))
		}
	}
	return lines, "↑/↓ select  enter ask agent  s start  x done  d delete  esc close"
}

func (a *App) loopPanelKey(key, text string) {
	if key == "rune" {
		key = text
	}
	var prompt string
	a.mu.Lock()
	if a.panels == nil {
		a.panels = map[string]int{}
	}
	switch key {
	case "up":
		a.panels["zot-loop-loops"] = clamp(a.panels["zot-loop-loops"]-1, len(a.loops))
	case "down":
		a.panels["zot-loop-loops"] = clamp(a.panels["zot-loop-loops"]+1, len(a.loops))
	case "enter", "r":
		if i, ok := selectedLoopIndexLocked(a); ok {
			a.loops[i].FireCount++
			a.loops[i].LastFire = time.Now()
			prompt = loopPrompt(a.loops[i], "manual panel run")
		}
	case "p":
		if i, ok := selectedLoopIndexLocked(a); ok {
			if a.loops[i].Status == "paused" {
				a.loops[i].Status = "active"
			} else {
				a.loops[i].Status = "paused"
			}
		}
	case "d", "delete", "backspace":
		if i, ok := selectedLoopIndexLocked(a); ok {
			a.loops = append(a.loops[:i], a.loops[i+1:]...)
			a.panels["zot-loop-loops"] = clamp(a.panels["zot-loop-loops"], len(a.loops))
		}
	}
	a.saveLocked()
	a.mu.Unlock()
	if prompt != "" {
		a.submit(prompt)
	}
	lines, footer := a.loopPanelLines()
	a.renderPanel("zot-loop-loops", "Loops", lines, footer)
}

func (a *App) taskPanelKey(key, text string) {
	if key == "rune" {
		key = text
	}
	var prompt string
	a.mu.Lock()
	if a.panels == nil {
		a.panels = map[string]int{}
	}
	switch key {
	case "up":
		a.panels["zot-loop-tasks"] = clamp(a.panels["zot-loop-tasks"]-1, len(a.tasks))
	case "down":
		a.panels["zot-loop-tasks"] = clamp(a.panels["zot-loop-tasks"]+1, len(a.tasks))
	case "enter":
		if t, ok := selectedTaskLocked(a); ok {
			prompt = fmt.Sprintf("[zot-loop] Work on task #%s: %s\n\n%s", t.ID, t.Subject, t.Description)
		}
	case "s":
		if i, ok := selectedTaskIndexLocked(a); ok {
			a.tasks[i].Status = "in_progress"
			a.tasks[i].UpdatedAt = time.Now()
		}
	case "x":
		if i, ok := selectedTaskIndexLocked(a); ok {
			a.tasks[i].Status = "done"
			a.tasks[i].UpdatedAt = time.Now()
		}
	case "d", "delete", "backspace":
		if i, ok := selectedTaskIndexLocked(a); ok {
			a.tasks = append(a.tasks[:i], a.tasks[i+1:]...)
			a.panels["zot-loop-tasks"] = clamp(a.panels["zot-loop-tasks"], len(a.tasks))
		}
	}
	a.saveLocked()
	a.mu.Unlock()
	if prompt != "" {
		a.submit(prompt)
	}
	lines, footer := a.taskPanelLines()
	a.renderPanel("zot-loop-tasks", "Tasks", lines, footer)
}

func selectedLoopIndexLocked(a *App) (int, bool) {
	if len(a.loops) == 0 {
		return 0, false
	}
	return clamp(a.panels["zot-loop-loops"], len(a.loops)), true
}

func selectedLoopLocked(a *App) (Loop, bool) {
	if i, ok := selectedLoopIndexLocked(a); ok {
		return a.loops[i], true
	}
	return Loop{}, false
}

func selectedTaskIndexLocked(a *App) (int, bool) {
	if len(a.tasks) == 0 {
		return 0, false
	}
	return clamp(a.panels["zot-loop-tasks"], len(a.tasks)), true
}

func selectedTaskLocked(a *App) (Task, bool) {
	if i, ok := selectedTaskIndexLocked(a); ok {
		return a.tasks[i], true
	}
	return Task{}, false
}

func clamp(i, n int) int {
	if n <= 0 {
		return 0
	}
	if i < 0 {
		return 0
	}
	if i >= n {
		return n - 1
	}
	return i
}

func inferTriggerType(trigger string) string {
	trimmed := strings.TrimSpace(strings.ToLower(trigger))
	if strings.HasPrefix(trimmed, "event:") {
		return "event"
	}
	if strings.Contains(trimmed, "hybrid") {
		return "hybrid"
	}
	if _, err := parseSchedule(trimmed, time.Now()); err == nil {
		return "cron"
	}
	return "event"
}

func loopPrompt(l Loop, cause string) string {
	mode := ""
	if l.ReadOnly {
		mode = "\nUse read-only tools only unless the user explicitly asks for changes."
	}
	return fmt.Sprintf("[zot-loop] Loop #%s fired via %s.\n\n%s%s", l.ID, cause, l.Prompt, mode)
}

func loopTriggerDesc(l Loop) string {
	t := l.TriggerType
	if t == "" {
		t = inferTriggerType(l.Trigger)
	}
	switch t {
	case "event":
		return "event: " + l.EventSource
	case "hybrid":
		return fmt.Sprintf("hybrid: every %s + event %s", l.Trigger, l.EventSource)
	default:
		return "every " + l.Trigger
	}
}

func parseSchedule(s string, from time.Time) (time.Duration, error) {
	if d, err := parseDuration(s); err == nil {
		return d, nil
	}
	parts := strings.Fields(s)
	if len(parts) != 5 {
		return 0, fmt.Errorf("invalid interval %q, use forms like 30s, 5m, 2h, 1d, or 5-field cron", s)
	}
	minutes, err := parseCronField(parts[0], 0, 59)
	if err != nil {
		return 0, fmt.Errorf("invalid cron minute: %w", err)
	}
	hours, err := parseCronField(parts[1], 0, 23)
	if err != nil {
		return 0, fmt.Errorf("invalid cron hour: %w", err)
	}
	dom, err := parseCronField(parts[2], 1, 31)
	if err != nil {
		return 0, fmt.Errorf("invalid cron day-of-month: %w", err)
	}
	months, err := parseCronField(parts[3], 1, 12)
	if err != nil {
		return 0, fmt.Errorf("invalid cron month: %w", err)
	}
	dow, err := parseCronField(parts[4], 0, 7)
	if err != nil {
		return 0, fmt.Errorf("invalid cron day-of-week: %w", err)
	}
	candidate := from.Truncate(time.Minute).Add(time.Minute)
	limit := from.AddDate(5, 0, 0)
	for candidate.Before(limit) {
		weekday := int(candidate.Weekday())
		if minutes[candidate.Minute()] && hours[candidate.Hour()] && dom[candidate.Day()] && months[int(candidate.Month())] && (dow[weekday] || (weekday == 0 && dow[7])) {
			return candidate.Sub(from), nil
		}
		candidate = candidate.Add(time.Minute)
	}
	return 0, fmt.Errorf("cron %q has no fire time within five years", s)
}

func parseCronField(field string, min int, max int) (map[int]bool, error) {
	out := map[int]bool{}
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, errors.New("empty field part")
		}
		step := 1
		if strings.Contains(part, "/") {
			pieces := strings.Split(part, "/")
			if len(pieces) != 2 {
				return nil, fmt.Errorf("bad step %q", part)
			}
			part = pieces[0]
			parsed, err := strconv.Atoi(pieces[1])
			if err != nil || parsed <= 0 {
				return nil, fmt.Errorf("bad step %q", pieces[1])
			}
			step = parsed
		}
		start, end := min, max
		if part != "*" {
			if strings.Contains(part, "-") {
				pieces := strings.Split(part, "-")
				if len(pieces) != 2 {
					return nil, fmt.Errorf("bad range %q", part)
				}
				var err error
				start, err = strconv.Atoi(pieces[0])
				if err != nil {
					return nil, err
				}
				end, err = strconv.Atoi(pieces[1])
				if err != nil {
					return nil, err
				}
			} else {
				parsed, err := strconv.Atoi(part)
				if err != nil {
					return nil, err
				}
				start, end = parsed, parsed
			}
		}
		if start < min || end > max || start > end {
			return nil, fmt.Errorf("value outside %d-%d", min, max)
		}
		for i := start; i <= end; i += step {
			out[i] = true
		}
	}
	return out, nil
}

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, errors.New("trigger is required")
	}
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid interval %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid interval %q, use forms like 30s, 5m, 2h, 1d", s)
	}
	return d, nil
}
func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func boolv(v any) bool { b, _ := v.(bool); return b }
func intv(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}
func has(m map[string]any, k string) bool { _, ok := m[k]; return ok }
func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}
func linesOrEmpty(text string) []string {
	if strings.TrimSpace(text) == "" {
		return []string{"(empty)"}
	}
	return strings.Split(text, "\n")
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
func logf(format string, args ...any) { fmt.Fprintf(os.Stderr, "[zot-loop] "+format+"\n", args...) }
