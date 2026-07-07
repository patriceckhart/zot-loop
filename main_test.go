package main

import (
	"testing"
	"time"
)

func TestParseScheduleDuration(t *testing.T) {
	d, err := parseSchedule("5m", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if d != 5*time.Minute {
		t.Fatalf("expected 5m, got %s", d)
	}
}

func TestParseScheduleCron(t *testing.T) {
	from := time.Date(2026, 7, 3, 8, 58, 30, 0, time.UTC)
	d, err := parseSchedule("0 9 * * 1-5", from)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Minute + 30*time.Second
	if d != want {
		t.Fatalf("expected %s, got %s", want, d)
	}
}

func TestEventLoopMaxFires(t *testing.T) {
	a := &App{monitors: map[string]*Monitor{}, nextLoop: 1, nextTask: 1, nextMon: 1, dataPath: t.TempDir() + "/state.json"}
	_, err := a.createLoopWithOptions(LoopOptions{Trigger: "tool_call", Prompt: "check", Recurring: true, TriggerType: "event", MaxFires: 1})
	if err != nil {
		t.Fatal(err)
	}
	notes := a.fireEventLoops("tool_call", time.Now())
	if len(notes) != 1 {
		t.Fatalf("expected 1 note, got %d", len(notes))
	}
	if len(a.loops) != 0 {
		t.Fatalf("expected loop removed after max fire")
	}
}

func TestTaskPrune(t *testing.T) {
	a := &App{monitors: map[string]*Monitor{}, nextLoop: 1, nextTask: 1, nextMon: 1, dataPath: t.TempDir() + "/state.json"}
	a.createTask("a", "")
	a.createTask("b", "")
	_, err := a.taskUpdate("1", map[string]any{"status": "done"})
	if err != nil {
		t.Fatal(err)
	}
	a.taskPrune()
	if len(a.tasks) != 1 || a.tasks[0].ID != "2" {
		t.Fatalf("unexpected tasks: %#v", a.tasks)
	}
}
