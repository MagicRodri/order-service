package config

import "testing"

func TestSelectorPrefersExplicitTopics(t *testing.T) {
	s := TopicSelector{Topics: []string{"a", "b"}, Pattern: `^ignored\..*`}
	if s.IsPattern() {
		t.Error("an explicit topic list must not be consumed as a pattern")
	}
	if got := s.Values(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Values() = %v, want [a b]", got)
	}
}

func TestSelectorFallsBackToPattern(t *testing.T) {
	s := TopicSelector{Pattern: `^business\.order\..*`}
	if !s.IsPattern() {
		t.Error("a selector without topics must be consumed as a pattern")
	}
	if got := s.Values(); len(got) != 1 || got[0] != `^business\.order\..*` {
		t.Errorf("Values() = %v, want the pattern", got)
	}
}

func TestSelectorValidation(t *testing.T) {
	if err := (TopicSelector{Pattern: `^ok\..*`}).validate("X"); err != nil {
		t.Errorf("valid pattern rejected: %v", err)
	}
	if err := (TopicSelector{Topics: []string{"a"}}).validate("X"); err != nil {
		t.Errorf("explicit topics need no pattern: %v", err)
	}
	if err := (TopicSelector{}).validate("X"); err == nil {
		t.Error("an empty selector must be rejected")
	}
	if err := (TopicSelector{Pattern: "^(unclosed"}).validate("X"); err == nil {
		t.Error("an invalid regular expression must be rejected")
	}
}

func TestSplitListTrimsAndDropsBlanks(t *testing.T) {
	got := splitList(" a , ,b,, c ")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("splitList() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("splitList()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if splitList("") != nil {
		t.Error("an empty string must yield no topics, so the pattern is used")
	}
}
