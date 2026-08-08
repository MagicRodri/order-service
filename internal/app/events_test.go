package app

import "testing"

func TestChannelForRoutesEveryPublishedEvent(t *testing.T) {
	cases := map[string]string{
		"OrderCreated":   channelOrderLifecycle,
		"OrderCancelled": channelOrderSettlement,
	}
	for eventType, want := range cases {
		if got := channelFor(eventType); got != want {
			t.Errorf("channelFor(%q) = %q, want %q", eventType, got, want)
		}
	}
}

// An unmapped type must still route somewhere valid: an empty channel would
// produce the topic `business..events`.
func TestChannelForFallsBackToAggregateType(t *testing.T) {
	if got := channelFor("SomethingAddedLater"); got != aggregateTypeOrder {
		t.Errorf("channelFor(unknown) = %q, want %q", got, aggregateTypeOrder)
	}
	if channelFor("SomethingAddedLater") == "" {
		t.Error("fallback channel must never be empty")
	}
}
