package main

import (
	"testing"
	"time"
)

// at returns a July 2026 timestamp at the given hour, for readable test events.
func at(hour int) time.Time {
	return time.Date(2026, 7, 1, hour, 0, 0, 0, time.UTC)
}

func TestPairSessions(t *testing.T) {
	// Test cases for the pairSessions function
	t.Run("Valid start and stop events", func(t *testing.T) {
		events := []VMEvent{
			{EventId: "1", OccurredAt: at(10), ProjectId: "proj1", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
			{EventId: "2", OccurredAt: at(12), ProjectId: "proj1", InstanceId: "inst1", Type: Stop, Flavor: "s1.medium"},
			{EventId: "3", OccurredAt: at(14), ProjectId: "proj1", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
			{EventId: "4", OccurredAt: at(16), ProjectId: "proj1", InstanceId: "inst1", Type: Stop, Flavor: "s1.medium"},
		}
		sessions, unmatchedStops := doPairSessions(events, periodEnd)
		if len(sessions) != 2 {
			t.Errorf("Expected 2 sessions, got %d", len(sessions))
		}
		if unmatchedStops != 0 {
			t.Errorf("Expected 0 unmatched stops, got %d", unmatchedStops)
		}
	})

	t.Run("Unmatched stop event", func(t *testing.T) {
		events := []VMEvent{
			{EventId: "1", OccurredAt: at(10), ProjectId: "proj1", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
			{EventId: "2", OccurredAt: at(12), ProjectId: "proj1", InstanceId: "inst1", Type: Stop, Flavor: "s1.medium"},
			{EventId: "3", OccurredAt: at(14), ProjectId: "proj1", InstanceId: "inst1", Type: Stop, Flavor: "s1.medium"},
		}
		sessions, unmatchedStops := doPairSessions(events, periodEnd)
		if len(sessions) != 1 {
			t.Errorf("Expected 1 session, got %d", len(sessions))
		}
		if unmatchedStops != 1 {
			t.Errorf("Expected 1 unmatched stop, got %d", unmatchedStops)
		}
	})

	t.Run("Start without stop runs until end of period", func(t *testing.T) {
		events := []VMEvent{
			{EventId: "1", OccurredAt: at(10), ProjectId: "proj1", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
		}
		sessions, unmatchedStops := doPairSessions(events, periodEnd)
		if len(sessions) != 1 {
			t.Fatalf("Expected 1 session, got %d", len(sessions))
		}
		if !sessions[0].End.Equal(periodEnd) {
			t.Errorf("Expected session to end at %v, got %v", periodEnd, sessions[0].End)
		}
		if unmatchedStops != 0 {
			t.Errorf("Expected 0 unmatched stops, got %d", unmatchedStops)
		}
	})

	t.Run("Repeated start keeps the earlier one", func(t *testing.T) {
		events := []VMEvent{
			{EventId: "1", OccurredAt: at(10), ProjectId: "proj1", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
			{EventId: "2", OccurredAt: at(11), ProjectId: "proj1", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
			{EventId: "3", OccurredAt: at(12), ProjectId: "proj1", InstanceId: "inst1", Type: Stop, Flavor: "s1.medium"},
		}
		sessions, _ := doPairSessions(events, periodEnd)
		if len(sessions) != 1 {
			t.Fatalf("Expected 1 session, got %d", len(sessions))
		}
		if !sessions[0].Start.Equal(at(10)) {
			t.Errorf("Expected session to start at %v, got %v", at(10), sessions[0].Start)
		}
	})
}

func TestBilledHours(t *testing.T) {
	base := at(10)
	cases := []struct {
		d    time.Duration
		want uint
	}{
		{25 * time.Minute, 1},
		{time.Hour, 1},
		{2 * time.Hour, 2},
		{150 * time.Minute, 3},
		{0, 0},
		{time.Second, 1},
		{744 * time.Hour, 744},
		{744*time.Hour - time.Nanosecond, 744},
		{time.Hour + time.Nanosecond, 2},
	}
	for _, c := range cases {
		s := Session{Start: base, End: base.Add(c.d)}
		if got := calculateBilledHoursFor(s); got != c.want {
			t.Errorf("d=%v: got %d want %d", c.d, got, c.want)
		}
	}
}

func TestCalculateCost(t *testing.T) {
	cases := []struct {
		flavor string
		hours  uint
		want   uint
	}{
		{"s1.small", 1, 5},
		{"s1.medium", 2, 20},
		{"s1.large", 3, 60},
		{"g1.xlarge", 4, 600},
		{"unknown", 5, 0},
	}
	for _, c := range cases {
		if got := calculateCostFor(c.flavor, c.hours); got != c.want {
			t.Errorf("flavor=%s hours=%d: got %d want %d", c.flavor, c.hours, got, c.want)
		}
	}
}

// TestSummarize checks the whole aggregation on a hand-computable set of
// events, including out-of-order input and an instance that never stopped.
func TestSummarize(t *testing.T) {
	// inst1: 10:00 -> 12:30 is 3 billed hours of s1.medium = 30 cents,
	//        fed in reverse order to prove the sort works.
	// inst2: starts at 10:00 and never stops, so it runs to the end of the
	//        period: 744 - 10 = 734 billed hours of s1.small = 3670 cents.
	// inst3: a stop with no start, billed to nobody but counted.
	events := []VMEvent{
		{EventId: "4", OccurredAt: at(12), ProjectId: "p-b", InstanceId: "inst3", Type: Stop, Flavor: "s1.small"},
		{EventId: "2", OccurredAt: at(10).Add(150 * time.Minute), ProjectId: "p-a", InstanceId: "inst1", Type: Stop, Flavor: "s1.medium"},
		{EventId: "3", OccurredAt: at(10), ProjectId: "p-b", InstanceId: "inst2", Type: Start, Flavor: "s1.small"},
		{EventId: "1", OccurredAt: at(10), ProjectId: "p-a", InstanceId: "inst1", Type: Start, Flavor: "s1.medium"},
	}

	summary := calculateBillingSummary(events, BillingStats{InvalidLines: 2, DuplicateEvents: 1})

	if got := summary.Projects["p-a"].BilledHours["s1.medium"]; got != 3 {
		t.Errorf("Expected 3 billed hours for p-a, got %d", got)
	}
	if got := summary.Projects["p-a"].CostCents; got != 30 {
		t.Errorf("Expected 30 cents for p-a, got %d", got)
	}
	if got := summary.Projects["p-b"].BilledHours["s1.small"]; got != 734 {
		t.Errorf("Expected 734 billed hours for p-b, got %d", got)
	}
	if got := summary.Projects["p-b"].CostCents; got != 3670 {
		t.Errorf("Expected 3670 cents for p-b, got %d", got)
	}
	if summary.TotalCostCents != 3700 {
		t.Errorf("Expected 3700 cents in total, got %d", summary.TotalCostCents)
	}

	want := BillingStats{InvalidLines: 2, DuplicateEvents: 1, UnmatchedStops: 1}
	if summary.Counters != want {
		t.Errorf("Expected counters %+v, got %+v", want, summary.Counters)
	}
}
