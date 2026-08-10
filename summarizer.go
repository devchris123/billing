package main

import (
	"sort"
	"time"
)

// The billing period: July 2026, start inclusive, end exclusive.
var (
	periodStart = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	periodEnd   = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
)

// flavorRates maps a flavor to its price in euro cents per started hour.
var flavorRates = map[string]uint{
	"s1.small":  5,
	"s1.medium": 10,
	"s1.large":  20,
	"g1.xlarge": 150,
}

type VMEvent struct {
	EventId    string    `json:"event_id"`
	OccurredAt time.Time `json:"occurred_at"`
	ProjectId  string    `json:"project_id"`
	InstanceId string    `json:"instance_id"`
	Type       EventType `json:"type"`
	Flavor     string    `json:"flavor"`
}

// Session is one billable run of an instance. An instance that never stopped
// gets End set to the end of the billing period.
type Session struct {
	ProjectId string
	Flavor    string
	Start     time.Time
	End       time.Time
}

type ProjectSummary struct {
	BilledHours map[string]uint `json:"billed_hours"`
	CostCents   uint            `json:"cost_cents"`
}

type BillingPeriod struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type BillingStats struct {
	InvalidLines    uint `json:"invalid_lines"`
	DuplicateEvents uint `json:"duplicate_events"`
	UnmatchedStops  uint `json:"unmatched_stops"`
}

type BillingSummary struct {
	Period         BillingPeriod             `json:"period"`
	Projects       map[string]ProjectSummary `json:"projects"`
	TotalCostCents uint                      `json:"total_cost_cents"`
	Counters       BillingStats              `json:"counters"`
}

type EventType string

const (
	Start EventType = "instance.start"
	Stop  EventType = "instance.stop"
)

// summarize turns deduplicated, validated events into the billing summary.
// The caller passes in the counters collected while parsing; summarize adds
// the unmatched stops it finds.
func summarize(events []VMEvent, stats BillingStats) BillingSummary {
	// Group by instance id: sessions belong to an instance, and each
	// instance's timeline can be paired independently of the others.
	var instanceGroups = make(map[string][]VMEvent)

	for _, event := range events {
		instanceGroups[event.InstanceId] = append(instanceGroups[event.InstanceId], event)
	}

	// Group by project and flavor, accumulate billed hours.
	var projectFlavorMap = make(map[string]map[string]uint) // project -> flavor -> billed hours

	for _, group := range instanceGroups {
		sort.Slice(group, func(i, j int) bool {
			return group[i].OccurredAt.Before(group[j].OccurredAt)
		})

		sessions, unmatchedStops := pairSessions(group, periodEnd)
		stats.UnmatchedStops += unmatchedStops

		for _, session := range sessions {
			if _, ok := projectFlavorMap[session.ProjectId]; !ok {
				projectFlavorMap[session.ProjectId] = make(map[string]uint)
			}
			projectFlavorMap[session.ProjectId][session.Flavor] += billedHours(session)
		}
	}

	projects := map[string]ProjectSummary{}
	var total uint
	for project, byFlavor := range projectFlavorMap {
		var cost uint
		for flavor, h := range byFlavor {
			cost += calculateCost(flavor, h)
		}
		projects[project] = ProjectSummary{BilledHours: byFlavor, CostCents: cost}
		total += cost
	}

	return BillingSummary{
		Period:         BillingPeriod{Start: periodStart, End: periodEnd},
		Projects:       projects,
		TotalCostCents: total,
		Counters:       stats,
	}
}

// pairSessions pairs the sorted events of a single instance into sessions and
// counts stops that have no matching start. A start still open at the end of
// the events is billed until periodEnd.
func pairSessions(events []VMEvent, periodEnd time.Time) ([]Session, uint) {
	var sessions []Session
	var openStart *VMEvent
	var unmatchedStops uint

	newSession := func(start VMEvent, end time.Time) Session {
		return Session{
			ProjectId: start.ProjectId,
			Flavor:    start.Flavor,
			Start:     start.OccurredAt,
			End:       end,
		}
	}

	for _, event := range events {
		switch event.Type {
		case Start:
			// A start while a session is already open is treated as a
			// redelivery artifact: the earlier start wins.
			if openStart == nil {
				started := event
				openStart = &started
			}
		case Stop:
			if openStart == nil {
				unmatchedStops++
				continue
			}
			sessions = append(sessions, newSession(*openStart, event.OccurredAt))
			openStart = nil
		}
	}

	// An instance with a start but no stop is still running: bill it until
	// the end of the billing period.
	if openStart != nil {
		sessions = append(sessions, newSession(*openStart, periodEnd))
	}

	return sessions, unmatchedStops
}

// billedHours returns the started hours of a session: a 25-minute session is
// 1 hour, 2.5 hours are 3 hours, an exact 2 hours stay 2.
func billedHours(session Session) uint {
	duration := session.End.Sub(session.Start)
	if duration <= 0 {
		return 0
	}
	// Integer ceiling division, so no float rounding touches the money.
	return uint((duration + time.Hour - 1) / time.Hour)
}

// calculateCost prices billed hours of a flavor. An unknown flavor costs
// nothing; parse rejects those before they get here.
func calculateCost(flavor string, hours uint) uint {
	return flavorRates[flavor] * hours
}
