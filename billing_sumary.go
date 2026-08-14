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

// calculateBillingSummary turns deduplicated, validated events into the billing summary.
// The caller passes in the counters collected while parsing; calculateBillingSummary adds
// the unmatched stops it finds.
func calculateBillingSummary(events []VMEvent, stats BillingStats) BillingSummary {
	// Group by instance id: sessions belong to an instance, and each
	// instance's timeline can be paired independently of the others.
	var instanceGroups = make(map[string][]VMEvent)

	for _, event := range events {
		instanceGroups[event.InstanceId] = append(instanceGroups[event.InstanceId], event)
	}

	// Group by project and flavor, accumulate billed hours.
	projectFlavorMap, unmatchedStops := groupByProjectAndFlavor(instanceGroups)
	stats.UnmatchedStops = unmatchedStops

	projectSummaries, total := createProjectSummaries(projectFlavorMap)

	return BillingSummary{
		Period:         BillingPeriod{Start: periodStart, End: periodEnd},
		Projects:       projectSummaries,
		TotalCostCents: total,
		Counters:       stats,
	}
}

func groupByProjectAndFlavor(instanceGroups map[string][]VMEvent) (map[string]map[string]uint, uint) {
	var projectFlavorMap = make(map[string]map[string]uint) // project -> flavor -> billed hours
	var unmatchedStops uint

	for _, group := range instanceGroups {
		sessions, us := pairSessions(group)
		unmatchedStops += us

		for _, session := range sessions {
			if _, ok := projectFlavorMap[session.ProjectId]; !ok {
				projectFlavorMap[session.ProjectId] = make(map[string]uint)
			}
			projectFlavorMap[session.ProjectId][session.Flavor] += calculateBilledHoursFor(session)
		}
	}
	return projectFlavorMap, unmatchedStops
}

func pairSessions(group []VMEvent) ([]Session, uint) {
	sort.Slice(group, func(i, j int) bool {
		return group[i].OccurredAt.Before(group[j].OccurredAt)
	})

	return doPairSessions(group, periodEnd)
}

// doPairSessions pairs the sorted events of a single instance into sessions and
// counts stops that have no matching start. A start still open at the end of
// the events is billed until periodEnd.
func doPairSessions(events []VMEvent, periodEnd time.Time) ([]Session, uint) {
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

func createProjectSummaries(projectFlavorMap map[string]map[string]uint) (map[string]ProjectSummary, uint) {
	projects := map[string]ProjectSummary{}
	var total uint

	for project, byFlavor := range projectFlavorMap {
		var cost uint
		for flavor, h := range byFlavor {
			cost += calculateCostFor(flavor, h)
		}
		projects[project] = ProjectSummary{BilledHours: byFlavor, CostCents: cost}
		total += cost
	}
	return projects, total
}

// calculateBilledHoursFor returns the started hours of a session: a 25-minute session is
// 1 hour, 2.5 hours are 3 hours, an exact 2 hours stay 2.
func calculateBilledHoursFor(session Session) uint {
	duration := session.End.Sub(session.Start)
	if duration <= 0 {
		return 0
	}
	// Integer ceiling division, so no float rounding touches the money.
	return uint((duration + time.Hour - 1) / time.Hour)
}

// calculateCostFor prices billed hours of a flavor. An unknown flavor costs
// nothing; parse rejects those before they get here.
func calculateCostFor(flavor string, hours uint) uint {
	return flavorRates[flavor] * hours
}
