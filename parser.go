package main

import (
	"bufio"
	"encoding/json"
	"io"
)

// maxLineBytes cap
const maxLineBytes = 1024 * 1024

// parse reads one JSON event per line and returns the valid, deduplicated
// events. Lines that are not usable events are counted, never fatal.
func parse(reader io.Reader) ([]VMEvent, BillingStats) {
	var events []VMEvent
	var stats BillingStats

	seen := make(map[string]struct{})

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, maxLineBytes)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // blank lines are not broken events
		}

		var event VMEvent
		// Unmarshal rejects broken JSON and unparseable timestamps for us.
		if err := json.Unmarshal(line, &event); err != nil {
			stats.InvalidLines++
			continue
		}
		if !isValid(event) {
			stats.InvalidLines++
			continue
		}

		// The queue delivers at-least-once: the same event_id is the same
		// event, no matter how often it arrives.
		if _, duplicate := seen[event.EventId]; duplicate {
			stats.DuplicateEvents++
			continue
		}
		seen[event.EventId] = struct{}{}

		events = append(events, event)
	}

	// A read error (or a line over the cap) leaves the rest of the file
	// unread; count it rather than crashing.
	if scanner.Err() != nil {
		stats.InvalidLines++
	}

	return events, stats
}

// isValid reports whether an event carries everything billing needs.
func isValid(event VMEvent) bool {
	if event.EventId == "" || event.ProjectId == "" || event.InstanceId == "" {
		return false
	}
	if event.OccurredAt.IsZero() {
		return false
	}
	if event.Type != Start && event.Type != Stop {
		return false
	}
	if _, known := flavorRates[event.Flavor]; !known {
		return false
	}
	return true
}
