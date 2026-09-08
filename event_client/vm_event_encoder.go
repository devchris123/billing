package event_client

import (
	"encoding/json"
	"fmt"
	"strings"

	ing "github.com/ghaering/core-api-task/event_ingestion"
)

type VmEventJsonEncoder struct{}

func (e *VmEventJsonEncoder) Decode(value []byte) (ing.VmEvent, error) {
	var res ing.VmEvent
	err := json.Unmarshal(value, &res)
	if err != nil {
		return ing.VmEvent{}, err
	}
	if err := validateVmEvent(res); err != nil {
		return ing.VmEvent{}, err
	}
	return res, nil
}

func (e *VmEventJsonEncoder) Encode(vmEvent ing.VmEvent) ([]byte, error) {
	return json.Marshal(vmEvent)
}

func validateVmEvent(event ing.VmEvent) error {
	var missingFields []string
	if strings.TrimSpace(event.EventId) == "" {
		missingFields = append(missingFields, "event_id")
	}
	if strings.TrimSpace(event.Flavour) == "" {
		missingFields = append(missingFields, "flavor")
	}
	if strings.TrimSpace(event.InstanceId) == "" {
		missingFields = append(missingFields, "instance_id")
	}
	if event.OccurredAt.IsZero() {
		missingFields = append(missingFields, "occurred_at")
	}
	if strings.TrimSpace(event.ProjectId) == "" {
		missingFields = append(missingFields, "project_id")
	}
	if strings.TrimSpace(event.EventType) == "" {
		missingFields = append(missingFields, "type")
	}
	if len(missingFields) > 0 {
		return fmt.Errorf("missing required event fields: %s", strings.Join(missingFields, ", "))
	}
	return nil
}
