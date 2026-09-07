package event_client

import (
	"encoding/json"

	ing "github.com/ghaering/core-api-task/event_ingestion"
)

type VmEventJsonEncoder struct{}

func (e *VmEventJsonEncoder) Decode(value []byte) (ing.VmEvent, error) {
	var res ing.VmEvent
	err := json.Unmarshal(value, &res)
	if err != nil {
		return ing.VmEvent{}, err
	}
	return res, nil
}

func (e *VmEventJsonEncoder) Encode(vmEvent ing.VmEvent) ([]byte, error) {
	return json.Marshal(vmEvent)
}
