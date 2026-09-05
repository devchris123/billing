package session

import (
	ing "github.com/ghaering/core-api-task/event_ingestion"
)

type Sessionizer struct {
	ingestor *ing.Ingestor
}

func NewSessionizer(ingestor *ing.Ingestor) *Sessionizer {
	return &Sessionizer{
		ingestor: ingestor,
	}
}
