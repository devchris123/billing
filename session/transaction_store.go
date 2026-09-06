package session

import "context"

type TxStores struct {
	EventReader     EventReader
	SessionStore    SessionStore
	CheckpointStore CheckpointStore
}

type Transactor interface {
	Run(
		ctx context.Context,
		work func(stores TxStores) error,
	) error
}
