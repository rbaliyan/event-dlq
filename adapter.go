package dlq

import (
	"context"

	"github.com/google/uuid"
	event "github.com/rbaliyan/event/v3"
)

// StoreAdapter wraps a dlq.Store to satisfy event.DLQStore.
// Use this to pass your DLQ store implementation to event.WithDLQ().
type StoreAdapter struct {
	store  Store
	source string
}

// NewStoreAdapter creates a new adapter. The source parameter identifies
// the service (e.g., "order-service").
func NewStoreAdapter(store Store, source string) *StoreAdapter {
	return &StoreAdapter{store: store, source: source}
}

var _ event.DLQStore = (*StoreAdapter)(nil)

func (a *StoreAdapter) Store(ctx context.Context, msg *event.DLQMessage) error {
	errStr := ""
	if msg.Error != nil {
		errStr = msg.Error.Error()
	}
	source := a.source
	if source == "" {
		source = msg.Source
	}
	return a.store.Store(ctx, &Message{
		ID:         uuid.New().String(),
		EventName:  msg.EventName,
		OriginalID: msg.MessageID,
		Payload:    msg.Payload,
		Metadata:   msg.Metadata,
		Error:      errStr,
		RetryCount: msg.RetryCount,
		CreatedAt:  msg.CreatedAt,
		Source:     source,
	})
}
