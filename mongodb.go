package dlq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// isNamespaceNotFoundError checks if the error is a MongoDB namespace not found error.
// This occurs when querying collection stats for a non-existent collection.
func isNamespaceNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "ns not found") ||
		strings.Contains(errStr, "NamespaceNotFound") ||
		strings.Contains(errStr, "Collection") && strings.Contains(errStr, "not found")
}

/*
MongoDB Schema:

Collection: event_dlq

Document structure:
{
    "_id": string (DLQ message ID),
    "event_name": string,
    "original_id": string,
    "payload": Binary,
    "metadata": object,
    "error": string,
    "retry_count": int,
    "source": string (optional),
    "created_at": ISODate,
    "retried_at": ISODate (optional)
}

Indexes:
db.event_dlq.createIndex({ "event_name": 1 })
db.event_dlq.createIndex({ "created_at": 1 })
db.event_dlq.createIndex({ "retried_at": 1 })
db.event_dlq.createIndex({ "event_name": 1, "created_at": 1 })
*/

// mongoMessage represents a DLQ message document in MongoDB
type mongoMessage struct {
	ID            string            `bson:"_id"`
	EventName     string            `bson:"event_name"`
	OriginalID    string            `bson:"original_id"`
	Payload       []byte            `bson:"payload"`
	Metadata      map[string]string `bson:"metadata,omitempty"`
	Error         string            `bson:"error"`
	RetryCount    int               `bson:"retry_count"`
	Source        string            `bson:"source,omitempty"`
	CreatedAt     time.Time         `bson:"created_at"`
	RetriedAt     *time.Time        `bson:"retried_at,omitempty"`
	QuarantinedAt *time.Time        `bson:"quarantined_at,omitempty"`
}

// decodeMongoDoc decodes a raw BSON document into a Message. Centralizing the
// decode keeps Get and List consistent and lets the BSON unmarshal path — an
// untrusted-input decode — be fuzzed (FuzzMongoDecode) without a live MongoDB.
func decodeMongoDoc(raw bson.Raw) (*Message, error) {
	var mm mongoMessage
	if err := bson.Unmarshal(raw, &mm); err != nil {
		return nil, fmt.Errorf("unmarshal bson: %w", err)
	}
	return mm.toMessage(), nil
}

// toMessage converts mongoMessage to Message
func (m *mongoMessage) toMessage() *Message {
	return &Message{
		ID:            m.ID,
		EventName:     m.EventName,
		OriginalID:    m.OriginalID,
		Payload:       m.Payload,
		Metadata:      m.Metadata,
		Error:         m.Error,
		RetryCount:    m.RetryCount,
		Source:        m.Source,
		CreatedAt:     m.CreatedAt,
		RetriedAt:     m.RetriedAt,
		QuarantinedAt: m.QuarantinedAt,
	}
}

// fromMessage creates a mongoMessage from Message
func fromMessage(m *Message) *mongoMessage {
	return &mongoMessage{
		ID:            m.ID,
		EventName:     m.EventName,
		OriginalID:    m.OriginalID,
		Payload:       m.Payload,
		Metadata:      m.Metadata,
		Error:         m.Error,
		RetryCount:    m.RetryCount,
		Source:        m.Source,
		CreatedAt:     m.CreatedAt,
		RetriedAt:     m.RetriedAt,
		QuarantinedAt: m.QuarantinedAt,
	}
}

// cappedInfo contains information about a capped collection
type cappedInfo struct {
	Capped   bool  // Whether the collection is capped
	Size     int64 // Maximum size in bytes
	MaxDocs  int64 // Maximum number of documents (0 = unlimited)
	StorSize int64 // Current storage size in bytes
	Count    int64 // Current document count
}

// MongoStoreOption configures a MongoStore.
type MongoStoreOption func(*mongoStoreOptions)

type mongoStoreOptions struct {
	collection string
	dedup      bool
}

// WithCollection sets a custom collection name.
func WithCollection(name string) MongoStoreOption {
	return func(o *mongoStoreOptions) {
		if name != "" {
			o.collection = name
		}
	}
}

// WithMongoDedup enables upsert-on-(EventName, OriginalID): re-storing a message
// with the same event name and (non-empty) original ID increments retry_count on
// the existing document instead of inserting a new one. Default: off (backward
// compatible — existing callers see no change).
func WithMongoDedup() MongoStoreOption {
	return func(o *mongoStoreOptions) { o.dedup = true }
}

// MongoStore is a MongoDB-based DLQ store
type MongoStore struct {
	collection *mongo.Collection
	dedup      bool        // Whether dedup upsert is enabled (off by default)
	cappedMu   sync.Mutex  // Protects capped fields
	cappedDone bool        // Whether capped info has been fetched
	cappedInfo *cappedInfo // Cached capped info (nil = not checked yet)
	cappedErr  error       // Error from first capped info fetch
}

// NewMongoStore creates a new MongoDB DLQ store.
// Returns an error if db is nil.
func NewMongoStore(db *mongo.Database, opts ...MongoStoreOption) (*MongoStore, error) {
	if db == nil {
		return nil, fmt.Errorf("dlq: db must not be nil")
	}

	o := &mongoStoreOptions{
		collection: "event_dlq",
	}
	for _, opt := range opts {
		opt(o)
	}

	s := &MongoStore{
		collection: db.Collection(o.collection),
		dedup:      o.dedup,
	}
	go func() { // #nosec G118 — background goroutine intentionally outlives constructor context
		if err := s.EnsureIndexes(context.Background()); err != nil {
			slog.Default().Error("failed to ensure DLQ indexes", "error", err, "collection", o.collection)
		}
	}()
	return s, nil
}

// Collection returns the underlying MongoDB collection
func (s *MongoStore) Collection() *mongo.Collection {
	return s.collection
}

// Indexes returns the required indexes for the DLQ collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := store.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "event_name", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
		},
		{
			// NOT sparse: pending messages omit retried_at (bson omitempty), and a
			// sparse index would exclude them, forcing {retried_at: nil} counts and
			// the ExcludeRetried filter into full collection scans. A non-sparse
			// index stores the absent field as null so those queries use a COUNT_SCAN.
			Keys: bson.D{{Key: "retried_at", Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
	}
}

// EnsureIndexes creates the required indexes for the DLQ collection.
//
// It also migrates the retried_at index: earlier versions created it as a sparse
// index, which cannot serve {retried_at: nil} queries (pending messages omit the
// field) and conflicts with the non-sparse definition this version creates. Any
// pre-existing sparse retried_at index is dropped first so it can be recreated.
//
// When dedup is enabled (WithMongoDedup), a unique index on (event_name,
// original_id) is created in a separate, failure-tolerant step. If the collection
// still contains legacy duplicate pairs the index creation will fail; run
// MigrateDedup to collapse duplicates first, then call EnsureIndexes again.
// When dedup is disabled (the default), no unique index is created, so multiple
// DLQ entries for the same (event_name, original_id) are permitted.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	if err := s.dropLegacySparseRetriedIndex(ctx); err != nil {
		return err
	}
	if _, err := s.collection.Indexes().CreateMany(ctx, s.Indexes()); err != nil {
		return err
	}

	// Unique dedup index on (event_name, original_id). Only created when dedup is
	// enabled: with dedup off, multiple DLQ entries for the same (event_name,
	// original_id) are allowed and expected, so no unique constraint must exist.
	// Created best-effort: on a collection that still contains legacy duplicates
	// this fails, which is expected — callers run MigrateDedup to collapse
	// duplicates first. A failure here must NOT break index bootstrap, so we log
	// and continue.
	if s.dedup {
		uniqModel := mongo.IndexModel{
			Keys:    bson.D{{Key: "event_name", Value: 1}, {Key: "original_id", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("uniq_event_original"),
		}
		if _, err := s.collection.Indexes().CreateOne(ctx, uniqModel); err != nil {
			slog.Default().Warn("dlq: unique dedup index not created (legacy duplicates?); run MigrateDedup",
				"collection", s.collection.Name(), "error", err)
		}
	}
	return nil
}

// dropLegacySparseRetriedIndex removes a pre-existing sparse retried_at_1 index so
// EnsureIndexes can recreate it as non-sparse. It is a no-op when the collection
// does not exist, the index is absent, or the index is already non-sparse, so it
// never rebuilds an index that is already correct.
func (s *MongoStore) dropLegacySparseRetriedIndex(ctx context.Context) error {
	cursor, err := s.collection.Indexes().List(ctx)
	if err != nil {
		if isNamespaceNotFoundError(err) {
			return nil // collection not created yet; nothing to migrate
		}
		return fmt.Errorf("list indexes: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var specs []struct {
		Name   string `bson:"name"`
		Sparse bool   `bson:"sparse"`
	}
	if err := cursor.All(ctx, &specs); err != nil {
		return fmt.Errorf("decode index specs: %w", err)
	}

	for _, spec := range specs {
		if spec.Name == "retried_at_1" && spec.Sparse {
			if err := s.collection.Indexes().DropOne(ctx, spec.Name); err != nil {
				return fmt.Errorf("drop legacy sparse index %q: %w", spec.Name, err)
			}
			break
		}
	}
	return nil
}

// IsCapped returns whether the collection is a capped collection.
// The result is cached after the first call.
func (s *MongoStore) IsCapped(ctx context.Context) (bool, error) {
	info, err := s.getCappedInfo(ctx)
	if err != nil {
		return false, err
	}
	return info.Capped, nil
}

// getCappedInfo returns detailed information about the collection's capped status.
// The result is cached after the first call. Thread-safe.
func (s *MongoStore) getCappedInfo(ctx context.Context) (*cappedInfo, error) {
	s.cappedMu.Lock()
	defer s.cappedMu.Unlock()
	if !s.cappedDone {
		s.cappedInfo, s.cappedErr = s.fetchCappedInfo(ctx)
		s.cappedDone = true
	}
	return s.cappedInfo, s.cappedErr
}

// fetchCappedInfo queries MongoDB for collection stats to determine if capped.
func (s *MongoStore) fetchCappedInfo(ctx context.Context) (*cappedInfo, error) {
	var result bson.M
	err := s.collection.Database().RunCommand(ctx, bson.D{
		{Key: "collStats", Value: s.collection.Name()},
	}).Decode(&result)

	if err != nil {
		// Collection might not exist yet - treat as non-capped
		// MongoDB returns "ns not found" or similar for missing collections
		if isNamespaceNotFoundError(err) {
			return &cappedInfo{Capped: false}, nil
		}
		return nil, fmt.Errorf("collStats: %w", err)
	}

	info := &cappedInfo{}

	if capped, ok := result["capped"].(bool); ok {
		info.Capped = capped
	}
	if size, ok := result["maxSize"].(int64); ok {
		info.Size = size
	} else if size, ok := result["maxSize"].(int32); ok {
		info.Size = int64(size)
	}
	if maxDocs, ok := result["max"].(int64); ok {
		info.MaxDocs = maxDocs
	} else if maxDocs, ok := result["max"].(int32); ok {
		info.MaxDocs = int64(maxDocs)
	}
	if storSize, ok := result["storageSize"].(int64); ok {
		info.StorSize = storSize
	} else if storSize, ok := result["storageSize"].(int32); ok {
		info.StorSize = int64(storSize)
	}
	if count, ok := result["count"].(int64); ok {
		info.Count = count
	} else if count, ok := result["count"].(int32); ok {
		info.Count = int64(count)
	}

	return info, nil
}

// CreateCapped creates the collection as a capped collection.
// This must be called before any documents are inserted.
// Returns an error if the collection already exists.
//
// Parameters:
//   - sizeBytes: Maximum size of the collection in bytes (required, minimum 4096)
//   - maxDocs: Maximum number of documents (0 = no limit, only size matters)
//
// Example:
//
//	// Create 100MB capped collection for DLQ
//	err := store.CreateCapped(ctx, 100*1024*1024, 0)
//
//	// Create capped collection with max 50000 DLQ messages
//	err := store.CreateCapped(ctx, 100*1024*1024, 50000)
func (s *MongoStore) CreateCapped(ctx context.Context, sizeBytes int64, maxDocs int64) error {
	opts := options.CreateCollection().SetCapped(true).SetSizeInBytes(sizeBytes)
	if maxDocs > 0 {
		opts.SetMaxDocuments(maxDocs)
	}

	err := s.collection.Database().CreateCollection(ctx, s.collection.Name(), opts)
	if err != nil {
		return fmt.Errorf("create capped collection: %w", err)
	}

	// Refresh cached info
	s.cappedMu.Lock()
	s.cappedDone = false
	s.cappedInfo = nil
	s.cappedErr = nil
	s.cappedMu.Unlock()

	return nil
}

// Store adds a message to the DLQ. When dedup is enabled and msg.OriginalID is
// non-empty, a duplicate (EventName, OriginalID) pair upserts the existing
// document: RetryCount is incremented, Error/Payload/Metadata are updated to the
// latest values, RetriedAt is cleared, and CreatedAt is preserved as first-seen.
func (s *MongoStore) Store(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}
	if s.dedup && msg.OriginalID != "" {
		return s.upsertDedup(ctx, msg)
	}
	doc := fromMessage(msg)
	if _, err := s.collection.InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("message already exists: %s", msg.ID)
		}
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

// upsertDedup collapses re-stores of the same (event_name, original_id) into one
// document: increments retry_count, updates error/payload/metadata to the latest,
// preserves the first-seen created_at, and clears retried_at. Uses an aggregation
// pipeline so the same retry_count field can be initialized on insert and
// incremented on update without a conflicting-operator error.
func (s *MongoStore) upsertDedup(ctx context.Context, msg *Message) error {
	filter := bson.M{"event_name": msg.EventName, "original_id": msg.OriginalID}
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$set", Value: bson.M{
			"retry_count": bson.M{"$cond": bson.A{
				bson.M{"$eq": bson.A{bson.M{"$type": "$retry_count"}, "missing"}},
				msg.RetryCount,
				bson.M{"$add": bson.A{"$retry_count", 1}},
			}},
			"_id":         bson.M{"$ifNull": bson.A{"$_id", msg.ID}},
			"event_name":  msg.EventName,
			"original_id": msg.OriginalID,
			"error":       msg.Error,
			"payload":     msg.Payload,
			"metadata":    msg.Metadata,
			"source":      bson.M{"$ifNull": bson.A{"$source", msg.Source}},
			"created_at":  bson.M{"$ifNull": bson.A{"$created_at", msg.CreatedAt}},
			"retried_at":  nil,
		}}},
	}
	opts := options.UpdateOne().SetUpsert(true)
	// Concurrent upserts of the same (event_name, original_id) race to insert
	// before the document exists; MongoDB lets only one insert win and fails the
	// rest with a duplicate-key error (E11000). Retry on that error: once any
	// inserter wins, the document exists and the retry takes the increment path.
	// A single retry is sufficient in principle (the doc exists after the first
	// round); the small bound guards against repeated contention.
	const maxAttempts = 3
	var err error
	for range maxAttempts {
		if _, err = s.collection.UpdateOne(ctx, filter, mongo.Pipeline(pipeline), opts); err == nil {
			return nil
		}
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("upsert dedup: %w", err)
		}
	}
	return fmt.Errorf("upsert dedup after %d attempts: %w", maxAttempts, err)
}

// Get retrieves a single message by ID
func (s *MongoStore) Get(ctx context.Context, id string) (*Message, error) {
	filter := bson.M{"_id": id}

	raw, err := s.collection.FindOne(ctx, filter).Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return decodeMongoDoc(raw)
}

// List returns messages matching the filter
func (s *MongoStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	mongoFilter := s.buildFilter(filter)

	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})
	if filter.Limit > 0 {
		opts.SetLimit(int64(filter.Limit))
	}
	if filter.Offset > 0 {
		opts.SetSkip(int64(filter.Offset))
	}

	cursor, err := s.collection.Find(ctx, mongoFilter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var messages []*Message
	for cursor.Next(ctx) {
		msg, err := decodeMongoDoc(cursor.Current)
		if err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		messages = append(messages, msg)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor iteration: %w", err)
	}

	return messages, nil
}

// Count returns the number of messages matching the filter
func (s *MongoStore) Count(ctx context.Context, filter Filter) (int64, error) {
	mongoFilter := s.buildFilter(filter)
	return s.collection.CountDocuments(ctx, mongoFilter)
}

// buildFilter creates a MongoDB filter from DLQ Filter
func (s *MongoStore) buildFilter(filter Filter) bson.M {
	mongoFilter := bson.M{}

	if filter.EventName != "" {
		mongoFilter["event_name"] = filter.EventName
	}

	if !filter.After.IsZero() || !filter.Before.IsZero() {
		createdFilter := bson.M{}
		if !filter.After.IsZero() {
			createdFilter["$gte"] = filter.After
		}
		if !filter.Before.IsZero() {
			createdFilter["$lte"] = filter.Before
		}
		mongoFilter["created_at"] = createdFilter
	}

	if filter.Error != "" {
		mongoFilter["error"] = bson.Regex{Pattern: filter.Error, Options: "i"}
	}

	if filter.MaxRetries > 0 {
		mongoFilter["retry_count"] = bson.M{"$lte": filter.MaxRetries}
	}

	if filter.Source != "" {
		mongoFilter["source"] = filter.Source
	}

	if filter.ExcludeRetried {
		mongoFilter["retried_at"] = nil
	}

	if filter.ExcludeQuarantined {
		mongoFilter["quarantined_at"] = nil
	}

	return mongoFilter
}

// MarkRetried marks a message as replayed
func (s *MongoStore) MarkRetried(ctx context.Context, id string) error {
	filter := bson.M{"_id": id}
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"retried_at": now,
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// Quarantine marks a message as a terminal, non-retryable failure.
func (s *MongoStore) Quarantine(ctx context.Context, id string) error {
	now := time.Now()
	filter := bson.M{"_id": id}
	update := bson.M{
		"$set": bson.M{
			"quarantined_at": now,
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("quarantine: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// Delete removes a message from the DLQ.
// For capped collections, this returns an error since deletion is not supported.
func (s *MongoStore) Delete(ctx context.Context, id string) error {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return fmt.Errorf("check capped: %w", err)
	}
	if capped {
		return fmt.Errorf("delete not supported on capped collection")
	}

	filter := bson.M{"_id": id}

	result, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// DeleteOlderThan removes messages older than the specified age.
// For capped collections, this is a no-op since MongoDB handles cleanup automatically.
func (s *MongoStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return 0, fmt.Errorf("check capped: %w", err)
	}
	if capped {
		// Capped collections auto-cleanup, deletion not needed
		return 0, nil
	}

	cutoff := time.Now().Add(-age)
	filter := bson.M{
		"created_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// DeleteByFilter removes messages matching the filter.
// For capped collections, this is a no-op since MongoDB handles cleanup automatically.
func (s *MongoStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return 0, fmt.Errorf("check capped: %w", err)
	}
	if capped {
		// Capped collections auto-cleanup, deletion not needed
		return 0, nil
	}

	mongoFilter := s.buildFilter(filter)

	result, err := s.collection.DeleteMany(ctx, mongoFilter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// Stats returns DLQ statistics
func (s *MongoStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	// Total count. EstimatedDocumentCount reads collection metadata (O(1)) instead
	// of scanning every document like CountDocuments(bson.M{}) would. The count can
	// be momentarily stale after an unclean shutdown, which is acceptable for stats.
	total, err := s.collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}
	stats.TotalMessages = total

	// Pending count: not retried AND not quarantined. Exact and index-backed.
	pending, err := s.collection.CountDocuments(ctx, bson.M{"retried_at": nil, "quarantined_at": nil})
	if err != nil {
		return nil, fmt.Errorf("count pending: %w", err)
	}
	stats.PendingMessages = pending

	// Quarantined count: messages marked as terminal (non-retryable).
	quarantined, err := s.collection.CountDocuments(ctx, bson.M{"quarantined_at": bson.M{"$ne": nil}})
	if err != nil {
		return nil, fmt.Errorf("count quarantined: %w", err)
	}
	stats.QuarantinedMessages = quarantined

	// Retried count: messages that have been replayed.
	retried, err := s.collection.CountDocuments(ctx, bson.M{"retried_at": bson.M{"$ne": nil}})
	if err != nil {
		return nil, fmt.Errorf("count retried: %w", err)
	}
	stats.RetriedMessages = retried

	// Count by event using aggregation
	eventPipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id":   "$event_name",
			"count": bson.M{"$sum": 1},
		}}},
	}

	cursor, err := s.collection.Aggregate(ctx, eventPipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate by event: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	for cursor.Next(ctx) {
		var result struct {
			EventName string `bson:"_id"`
			Count     int64  `bson:"count"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		stats.MessagesByEvent[result.EventName] = result.Count
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor iteration: %w", err)
	}

	// Find oldest and newest
	oldestOpts := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: 1}})
	var oldest mongoMessage
	err = s.collection.FindOne(ctx, bson.M{}, oldestOpts).Decode(&oldest)
	if err == nil {
		stats.OldestMessage = &oldest.CreatedAt
	}

	newestOpts := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: -1}})
	var newest mongoMessage
	err = s.collection.FindOne(ctx, bson.M{}, newestOpts).Decode(&newest)
	if err == nil {
		stats.NewestMessage = &newest.CreatedAt
	}

	return stats, nil
}

// GetByOriginalID retrieves a message by its original message ID
func (s *MongoStore) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	filter := bson.M{"original_id": originalID}

	raw, err := s.collection.FindOne(ctx, filter).Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return decodeMongoDoc(raw)
}

// Health performs a health check by pinging the MongoDB database and counting documents.
// Returns healthy if both the ping and count succeed, unhealthy otherwise.
// The result includes message_count in details, matching RedisStore and PostgresStore.
func (s *MongoStore) Health(ctx context.Context) *health.Result {
	start := time.Now()

	err := s.collection.Database().Client().Ping(ctx, nil)
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"database":   s.collection.Database().Name(),
				"collection": s.collection.Name(),
			},
		}
	}

	// Get message count to include in health details. Uses the O(1) metadata count
	// so a health probe never triggers a full collection scan.
	count, err := s.collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("count failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"database":   s.collection.Database().Name(),
				"collection": s.collection.Name(),
			},
		}
	}

	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"database":      s.collection.Database().Name(),
			"collection":    s.collection.Name(),
			"message_count": count,
		},
	}
}

// MigrateDedup collapses historical duplicate (event_name, original_id) groups
// into a single document: the newest document by created_at is kept as the
// survivor; its retry_count is replaced with the sum of all duplicates' counts;
// its created_at is set to the oldest in the group; and the earliest
// quarantined_at (if any document in the group was quarantined) is propagated.
// After all duplicates are resolved, the unique (event_name, original_id) index
// is created via EnsureIndexes.
//
// Safety guard: if collapsing would delete more than 50% of all documents in the
// collection, MigrateDedup returns an error without modifying anything. Pass
// WithForce to skip the guard. This protects against accidentally running the
// migration against the wrong collection.
//
// MigrateDedup is idempotent: a second call on a clean (already-deduplicated)
// collection removes 0 documents and re-runs EnsureIndexes (which is a no-op
// when the index already exists).
//
// Returns the number of documents removed.
func (s *MongoStore) MigrateDedup(ctx context.Context, opts ...MigrateDedupOption) (int64, error) {
	o := applyMigrateDedupOptions(opts)

	total, err := s.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}

	// Find all groups of (event_name, original_id) that have more than one document.
	// Sort by created_at ascending so $push produces IDs in oldest-first order;
	// the last element in each group is the newest (survivor).
	cursor, err := s.collection.Aggregate(ctx, mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.M{"original_id": bson.M{"$ne": ""}}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "created_at", Value: 1}}}},
		bson.D{{Key: "$group", Value: bson.M{
			"_id":            bson.M{"event_name": "$event_name", "original_id": "$original_id"},
			"ids":            bson.M{"$push": "$_id"},
			"sumRetry":       bson.M{"$sum": "$retry_count"},
			"minCreated":     bson.M{"$min": "$created_at"},
			"minQuarantined": bson.M{"$min": "$quarantined_at"},
			"count":          bson.M{"$sum": 1},
		}}},
		bson.D{{Key: "$match", Value: bson.M{"count": bson.M{"$gt": 1}}}},
	})
	if err != nil {
		return 0, fmt.Errorf("aggregate duplicates: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	type group struct {
		IDs            []string   `bson:"ids"`
		SumRetry       int        `bson:"sumRetry"`
		MinCreated     time.Time  `bson:"minCreated"`
		MinQuarantined *time.Time `bson:"minQuarantined"`
	}
	var groups []group
	if err := cursor.All(ctx, &groups); err != nil {
		return 0, fmt.Errorf("decode groups: %w", err)
	}

	// Count how many documents would be deleted (all but the survivor in each group).
	var toDelete int64
	for _, g := range groups {
		toDelete += int64(len(g.IDs) - 1)
	}

	if !o.force && total > 0 && toDelete*2 > total {
		return 0, fmt.Errorf(
			"dlq: MigrateDedup would delete %d of %d documents (>50%%); pass WithForce to proceed",
			toDelete, total,
		)
	}

	var removed int64
	for _, g := range groups {
		// IDs are sorted oldest→newest by the $sort + $push; keep the newest (last).
		keep := g.IDs[len(g.IDs)-1]
		drop := g.IDs[:len(g.IDs)-1]

		set := bson.M{
			"retry_count": g.SumRetry,
			"created_at":  g.MinCreated,
		}
		if g.MinQuarantined != nil {
			set["quarantined_at"] = *g.MinQuarantined
		}

		if _, err := s.collection.UpdateByID(ctx, keep, bson.M{"$set": set}); err != nil {
			return removed, fmt.Errorf("update survivor %q: %w", keep, err)
		}

		res, err := s.collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": drop}})
		if err != nil {
			return removed, fmt.Errorf("delete duplicates: %w", err)
		}
		removed += res.DeletedCount
	}

	// Create the unique (event_name, original_id) index now that duplicates are gone.
	if err := s.EnsureIndexes(ctx); err != nil {
		return removed, fmt.Errorf("ensure indexes after dedup: %w", err)
	}
	return removed, nil
}

// Compile-time checks
var _ Store = (*MongoStore)(nil)
var _ StatsProvider = (*MongoStore)(nil)
var _ health.Checker = (*MongoStore)(nil)
var _ Quarantiner = (*MongoStore)(nil)
