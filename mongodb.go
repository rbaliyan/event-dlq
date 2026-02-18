package dlq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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
db.event_dlq.createIndex({ "retried_at": 1 }, { sparse: true })
db.event_dlq.createIndex({ "event_name": 1, "created_at": 1 })
*/

// mongoMessage represents a DLQ message document in MongoDB
type mongoMessage struct {
	ID         string            `bson:"_id"`
	EventName  string            `bson:"event_name"`
	OriginalID string            `bson:"original_id"`
	Payload    []byte            `bson:"payload"`
	Metadata   map[string]string `bson:"metadata,omitempty"`
	Error      string            `bson:"error"`
	RetryCount int               `bson:"retry_count"`
	Source     string            `bson:"source,omitempty"`
	CreatedAt  time.Time         `bson:"created_at"`
	RetriedAt  *time.Time        `bson:"retried_at,omitempty"`
}

// toMessage converts mongoMessage to Message
func (m *mongoMessage) toMessage() *Message {
	return &Message{
		ID:         m.ID,
		EventName:  m.EventName,
		OriginalID: m.OriginalID,
		Payload:    m.Payload,
		Metadata:   m.Metadata,
		Error:      m.Error,
		RetryCount: m.RetryCount,
		Source:     m.Source,
		CreatedAt:  m.CreatedAt,
		RetriedAt:  m.RetriedAt,
	}
}

// fromMessage creates a mongoMessage from Message
func fromMessage(m *Message) *mongoMessage {
	return &mongoMessage{
		ID:         m.ID,
		EventName:  m.EventName,
		OriginalID: m.OriginalID,
		Payload:    m.Payload,
		Metadata:   m.Metadata,
		Error:      m.Error,
		RetryCount: m.RetryCount,
		Source:     m.Source,
		CreatedAt:  m.CreatedAt,
		RetriedAt:  m.RetriedAt,
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
}

// WithCollection sets a custom collection name.
func WithCollection(name string) MongoStoreOption {
	return func(o *mongoStoreOptions) {
		if name != "" {
			o.collection = name
		}
	}
}

// MongoStore is a MongoDB-based DLQ store
type MongoStore struct {
	collection *mongo.Collection
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

	return &MongoStore{
		collection: db.Collection(o.collection),
	}, nil
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
			Keys:    bson.D{{Key: "retried_at", Value: 1}},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
	}
}

// EnsureIndexes creates the required indexes for the DLQ collection
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
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

// Store adds a message to the DLQ
func (s *MongoStore) Store(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}

	mongoMsg := fromMessage(msg)

	_, err := s.collection.InsertOne(ctx, mongoMsg)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("message already exists: %s", msg.ID)
		}
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves a single message by ID
func (s *MongoStore) Get(ctx context.Context, id string) (*Message, error) {
	filter := bson.M{"_id": id}

	var mongoMsg mongoMessage
	err := s.collection.FindOne(ctx, filter).Decode(&mongoMsg)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoMsg.toMessage(), nil
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
		var mongoMsg mongoMessage
		if err := cursor.Decode(&mongoMsg); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		messages = append(messages, mongoMsg.toMessage())
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

	if !filter.StartTime.IsZero() || !filter.EndTime.IsZero() {
		createdFilter := bson.M{}
		if !filter.StartTime.IsZero() {
			createdFilter["$gte"] = filter.StartTime
		}
		if !filter.EndTime.IsZero() {
			createdFilter["$lte"] = filter.EndTime
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

	// Total count
	total, err := s.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}
	stats.TotalMessages = total

	// Pending count (not retried)
	pending, err := s.collection.CountDocuments(ctx, bson.M{"retried_at": nil})
	if err != nil {
		return nil, fmt.Errorf("count pending: %w", err)
	}
	stats.PendingMessages = pending
	stats.RetriedMessages = total - pending

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

	var mongoMsg mongoMessage
	err := s.collection.FindOne(ctx, filter).Decode(&mongoMsg)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoMsg.toMessage(), nil
}

// Compile-time checks
var _ Store = (*MongoStore)(nil)
var _ StatsProvider = (*MongoStore)(nil)
