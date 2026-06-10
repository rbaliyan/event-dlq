package dlq

// MigrateDedupOption configures a backend's MigrateDedup operation.
type MigrateDedupOption func(*migrateDedupOptions)

// migrateDedupOptions holds configuration for MigrateDedup (unexported).
type migrateDedupOptions struct {
	force bool
}

// WithForce allows MigrateDedup to proceed even when collapsing historical
// duplicates would delete more than half of all rows. Without it, MigrateDedup
// refuses such a large deletion and returns an error, as a safety guard against
// running it against the wrong collection/table.
func WithForce() MigrateDedupOption {
	return func(o *migrateDedupOptions) {
		o.force = true
	}
}

// applyMigrateDedupOptions builds a migrateDedupOptions from the given options.
func applyMigrateDedupOptions(opts []MigrateDedupOption) migrateDedupOptions {
	var o migrateDedupOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
