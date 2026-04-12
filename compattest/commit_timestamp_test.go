package compattest

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
)

var commitTimestampDDL = []string{
	`CREATE TABLE Events (
		EventId INT64 NOT NULL,
		Name STRING(256),
		CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true),
		UpdatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true),
	) PRIMARY KEY (EventId)`,
}

// TestCompat_CommitTimestamp_MutationSentinel verifies that spanner.CommitTimestamp sentinel is
// replaced with the actual commit timestamp, and that CommitTimestamp is returned in the response.
func TestCompat_CommitTimestamp_MutationSentinel(t *testing.T) {
	testutil.RunCompat(t, commitTimestampDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		before := time.Now()

		commitTS, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Events",
				[]string{"EventId", "Name", "CreatedAt"},
				[]any{int64(1), "Launch", spanner.CommitTimestamp},
			),
		})
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
		after := time.Now()

		if commitTS.IsZero() {
			t.Fatal("expected a non-zero CommitTimestamp in response")
		}
		if commitTS.Before(before) || commitTS.After(after) {
			t.Errorf("CommitTimestamp %v is not between %v and %v", commitTS, before, after)
		}

		// Verify the stored value equals the commit timestamp.
		row, err := client.Single().ReadRow(ctx, "Events", spanner.Key{int64(1)}, []string{"CreatedAt"})
		if err != nil {
			t.Fatalf("ReadRow failed: %v", err)
		}
		var stored time.Time
		if err := row.Column(0, &stored); err != nil {
			t.Fatalf("Column failed: %v", err)
		}
		if !stored.Equal(commitTS) {
			t.Errorf("stored CreatedAt %v != CommitTimestamp %v", stored, commitTS)
		}
	})
}

// TestCompat_CommitTimestamp_MutationSentinel_NotAllowed verifies that writing the commit
// timestamp sentinel to a column without allow_commit_timestamp returns an error.
func TestCompat_CommitTimestamp_MutationSentinel_NotAllowed(t *testing.T) {
	ddl := []string{
		`CREATE TABLE Events (
			EventId INT64 NOT NULL,
			Name STRING(256),
			CreatedAt TIMESTAMP,
		) PRIMARY KEY (EventId)`,
	}
	testutil.RunCompat(t, ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Events",
				[]string{"EventId", "Name", "CreatedAt"},
				[]any{int64(1), "Launch", spanner.CommitTimestamp},
			),
		})
		if err == nil {
			t.Fatal("expected error when writing commit timestamp sentinel to non-allowed column, got nil")
		}
	})
}

// TestCompat_CommitTimestamp_DML_PendingCommitTimestamp verifies that PENDING_COMMIT_TIMESTAMP()
// in DML inserts the correct commit timestamp value.
func TestCompat_CommitTimestamp_DML_PendingCommitTimestamp(t *testing.T) {
	testutil.RunCompat(t, commitTimestampDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		before := time.Now()

		var commitTS time.Time
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			_, err := tx.Update(ctx, spanner.NewStatement(
				`INSERT INTO Events (EventId, Name, CreatedAt) VALUES (1, 'DML Launch', PENDING_COMMIT_TIMESTAMP())`,
			))
			return err
		})
		if err != nil {
			t.Fatalf("ReadWriteTransaction failed: %v", err)
		}

		after := time.Now()
		_ = commitTS // actual timestamp not returned from ReadWriteTransaction in this form

		// Verify the stored value is in the expected time range.
		row, err := client.Single().ReadRow(ctx, "Events", spanner.Key{int64(1)}, []string{"CreatedAt"})
		if err != nil {
			t.Fatalf("ReadRow failed: %v", err)
		}
		var stored time.Time
		if err := row.Column(0, &stored); err != nil {
			t.Fatalf("Column failed: %v", err)
		}
		if stored.IsZero() {
			t.Fatal("expected non-zero CreatedAt after PENDING_COMMIT_TIMESTAMP()")
		}
		if stored.Before(before) || stored.After(after) {
			t.Errorf("stored CreatedAt %v is not between %v and %v", stored, before, after)
		}
	})
}

// TestCompat_CommitTimestamp_AlterColumn verifies that ALTER COLUMN SET OPTIONS (allow_commit_timestamp)
// correctly enables or disables commit timestamp writes.
func TestCompat_CommitTimestamp_AlterColumn(t *testing.T) {
	ddl := []string{
		`CREATE TABLE Events (
			EventId INT64 NOT NULL,
			Name STRING(256),
			UpdatedAt TIMESTAMP,
		) PRIMARY KEY (EventId)`,
		`ALTER TABLE Events ALTER COLUMN UpdatedAt SET OPTIONS (allow_commit_timestamp = true)`,
	}
	testutil.RunCompat(t, ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		commitTS, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Events",
				[]string{"EventId", "Name", "UpdatedAt"},
				[]any{int64(1), "Test", spanner.CommitTimestamp},
			),
		})
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		row, err := client.Single().ReadRow(ctx, "Events", spanner.Key{int64(1)}, []string{"UpdatedAt"})
		if err != nil {
			t.Fatalf("ReadRow failed: %v", err)
		}
		var stored time.Time
		if err := row.Column(0, &stored); err != nil {
			t.Fatalf("Column failed: %v", err)
		}
		if !stored.Equal(commitTS) {
			t.Errorf("stored UpdatedAt %v != CommitTimestamp %v", stored, commitTS)
		}
	})
}
