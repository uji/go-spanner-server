package compattest

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// InsertAndReadDDL is the DDL for the InsertAndRead test.
var InsertAndReadDDL = []string{
	`CREATE TABLE Singers (
		SingerId INT64 NOT NULL,
		FirstName STRING(1024),
		LastName STRING(1024),
	) PRIMARY KEY (SingerId)`,
}

// RunInsertAndRead tests inserting and reading rows via the Spanner client.
func RunInsertAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
		spanner.InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Catalina", "Smith"},
		),
	})
	if err != nil {
		t.Fatalf("failed to apply mutations: %v", err)
	}

	iter := client.Single().Read(ctx, "Singers",
		spanner.AllKeys(),
		[]string{"SingerId", "FirstName", "LastName"},
	)
	defer iter.Stop()

	type Singer struct {
		SingerId  int64
		FirstName string
		LastName  string
	}

	var singers []Singer
	err = iter.Do(func(row *spanner.Row) error {
		var s Singer
		if err := row.Columns(&s.SingerId, &s.FirstName, &s.LastName); err != nil {
			return err
		}
		singers = append(singers, s)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	if len(singers) != 2 {
		t.Fatalf("expected 2 singers, got %d", len(singers))
	}

	if singers[0].SingerId != 1 || singers[0].FirstName != "Marc" || singers[0].LastName != "Richards" {
		t.Errorf("unexpected singer[0]: %+v", singers[0])
	}
	if singers[1].SingerId != 2 || singers[1].FirstName != "Catalina" || singers[1].LastName != "Smith" {
		t.Errorf("unexpected singer[1]: %+v", singers[1])
	}
}

// UpdateAndReadDDL is the DDL for the UpdateAndRead test.
var UpdateAndReadDDL = InsertAndReadDDL

// RunUpdateAndRead tests inserting, updating, and reading rows via the Spanner client.
func RunUpdateAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert initial rows
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Catalina", "Smith"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Update one row (only FirstName)
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Singers",
			[]string{"SingerId", "FirstName"},
			[]interface{}{int64(1), "Marcus"},
		),
	})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Read and verify
	row, err := client.Single().ReadRow(ctx, "Singers",
		spanner.Key{int64(1)},
		[]string{"SingerId", "FirstName", "LastName"},
	)
	if err != nil {
		t.Fatalf("failed to read row: %v", err)
	}

	var singerID int64
	var firstName, lastName string
	if err := row.Columns(&singerID, &firstName, &lastName); err != nil {
		t.Fatalf("failed to scan columns: %v", err)
	}

	if firstName != "Marcus" {
		t.Errorf("expected FirstName=Marcus, got %q", firstName)
	}
	if lastName != "Richards" {
		t.Errorf("expected LastName=Richards (unchanged), got %q", lastName)
	}
}

// DeleteAndReadDDL is the DDL for the DeleteAndRead test.
var DeleteAndReadDDL = InsertAndReadDDL

// RunDeleteAndRead tests inserting, deleting, and reading rows via the Spanner client.
func RunDeleteAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert rows
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Catalina", "Smith"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(3), "Alice", "Trentor"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Delete one row by key
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Singers", spanner.Key{int64(2)}),
	})
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Read all remaining rows
	iter := client.Single().Read(ctx, "Singers",
		spanner.AllKeys(),
		[]string{"SingerId"},
	)
	defer iter.Stop()

	var ids []int64
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}
		var id int64
		if err := row.Columns(&id); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		ids = append(ids, id)
	}

	if len(ids) != 2 {
		t.Fatalf("expected 2 remaining rows, got %d", len(ids))
	}
	if ids[0] != 1 || ids[1] != 3 {
		t.Errorf("expected ids [1,3], got %v", ids)
	}
}

// InsertOrUpdateExistingDDL is the DDL for the InsertOrUpdateExisting test.
var InsertOrUpdateExistingDDL = InsertAndReadDDL

// RunInsertOrUpdateExisting tests that InsertOrUpdate on an existing row updates it.
func RunInsertOrUpdateExisting(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert initial row
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// InsertOrUpdate on the same key should update
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marcus", "Johnson"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert_or_update: %v", err)
	}

	row, err := client.Single().ReadRow(ctx, "Singers",
		spanner.Key{int64(1)},
		[]string{"SingerId", "FirstName", "LastName"},
	)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	var id int64
	var first, last string
	if err := row.Columns(&id, &first, &last); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}
	if first != "Marcus" || last != "Johnson" {
		t.Errorf("expected Marcus Johnson, got %s %s", first, last)
	}
}

// ReplaceAndReadDDL is the DDL for the ReplaceAndRead test.
var ReplaceAndReadDDL = InsertAndReadDDL

// RunReplaceAndRead tests Replace on an existing row and on a new row.
func RunReplaceAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert initial row
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Replace existing row (should overwrite)
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Replace("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marcus", "Johnson"},
		),
	})
	if err != nil {
		t.Fatalf("failed to replace existing: %v", err)
	}

	// Replace with new key (should insert)
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Replace("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Alice", "Trentor"},
		),
	})
	if err != nil {
		t.Fatalf("failed to replace new: %v", err)
	}

	// Read all
	iter := client.Single().Read(ctx, "Singers",
		spanner.AllKeys(),
		[]string{"SingerId", "FirstName", "LastName"},
	)
	defer iter.Stop()

	type Singer struct {
		ID    int64
		First string
		Last  string
	}
	var singers []Singer
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}
		var s Singer
		if err := row.Columns(&s.ID, &s.First, &s.Last); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		singers = append(singers, s)
	}

	if len(singers) != 2 {
		t.Fatalf("expected 2 singers, got %d", len(singers))
	}
	if singers[0].First != "Marcus" || singers[0].Last != "Johnson" {
		t.Errorf("expected replaced singer: Marcus Johnson, got %s %s", singers[0].First, singers[0].Last)
	}
	if singers[1].First != "Alice" || singers[1].Last != "Trentor" {
		t.Errorf("expected new singer: Alice Trentor, got %s %s", singers[1].First, singers[1].Last)
	}
}

// DeleteAllAndReadDDL is the DDL for the DeleteAllAndRead test.
var DeleteAllAndReadDDL = InsertAndReadDDL

// RunDeleteAllAndRead tests Delete with AllKeys().
func RunDeleteAllAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert rows
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Catalina", "Smith"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Delete all
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Singers", spanner.AllKeys()),
	})
	if err != nil {
		t.Fatalf("failed to delete all: %v", err)
	}

	// Read should return 0 rows
	iter := client.Single().Read(ctx, "Singers",
		spanner.AllKeys(),
		[]string{"SingerId"},
	)
	defer iter.Stop()

	var count int
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 rows after delete all, got %d", count)
	}
}

// WhereClauseDDL is the DDL for the WhereClause test.
var WhereClauseDDL = InsertAndReadDDL

// RunWhereClause tests SELECT with various WHERE conditions.
func RunWhereClause(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert test data (including a row with NULL LastName)
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Catalina", "Smith"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName"},
			[]interface{}{int64(3), "Alice"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(4), "Maria", "Garcia"},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	tests := []struct {
		name string
		sql  string
		want []int64
	}{
		{
			name: "equal",
			sql:  "SELECT SingerId FROM Singers WHERE SingerId = 1",
			want: []int64{1},
		},
		{
			name: "range_and",
			sql:  "SELECT SingerId FROM Singers WHERE SingerId > 1 AND SingerId < 4",
			want: []int64{2, 3},
		},
		{
			name: "like",
			sql:  "SELECT SingerId FROM Singers WHERE FirstName LIKE 'Ma%'",
			want: []int64{1, 4},
		},
		{
			name: "in",
			sql:  "SELECT SingerId FROM Singers WHERE SingerId IN (1, 3)",
			want: []int64{1, 3},
		},
		{
			name: "is_null",
			sql:  "SELECT SingerId FROM Singers WHERE LastName IS NULL",
			want: []int64{3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := client.Single().Query(ctx, spanner.NewStatement(tt.sql))
			defer iter.Stop()

			var ids []int64
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Fatalf("query %q failed: %v", tt.sql, err)
				}
				var id int64
				if err := row.Columns(&id); err != nil {
					t.Fatalf("scan failed: %v", err)
				}
				ids = append(ids, id)
			}

			if !reflect.DeepEqual(ids, tt.want) {
				t.Errorf("query %q: got %v, want %v", tt.sql, ids, tt.want)
			}
		})
	}
}

// DeleteByRangeAndReadDDL is the DDL for the DeleteByRangeAndRead test.
var DeleteByRangeAndReadDDL = InsertAndReadDDL

// RunDeleteByRangeAndRead tests Delete with a KeyRange.
func RunDeleteByRangeAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert rows with IDs 1-5
	var mutations []*spanner.Mutation
	for i := int64(1); i <= 5; i++ {
		mutations = append(mutations, spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{i, fmt.Sprintf("First%d", i), fmt.Sprintf("Last%d", i)},
		))
	}
	_, err := client.Apply(ctx, mutations)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Delete range [2, 4] (closed-closed)
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Singers", spanner.KeyRange{
			Start: spanner.Key{int64(2)},
			End:   spanner.Key{int64(4)},
			Kind:  spanner.ClosedClosed,
		}),
	})
	if err != nil {
		t.Fatalf("failed to delete range: %v", err)
	}

	// Read remaining: should be 1 and 5
	iter := client.Single().Read(ctx, "Singers",
		spanner.AllKeys(),
		[]string{"SingerId"},
	)
	defer iter.Stop()

	var ids []int64
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}
		var id int64
		if err := row.Columns(&id); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		ids = append(ids, id)
	}

	if len(ids) != 2 {
		t.Fatalf("expected 2 remaining rows, got %d", len(ids))
	}
	if ids[0] != 1 || ids[1] != 5 {
		t.Errorf("expected ids [1,5], got %v", ids)
	}
}
