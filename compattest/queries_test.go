package compattest

import (
	"context"
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

var whereClauseDDL = insertAndReadDDL

func runWhereClause(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	// Insert test data (including a row with NULL LastName)
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]any{int64(1), "Marc", "Richards"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]any{int64(2), "Catalina", "Smith"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName"},
			[]any{int64(3), "Alice"},
		),
		spanner.Insert("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]any{int64(4), "Maria", "Garcia"},
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
			sql:  "SELECT SingerId FROM Singers WHERE SingerId > 1 AND SingerId < 4 ORDER BY SingerId",
			want: []int64{2, 3},
		},
		{
			name: "like",
			sql:  "SELECT SingerId FROM Singers WHERE FirstName LIKE 'Ma%' ORDER BY SingerId",
			want: []int64{1, 4},
		},
		{
			name: "in",
			sql:  "SELECT SingerId FROM Singers WHERE SingerId IN (1, 3) ORDER BY SingerId",
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

func TestCompat_WhereClause(t *testing.T) {
	runCompat(t, whereClauseDDL, runWhereClause)
}
