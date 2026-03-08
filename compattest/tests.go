package compattest

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
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
