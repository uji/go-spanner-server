package compattest

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
)

// mutationTestCase defines a single mutation test case declaratively.
type mutationTestCase struct {
	name      string
	ddl       []string
	ops       [][]*spanner.Mutation // each element is one Apply call, executed in order
	readTable string
	readCols  []string
	readKeys  spanner.KeySet
	wantRows  []string // expected rows in testutil.FormatRow format: (val1, val2, ...)
}

var singersDDL = []string{
	`CREATE TABLE Singers (
		SingerId INT64 NOT NULL,
		FirstName STRING(1024),
		LastName STRING(1024),
	) PRIMARY KEY (SingerId)`,
}

// mutationTests lists all mutation test cases.
var mutationTests = []mutationTestCase{
	{
		name: "InsertAndRead",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.InsertOrUpdate("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marc", "Richards"},
				),
				spanner.InsertOrUpdate("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(2), "Catalina", "Smith"},
				),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId", "FirstName", "LastName"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{`(1, "Marc", "Richards")`, `(2, "Catalina", "Smith")`},
	},
	{
		name: "UpdateAndRead",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marc", "Richards"},
				),
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(2), "Catalina", "Smith"},
				),
			},
			{
				spanner.Update("Singers",
					[]string{"SingerId", "FirstName"},
					[]any{int64(1), "Marcus"},
				),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId", "FirstName", "LastName"},
		readKeys:  spanner.Key{int64(1)},
		wantRows:  []string{`(1, "Marcus", "Richards")`},
	},
	{
		name: "DeleteAndRead",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marc", "Richards"},
				),
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(2), "Catalina", "Smith"},
				),
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(3), "Alice", "Trentor"},
				),
			},
			{
				spanner.Delete("Singers", spanner.Key{int64(2)}),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{"(1)", "(3)"},
	},
	{
		name: "InsertOrUpdateExisting",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marc", "Richards"},
				),
			},
			{
				spanner.InsertOrUpdate("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marcus", "Johnson"},
				),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId", "FirstName", "LastName"},
		readKeys:  spanner.Key{int64(1)},
		wantRows:  []string{`(1, "Marcus", "Johnson")`},
	},
	{
		name: "ReplaceAndRead",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marc", "Richards"},
				),
			},
			{
				spanner.Replace("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marcus", "Johnson"},
				),
			},
			{
				spanner.Replace("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(2), "Alice", "Trentor"},
				),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId", "FirstName", "LastName"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{`(1, "Marcus", "Johnson")`, `(2, "Alice", "Trentor")`},
	},
	{
		name: "DeleteAllAndRead",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(1), "Marc", "Richards"},
				),
				spanner.Insert("Singers",
					[]string{"SingerId", "FirstName", "LastName"},
					[]any{int64(2), "Catalina", "Smith"},
				),
			},
			{
				spanner.Delete("Singers", spanner.AllKeys()),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
	{
		name: "DeleteByRangeAndRead",
		ddl:  singersDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "FirstName", "LastName"}, []any{int64(1), "First1", "Last1"}),
				spanner.Insert("Singers", []string{"SingerId", "FirstName", "LastName"}, []any{int64(2), "First2", "Last2"}),
				spanner.Insert("Singers", []string{"SingerId", "FirstName", "LastName"}, []any{int64(3), "First3", "Last3"}),
				spanner.Insert("Singers", []string{"SingerId", "FirstName", "LastName"}, []any{int64(4), "First4", "Last4"}),
				spanner.Insert("Singers", []string{"SingerId", "FirstName", "LastName"}, []any{int64(5), "First5", "Last5"}),
			},
			{
				spanner.Delete("Singers", spanner.KeyRange{
					Start: spanner.Key{int64(2)},
					End:   spanner.Key{int64(4)},
					Kind:  spanner.ClosedClosed,
				}),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{"(1)", "(5)"},
	},
}

// runMutationTest is the generic runner for mutation test cases.
func runMutationTest(ctx context.Context, t *testing.T, client *spanner.Client, tc mutationTestCase) {
	t.Helper()

	for i, mutations := range tc.ops {
		if _, err := client.Apply(ctx, mutations); err != nil {
			t.Fatalf("ops[%d]: failed to apply mutations: %v", i, err)
		}
	}

	iter := client.Single().Read(ctx, tc.readTable, tc.readKeys, tc.readCols)
	defer iter.Stop()

	var gotRows []string
	err := iter.Do(func(row *spanner.Row) error {
		gotRows = append(gotRows, testutil.FormatRow(row))
		return nil
	})
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}
	if gotRows == nil {
		gotRows = []string{}
	}

	if len(gotRows) != len(tc.wantRows) {
		t.Fatalf("row count: got %d, want %d\ngot:\n%s\nwant:\n%s",
			len(gotRows), len(tc.wantRows),
			strings.Join(gotRows, "\n"),
			strings.Join(tc.wantRows, "\n"),
		)
	}
	for i, want := range tc.wantRows {
		if gotRows[i] != want {
			t.Errorf("row[%d]: got %s, want %s", i, gotRows[i], want)
		}
	}
}

func TestCompat_Mutations(t *testing.T) {
	for _, tc := range mutationTests {
		t.Run(tc.name, func(t *testing.T) {
			runCompat(t, tc.ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
				runMutationTest(ctx, t, client, tc)
			})
		})
	}
}
