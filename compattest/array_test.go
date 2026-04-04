package compattest

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
)

// arrayTestCase defines a single array test case declaratively.
type arrayTestCase struct {
	ddl       []string
	ops       [][]*spanner.Mutation
	query     string // if set, use Query instead of Read
	readTable string
	readCols  []string
	readKeys  spanner.KeySet
	wantRows  []string // expected rows in testutil.FormatRow format
}

var arrayDDL = []string{
	`CREATE TABLE Items (
		ItemId   INT64 NOT NULL,
		Tags     ARRAY<STRING(MAX)>,
		Scores   ARRAY<INT64>,
	) PRIMARY KEY (ItemId)`,
}

var arrayAllTypesDDL = []string{
	`CREATE TABLE AllArrayTypes (
		Id      INT64 NOT NULL,
		Bools   ARRAY<BOOL>,
		Ints    ARRAY<INT64>,
		Floats  ARRAY<FLOAT64>,
		Strings ARRAY<STRING(MAX)>,
		Bytes   ARRAY<BYTES(MAX)>,
	) PRIMARY KEY (Id)`,
}

var arrayWithNullsDDL = []string{
	`CREATE TABLE NullableArrays (
		Id   INT64 NOT NULL,
		Tags ARRAY<STRING(MAX)>,
	) PRIMARY KEY (Id)`,
}

var arraySelectStarDDL = []string{
	`CREATE TABLE TaggedItems (
		Id   INT64 NOT NULL,
		Name STRING(256) NOT NULL,
		Tags ARRAY<STRING(MAX)>,
	) PRIMARY KEY (Id)`,
}

func strPtr(s string) *string {
	return &s
}

// arrayTests lists all array test cases.
var arrayTests = map[string]arrayTestCase{
	"InsertAndRead": {
		ddl: arrayDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Items",
					[]string{"ItemId", "Tags", "Scores"},
					[]any{int64(1), []string{"go", "spanner"}, []int64{10, 20, 30}},
				),
				spanner.Insert("Items",
					[]string{"ItemId", "Tags", "Scores"},
					[]any{int64(2), []string{"cloud"}, []int64{}},
				),
				spanner.Insert("Items",
					[]string{"ItemId"},
					[]any{int64(3)},
				),
			},
		},
		readTable: "Items",
		readCols:  []string{"ItemId", "Tags", "Scores"},
		readKeys:  spanner.AllKeys(),
		wantRows: []string{
			`(1, ["go", "spanner"], [10, 20, 30])`,
			`(2, ["cloud"], [])`,
			`(3, NULL, NULL)`,
		},
	},
	"Query": {
		ddl: arrayDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Items",
					[]string{"ItemId", "Tags", "Scores"},
					[]any{int64(1), []string{"go", "spanner"}, []int64{10, 20, 30}},
				),
				spanner.Insert("Items",
					[]string{"ItemId", "Tags"},
					[]any{int64(2), []string{"cloud"}},
				),
			},
		},
		query: "SELECT ItemId, Tags FROM Items ORDER BY ItemId",
		wantRows: []string{
			`(1, ["go", "spanner"])`,
			`(2, ["cloud"])`,
		},
	},
	"Update": {
		ddl: arrayDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Items",
					[]string{"ItemId", "Tags"},
					[]any{int64(1), []string{"initial"}},
				),
			},
			{
				spanner.Update("Items",
					[]string{"ItemId", "Tags"},
					[]any{int64(1), []string{"updated", "tags"}},
				),
			},
		},
		readTable: "Items",
		readCols:  []string{"ItemId", "Tags"},
		readKeys:  spanner.Key{int64(1)},
		wantRows:  []string{`(1, ["updated", "tags"])`},
	},
	"AllArrayTypes": {
		ddl: arrayAllTypesDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("AllArrayTypes",
					[]string{"Id", "Bools", "Ints", "Floats", "Strings", "Bytes"},
					[]any{
						int64(1),
						[]bool{true, false, true},
						[]int64{1, 2, 3},
						[]float64{1.1, 2.2},
						[]string{"a", "b"},
						[][]byte{[]byte("x"), []byte("y")},
					},
				),
			},
		},
		readTable: "AllArrayTypes",
		readCols:  []string{"Id", "Bools", "Ints", "Floats", "Strings", "Bytes"},
		readKeys:  spanner.Key{int64(1)},
		wantRows:  []string{`(1, [true, false, true], [1, 2, 3], [1.1, 2.2], ["a", "b"], [b"x", b"y"])`},
	},
	"WithNullElements": {
		ddl: arrayWithNullsDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("NullableArrays",
					[]string{"Id", "Tags"},
					[]any{int64(1), []*string{strPtr("hello"), nil, strPtr("world")}},
				),
			},
		},
		readTable: "NullableArrays",
		readCols:  []string{"Id", "Tags"},
		readKeys:  spanner.Key{int64(1)},
		wantRows:  []string{`(1, ["hello", NULL, "world"])`},
	},
	"SelectStar": {
		ddl: arraySelectStarDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("TaggedItems",
					[]string{"Id", "Name", "Tags"},
					[]any{int64(1), "item1", []string{"a", "b"}},
				),
			},
		},
		query:    "SELECT * FROM TaggedItems",
		wantRows: []string{`(1, "item1", ["a", "b"])`},
	},
}

// runArrayTest is the generic runner for array test cases.
func runArrayTest(ctx context.Context, t *testing.T, client *spanner.Client, tc arrayTestCase) {
	t.Helper()

	for i, mutations := range tc.ops {
		if _, err := client.Apply(ctx, mutations); err != nil {
			t.Fatalf("ops[%d]: failed to apply mutations: %v", i, err)
		}
	}

	var iter *spanner.RowIterator
	if tc.query != "" {
		iter = client.Single().Query(ctx, spanner.NewStatement(tc.query))
	} else {
		iter = client.Single().Read(ctx, tc.readTable, tc.readKeys, tc.readCols)
	}
	defer iter.Stop()

	var gotRows []string
	if err := iter.Do(func(row *spanner.Row) error {
		gotRows = append(gotRows, testutil.FormatRow(row))
		return nil
	}); err != nil {
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

func TestCompat_Arrays(t *testing.T) {
	for name, tc := range arrayTests {
		t.Run(name, func(t *testing.T) {
			testutil.RunCompat(t, tc.ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
				runArrayTest(ctx, t, client, tc)
			})
		})
	}
}
