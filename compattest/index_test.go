package compattest

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
	"google.golang.org/api/iterator"
)

// indexTestCase defines a single index test case declaratively.
type indexTestCase struct {
	ddl       []string
	ops       [][]*spanner.Mutation
	readTable string
	readIndex string
	readCols  []string
	readKeys  spanner.KeySet
	wantRows  []string // expected rows in testutil.FormatRow format
}

var indexDDL = []string{
	`CREATE TABLE Songs (
		SongId INT64 NOT NULL,
		Title STRING(256),
		Genre STRING(64),
		Rating INT64,
	) PRIMARY KEY (SongId)`,
	`CREATE INDEX SongsByGenre ON Songs(Genre)`,
}

var uniqueIndexDDL = []string{
	`CREATE TABLE Items (
		ItemId INT64 NOT NULL,
		Code STRING(64),
	) PRIMARY KEY (ItemId)`,
	`CREATE UNIQUE INDEX ItemsByCode ON Items(Code)`,
}

var nullFilteredIndexDDL = []string{
	`CREATE TABLE Products (
		ProductId INT64 NOT NULL,
		Category STRING(64),
	) PRIMARY KEY (ProductId)`,
	`CREATE NULL_FILTERED INDEX ProductsByCategory ON Products(Category)`,
}

var storingIndexDDL = []string{
	`CREATE TABLE Employees (
		EmpId INT64 NOT NULL,
		Name STRING(256),
		Dept STRING(64),
		Salary INT64,
	) PRIMARY KEY (EmpId)`,
	`CREATE INDEX EmployeesByDept ON Employees(Dept) STORING (Salary)`,
}

var indexRangeScanDDL = []string{
	`CREATE TABLE Scores (
		ScoreId INT64 NOT NULL,
		Value INT64,
	) PRIMARY KEY (ScoreId)`,
	`CREATE INDEX ScoresByValue ON Scores(Value)`,
}

var descIndexDDL = []string{
	`CREATE TABLE Events (
		EventId INT64 NOT NULL,
		Priority INT64,
	) PRIMARY KEY (EventId)`,
	`CREATE INDEX EventsByPriorityDesc ON Events(Priority DESC)`,
}

var dropIndexDDL = []string{
	`CREATE TABLE Tags (
		TagId INT64 NOT NULL,
		Label STRING(64),
	) PRIMARY KEY (TagId)`,
	`CREATE INDEX TagsByLabel ON Tags(Label)`,
}

var indexMutationSyncDDL = indexDDL

// indexTests lists all table-driven index test cases.
var indexTests = map[string]indexTestCase{
	"CreateIndexAndReadByIndex": {
		ddl: indexDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(1), "Song A", "Pop", int64(5)}),
				spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(2), "Song B", "Rock", int64(3)}),
				spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(3), "Song C", "Pop", int64(4)}),
			},
		},
		readTable: "Songs",
		readIndex: "SongsByGenre",
		readCols:  []string{"SongId", "Genre"},
		readKeys:  spanner.KeySets(spanner.Key{"Pop"}),
		wantRows: []string{
			`(1, "Pop")`,
			`(3, "Pop")`,
		},
	},
	"NullFilteredIndex": {
		ddl: nullFilteredIndexDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Products", []string{"ProductId", "Category"}, []any{int64(1), "Electronics"}),
				spanner.Insert("Products", []string{"ProductId"}, []any{int64(2)}),
				spanner.Insert("Products", []string{"ProductId", "Category"}, []any{int64(3), "Books"}),
			},
		},
		readTable: "Products",
		readIndex: "ProductsByCategory",
		readCols:  []string{"ProductId", "Category"},
		readKeys:  spanner.AllKeys(),
		wantRows: []string{
			`(3, "Books")`,
			`(1, "Electronics")`,
		},
	},
	"IndexWithStoring": {
		ddl: storingIndexDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Employees", []string{"EmpId", "Name", "Dept", "Salary"}, []any{int64(1), "Alice", "Eng", int64(100)}),
				spanner.Insert("Employees", []string{"EmpId", "Name", "Dept", "Salary"}, []any{int64(2), "Bob", "Sales", int64(80)}),
				spanner.Insert("Employees", []string{"EmpId", "Name", "Dept", "Salary"}, []any{int64(3), "Carol", "Eng", int64(120)}),
			},
		},
		readTable: "Employees",
		readIndex: "EmployeesByDept",
		readCols:  []string{"EmpId", "Dept", "Salary"},
		readKeys:  spanner.KeySets(spanner.Key{"Eng"}),
		wantRows: []string{
			`(1, "Eng", 100)`,
			`(3, "Eng", 120)`,
		},
	},
	"IndexRangeScan": {
		ddl: indexRangeScanDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(1), int64(10)}),
				spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(2), int64(20)}),
				spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(3), int64(30)}),
				spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(4), int64(40)}),
				spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(5), int64(50)}),
			},
		},
		readTable: "Scores",
		readIndex: "ScoresByValue",
		readCols:  []string{"ScoreId", "Value"},
		readKeys: spanner.KeySets(spanner.KeyRange{
			Start: spanner.Key{int64(20)},
			End:   spanner.Key{int64(40)},
			Kind:  spanner.ClosedClosed,
		}),
		wantRows: []string{
			"(2, 20)",
			"(3, 30)",
			"(4, 40)",
		},
	},
	"DescIndex": {
		ddl: descIndexDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Events", []string{"EventId", "Priority"}, []any{int64(1), int64(10)}),
				spanner.Insert("Events", []string{"EventId", "Priority"}, []any{int64(2), int64(30)}),
				spanner.Insert("Events", []string{"EventId", "Priority"}, []any{int64(3), int64(20)}),
			},
		},
		readTable: "Events",
		readIndex: "EventsByPriorityDesc",
		readCols:  []string{"EventId", "Priority"},
		readKeys:  spanner.AllKeys(),
		wantRows: []string{
			"(2, 30)",
			"(3, 20)",
			"(1, 10)",
		},
	},
	"DropIndex": {
		ddl: dropIndexDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Tags", []string{"TagId", "Label"}, []any{int64(1), "Go"}),
				spanner.Insert("Tags", []string{"TagId", "Label"}, []any{int64(2), "Rust"}),
			},
		},
		readTable: "Tags",
		readIndex: "TagsByLabel",
		readCols:  []string{"TagId"},
		readKeys:  spanner.KeySets(spanner.Key{"Go"}),
		wantRows:  []string{"(1)"},
	},
}

// runIndexTest is the generic runner for index test cases.
func runIndexTest(ctx context.Context, t *testing.T, client *spanner.Client, tc indexTestCase) {
	t.Helper()

	for i, mutations := range tc.ops {
		if _, err := client.Apply(ctx, mutations); err != nil {
			t.Fatalf("ops[%d]: failed to apply mutations: %v", i, err)
		}
	}

	iter := client.Single().ReadUsingIndex(ctx, tc.readTable, tc.readIndex, tc.readKeys, tc.readCols)
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

func TestCompat_Indexes(t *testing.T) {
	for name, tc := range indexTests {
		t.Run(name, func(t *testing.T) {
			testutil.RunCompat(t, tc.ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
				runIndexTest(ctx, t, client, tc)
			})
		})
	}
}

func TestCompat_UniqueIndex(t *testing.T) {
	testutil.RunCompat(t, uniqueIndexDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Items", []string{"ItemId", "Code"}, []any{int64(1), "ABC"}),
		})
		if err != nil {
			t.Fatalf("failed to insert first row: %v", err)
		}

		// Inserting a row with duplicate Code should fail
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Items", []string{"ItemId", "Code"}, []any{int64(2), "ABC"}),
		})
		if err == nil {
			t.Fatal("expected UNIQUE violation error, got nil")
		}
	})
}

func TestCompat_IndexMutationSync(t *testing.T) {
	testutil.RunCompat(t, indexMutationSyncDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(1), "Song A", "Pop", int64(5)}),
			spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(2), "Song B", "Rock", int64(3)}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Update genre of Song 2 from Rock to Pop
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Update("Songs", []string{"SongId", "Genre"}, []any{int64(2), "Pop"}),
		})
		if err != nil {
			t.Fatalf("failed to update: %v", err)
		}

		// Now both songs should be Pop
		iter := client.Single().ReadUsingIndex(ctx, "Songs", "SongsByGenre",
			spanner.KeySets(spanner.Key{"Pop"}),
			[]string{"SongId"},
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
			t.Fatalf("expected 2 Pop songs after update, got %d", len(ids))
		}

		// Rock should have 0 songs
		iter2 := client.Single().ReadUsingIndex(ctx, "Songs", "SongsByGenre",
			spanner.KeySets(spanner.Key{"Rock"}),
			[]string{"SongId"},
		)
		defer iter2.Stop()

		var rockCount int
		for {
			_, err := iter2.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("failed to read rock: %v", err)
			}
			rockCount++
		}
		if rockCount != 0 {
			t.Errorf("expected 0 Rock songs after update, got %d", rockCount)
		}
	})
}
