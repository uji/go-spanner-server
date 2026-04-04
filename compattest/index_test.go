package compattest

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
	"google.golang.org/api/iterator"
)

var indexDDL = []string{
	`CREATE TABLE Songs (
		SongId INT64 NOT NULL,
		Title STRING(256),
		Genre STRING(64),
		Rating INT64,
	) PRIMARY KEY (SongId)`,
	`CREATE INDEX SongsByGenre ON Songs(Genre)`,
}

func TestCompat_CreateIndexAndReadByIndex(t *testing.T) {
	testutil.RunCompat(t, indexDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(1), "Song A", "Pop", int64(5)}),
			spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(2), "Song B", "Rock", int64(3)}),
			spanner.Insert("Songs", []string{"SongId", "Title", "Genre", "Rating"}, []any{int64(3), "Song C", "Pop", int64(4)}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// ReadUsingIndex with key lookup (only index key + PK columns are readable)
		iter := client.Single().ReadUsingIndex(ctx, "Songs", "SongsByGenre",
			spanner.KeySets(spanner.Key{"Pop"}),
			[]string{"SongId", "Genre"},
		)
		defer iter.Stop()

		type Song struct {
			SongId int64
			Genre  string
		}
		var songs []Song
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("failed to read: %v", err)
			}
			var s Song
			if err := row.Columns(&s.SongId, &s.Genre); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
			songs = append(songs, s)
		}

		if len(songs) != 2 {
			t.Fatalf("expected 2 songs with Genre=Pop, got %d", len(songs))
		}
		for _, s := range songs {
			if s.Genre != "Pop" {
				t.Errorf("expected Genre=Pop, got %q", s.Genre)
			}
		}
	})
}

var uniqueIndexDDL = []string{
	`CREATE TABLE Items (
		ItemId INT64 NOT NULL,
		Code STRING(64),
	) PRIMARY KEY (ItemId)`,
	`CREATE UNIQUE INDEX ItemsByCode ON Items(Code)`,
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

var nullFilteredIndexDDL = []string{
	`CREATE TABLE Products (
		ProductId INT64 NOT NULL,
		Category STRING(64),
	) PRIMARY KEY (ProductId)`,
	`CREATE NULL_FILTERED INDEX ProductsByCategory ON Products(Category)`,
}

func TestCompat_NullFilteredIndex(t *testing.T) {
	testutil.RunCompat(t, nullFilteredIndexDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Products", []string{"ProductId", "Category"}, []any{int64(1), "Electronics"}),
			spanner.Insert("Products", []string{"ProductId"}, []any{int64(2)}), // NULL Category
			spanner.Insert("Products", []string{"ProductId", "Category"}, []any{int64(3), "Books"}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// ReadUsingIndex should not return the NULL row
		iter := client.Single().ReadUsingIndex(ctx, "Products", "ProductsByCategory",
			spanner.AllKeys(),
			[]string{"ProductId", "Category"},
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
		if count != 2 {
			t.Errorf("expected 2 rows (NULL filtered out), got %d", count)
		}
	})
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

func TestCompat_IndexWithStoring(t *testing.T) {
	testutil.RunCompat(t, storingIndexDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Employees", []string{"EmpId", "Name", "Dept", "Salary"}, []any{int64(1), "Alice", "Eng", int64(100)}),
			spanner.Insert("Employees", []string{"EmpId", "Name", "Dept", "Salary"}, []any{int64(2), "Bob", "Sales", int64(80)}),
			spanner.Insert("Employees", []string{"EmpId", "Name", "Dept", "Salary"}, []any{int64(3), "Carol", "Eng", int64(120)}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		iter := client.Single().ReadUsingIndex(ctx, "Employees", "EmployeesByDept",
			spanner.KeySets(spanner.Key{"Eng"}),
			[]string{"EmpId", "Dept", "Salary"},
		)
		defer iter.Stop()

		type Result struct {
			EmpId  int64
			Dept   string
			Salary int64
		}
		var results []Result
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("failed to read: %v", err)
			}
			var r Result
			if err := row.Columns(&r.EmpId, &r.Dept, &r.Salary); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
			results = append(results, r)
		}

		if len(results) != 2 {
			t.Fatalf("expected 2 Eng employees, got %d", len(results))
		}
	})
}

var indexRangeScanDDL = []string{
	`CREATE TABLE Scores (
		ScoreId INT64 NOT NULL,
		Value INT64,
	) PRIMARY KEY (ScoreId)`,
	`CREATE INDEX ScoresByValue ON Scores(Value)`,
}

func TestCompat_IndexRangeScan(t *testing.T) {
	testutil.RunCompat(t, indexRangeScanDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(1), int64(10)}),
			spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(2), int64(20)}),
			spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(3), int64(30)}),
			spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(4), int64(40)}),
			spanner.Insert("Scores", []string{"ScoreId", "Value"}, []any{int64(5), int64(50)}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Range scan: Value in [20, 40]
		iter := client.Single().ReadUsingIndex(ctx, "Scores", "ScoresByValue",
			spanner.KeySets(spanner.KeyRange{
				Start: spanner.Key{int64(20)},
				End:   spanner.Key{int64(40)},
				Kind:  spanner.ClosedClosed,
			}),
			[]string{"ScoreId", "Value"},
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
			var id, val int64
			if err := row.Columns(&id, &val); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
			ids = append(ids, id)
		}

		if len(ids) != 3 {
			t.Fatalf("expected 3 rows in range [20,40], got %d", len(ids))
		}
	})
}

var descIndexDDL = []string{
	`CREATE TABLE Events (
		EventId INT64 NOT NULL,
		Priority INT64,
	) PRIMARY KEY (EventId)`,
	`CREATE INDEX EventsByPriorityDesc ON Events(Priority DESC)`,
}

func TestCompat_DescIndex(t *testing.T) {
	testutil.RunCompat(t, descIndexDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Events", []string{"EventId", "Priority"}, []any{int64(1), int64(10)}),
			spanner.Insert("Events", []string{"EventId", "Priority"}, []any{int64(2), int64(30)}),
			spanner.Insert("Events", []string{"EventId", "Priority"}, []any{int64(3), int64(20)}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// ReadUsingIndex with AllKeys should return rows in DESC Priority order
		iter := client.Single().ReadUsingIndex(ctx, "Events", "EventsByPriorityDesc",
			spanner.AllKeys(),
			[]string{"EventId", "Priority"},
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
			var id, priority int64
			if err := row.Columns(&id, &priority); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
			ids = append(ids, id)
		}

		if len(ids) != 3 {
			t.Fatalf("expected 3 events, got %d", len(ids))
		}
		// DESC order: Priority 30 (EventId=2), 20 (EventId=3), 10 (EventId=1)
		if ids[0] != 2 || ids[1] != 3 || ids[2] != 1 {
			t.Errorf("expected DESC order [2,3,1], got %v", ids)
		}
	})
}

var dropIndexDDL = []string{
	`CREATE TABLE Tags (
		TagId INT64 NOT NULL,
		Label STRING(64),
	) PRIMARY KEY (TagId)`,
	`CREATE INDEX TagsByLabel ON Tags(Label)`,
}

func TestCompat_DropIndex(t *testing.T) {
	testutil.RunCompat(t, dropIndexDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		// Insert data and verify index works
		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Tags", []string{"TagId", "Label"}, []any{int64(1), "Go"}),
			spanner.Insert("Tags", []string{"TagId", "Label"}, []any{int64(2), "Rust"}),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Verify index works before dropping
		iter := client.Single().ReadUsingIndex(ctx, "Tags", "TagsByLabel",
			spanner.KeySets(spanner.Key{"Go"}),
			[]string{"TagId"},
		)
		var count int
		for {
			_, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("failed to read via index: %v", err)
			}
			count++
		}
		iter.Stop()
		if count != 1 {
			t.Fatalf("expected 1 row via index, got %d", count)
		}
	})
}

var indexMutationSyncDDL = indexDDL

func TestCompat_IndexMutationSync(t *testing.T) {
	testutil.RunCompat(t, indexMutationSyncDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		t.Helper()

		// Insert rows
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
