package compattest

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
	"google.golang.org/api/iterator"
)

var arrayDDL = []string{
	`CREATE TABLE Items (
		ItemId   INT64 NOT NULL,
		Tags     ARRAY<STRING(MAX)>,
		Scores   ARRAY<INT64>,
	) PRIMARY KEY (ItemId)`,
}

func runArrayInsertAndRead(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items",
			[]string{"ItemId", "Tags", "Scores"},
			[]any{
				int64(1),
				[]string{"go", "spanner"},
				[]int64{10, 20, 30},
			},
		),
		spanner.Insert("Items",
			[]string{"ItemId", "Tags", "Scores"},
			[]any{
				int64(2),
				[]string{"cloud"},
				[]int64{},
			},
		),
		spanner.Insert("Items",
			[]string{"ItemId"},
			[]any{int64(3)},
		),
	})
	if err != nil {
		t.Fatalf("failed to apply mutations: %v", err)
	}

	iter := client.Single().Read(ctx, "Items",
		spanner.AllKeys(),
		[]string{"ItemId", "Tags", "Scores"},
	)
	defer iter.Stop()

	type Item struct {
		ID     int64
		Tags   []string
		Scores []int64
	}

	var items []Item
	err = iter.Do(func(row *spanner.Row) error {
		var item Item
		if err := row.Columns(&item.ID, &item.Tags, &item.Scores); err != nil {
			return err
		}
		items = append(items, item)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}

	// Item 1: tags=["go","spanner"], scores=[10,20,30]
	if len(items[0].Tags) != 2 || items[0].Tags[0] != "go" || items[0].Tags[1] != "spanner" {
		t.Errorf("item[0].Tags: got %v, want [go spanner]", items[0].Tags)
	}
	if len(items[0].Scores) != 3 || items[0].Scores[0] != 10 || items[0].Scores[1] != 20 || items[0].Scores[2] != 30 {
		t.Errorf("item[0].Scores: got %v, want [10 20 30]", items[0].Scores)
	}

	// Item 2: tags=["cloud"], scores=[]
	if len(items[1].Tags) != 1 || items[1].Tags[0] != "cloud" {
		t.Errorf("item[1].Tags: got %v, want [cloud]", items[1].Tags)
	}
	if len(items[1].Scores) != 0 {
		t.Errorf("item[1].Scores: got %v, want []", items[1].Scores)
	}

	// Item 3: tags=nil (NULL), scores=nil (NULL)
	if items[2].Tags != nil {
		t.Errorf("item[2].Tags: got %v, want nil", items[2].Tags)
	}
	if items[2].Scores != nil {
		t.Errorf("item[2].Scores: got %v, want nil", items[2].Scores)
	}
}

func TestCompat_ArrayInsertAndRead(t *testing.T) {
	testutil.RunCompat(t, arrayDDL, runArrayInsertAndRead)
}

func runArrayQuery(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items",
			[]string{"ItemId", "Tags", "Scores"},
			[]any{
				int64(1),
				[]string{"go", "spanner"},
				[]int64{10, 20, 30},
			},
		),
		spanner.Insert("Items",
			[]string{"ItemId", "Tags"},
			[]any{
				int64(2),
				[]string{"cloud"},
			},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	iter := client.Single().Query(ctx, spanner.NewStatement(
		"SELECT ItemId, Tags FROM Items ORDER BY ItemId",
	))
	defer iter.Stop()

	type Result struct {
		ID   int64
		Tags []string
	}

	var results []Result
	err = iter.Do(func(row *spanner.Row) error {
		var r Result
		if err := row.Columns(&r.ID, &r.Tags); err != nil {
			return err
		}
		results = append(results, r)
		return nil
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if len(results[0].Tags) != 2 || results[0].Tags[0] != "go" || results[0].Tags[1] != "spanner" {
		t.Errorf("result[0].Tags: got %v, want [go spanner]", results[0].Tags)
	}
	if len(results[1].Tags) != 1 || results[1].Tags[0] != "cloud" {
		t.Errorf("result[1].Tags: got %v, want [cloud]", results[1].Tags)
	}
}

func TestCompat_ArrayQuery(t *testing.T) {
	testutil.RunCompat(t, arrayDDL, runArrayQuery)
}

func runArrayUpdate(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Items",
			[]string{"ItemId", "Tags"},
			[]any{int64(1), []string{"initial"}},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Items",
			[]string{"ItemId", "Tags"},
			[]any{int64(1), []string{"updated", "tags"}},
		),
	})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	row, err := client.Single().ReadRow(ctx, "Items",
		spanner.Key{int64(1)},
		[]string{"Tags"},
	)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	var tags []string
	if err := row.Columns(&tags); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}
	if len(tags) != 2 || tags[0] != "updated" || tags[1] != "tags" {
		t.Errorf("Tags: got %v, want [updated tags]", tags)
	}
}

func TestCompat_ArrayUpdate(t *testing.T) {
	testutil.RunCompat(t, arrayDDL, runArrayUpdate)
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

func runAllArrayTypes(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
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
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	row, err := client.Single().ReadRow(ctx, "AllArrayTypes",
		spanner.Key{int64(1)},
		[]string{"Bools", "Ints", "Floats", "Strings", "Bytes"},
	)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	var bools []bool
	var ints []int64
	var floats []float64
	var strs []string
	var bs [][]byte
	if err := row.Columns(&bools, &ints, &floats, &strs, &bs); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if len(bools) != 3 || bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("Bools: got %v, want [true false true]", bools)
	}
	if len(ints) != 3 || ints[0] != 1 || ints[1] != 2 || ints[2] != 3 {
		t.Errorf("Ints: got %v, want [1 2 3]", ints)
	}
	if len(floats) != 2 || floats[0] != 1.1 || floats[1] != 2.2 {
		t.Errorf("Floats: got %v, want [1.1 2.2]", floats)
	}
	if len(strs) != 2 || strs[0] != "a" || strs[1] != "b" {
		t.Errorf("Strings: got %v, want [a b]", strs)
	}
	if len(bs) != 2 || string(bs[0]) != "x" || string(bs[1]) != "y" {
		t.Errorf("Bytes: got %v, want [[x] [y]]", bs)
	}
}

func TestCompat_AllArrayTypes(t *testing.T) {
	testutil.RunCompat(t, arrayAllTypesDDL, runAllArrayTypes)
}

var arrayWithNullsDDL = []string{
	`CREATE TABLE NullableArrays (
		Id   INT64 NOT NULL,
		Tags ARRAY<STRING(MAX)>,
	) PRIMARY KEY (Id)`,
}

func runArrayWithNullElements(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("NullableArrays",
			[]string{"Id", "Tags"},
			[]any{int64(1), []*string{strPtr("hello"), nil, strPtr("world")}},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	row, err := client.Single().ReadRow(ctx, "NullableArrays",
		spanner.Key{int64(1)},
		[]string{"Tags"},
	)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	var tags []*string
	if err := row.Columns(&tags); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if len(tags) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(tags))
	}
	if tags[0] == nil || *tags[0] != "hello" {
		t.Errorf("tags[0]: got %v, want \"hello\"", tags[0])
	}
	if tags[1] != nil {
		t.Errorf("tags[1]: got %v, want nil", tags[1])
	}
	if tags[2] == nil || *tags[2] != "world" {
		t.Errorf("tags[2]: got %v, want \"world\"", tags[2])
	}
}

func TestCompat_ArrayWithNullElements(t *testing.T) {
	testutil.RunCompat(t, arrayWithNullsDDL, runArrayWithNullElements)
}

func strPtr(s string) *string {
	return &s
}

var arraySelectStarDDL = []string{
	`CREATE TABLE TaggedItems (
		Id   INT64 NOT NULL,
		Name STRING(256) NOT NULL,
		Tags ARRAY<STRING(MAX)>,
	) PRIMARY KEY (Id)`,
}

func runArraySelectStar(ctx context.Context, t *testing.T, client *spanner.Client) {
	t.Helper()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("TaggedItems",
			[]string{"Id", "Name", "Tags"},
			[]any{int64(1), "item1", []string{"a", "b"}},
		),
	})
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	iter := client.Single().Query(ctx, spanner.NewStatement(
		"SELECT * FROM TaggedItems",
	))
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Fatalf("failed to get row: %v", err)
	}

	var id int64
	var name string
	var tags []string
	if err := row.Columns(&id, &name, &tags); err != nil {
		t.Fatalf("failed to scan: %v", err)
	}

	if id != 1 || name != "item1" {
		t.Errorf("got id=%d name=%q", id, name)
	}
	if len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("Tags: got %v, want [a b]", tags)
	}

	_, err = iter.Next()
	if err != iterator.Done {
		t.Fatalf("expected iterator.Done, got %v", err)
	}
}

func TestCompat_ArraySelectStar(t *testing.T) {
	testutil.RunCompat(t, arraySelectStarDDL, runArraySelectStar)
}
