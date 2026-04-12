package engine

import (
	"strconv"
	"testing"

	"github.com/uji/go-spanner-server/store"
)

func setupTestDB(t *testing.T) *store.Database {
	t.Helper()
	db := store.NewDatabase()
	ddls := []string{
		`CREATE TABLE Singers (
			SingerId INT64 NOT NULL,
			FirstName STRING(1024),
			LastName STRING(1024),
		) PRIMARY KEY (SingerId)`,
	}
	for _, ddl := range ddls {
		if err := db.ApplyDDL(ddl); err != nil {
			t.Fatalf("failed to apply DDL: %v", err)
		}
	}

	table, _ := db.GetTable("Singers")
	// SingerId=1, FirstName="Marc", LastName="Richards"
	if err := table.InsertRow([]string{"SingerId", "FirstName", "LastName"}, []any{int64(1), "Marc", "Richards"}); err != nil {
		t.Fatal(err)
	}
	// SingerId=2, FirstName="Catalina", LastName="Smith"
	if err := table.InsertRow([]string{"SingerId", "FirstName", "LastName"}, []any{int64(2), "Catalina", "Smith"}); err != nil {
		t.Fatal(err)
	}
	// SingerId=3, FirstName="Alice", LastName=NULL
	if err := table.InsertRow([]string{"SingerId", "FirstName"}, []any{int64(3), "Alice"}); err != nil {
		t.Fatal(err)
	}
	// SingerId=4, FirstName="Maria", LastName="Garcia"
	if err := table.InsertRow([]string{"SingerId", "FirstName", "LastName"}, []any{int64(4), "Maria", "Garcia"}); err != nil {
		t.Fatal(err)
	}

	return db
}

func queryIDs(t *testing.T, db *store.Database, sql string) []int64 {
	t.Helper()
	result, err := Execute(db, sql)
	if err != nil {
		t.Fatalf("Execute(%q) failed: %v", sql, err)
	}
	var ids []int64
	for _, row := range result.Rows {
		if len(row.Values) == 0 {
			t.Fatal("empty row")
		}
		v := row.Values[0].GetStringValue()
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("failed to parse id %q: %v", v, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestWhere_Equal(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId = 1")
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("expected [1], got %v", ids)
	}
}

func TestWhere_NotEqual(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId != 1")
	if len(ids) != 3 {
		t.Errorf("expected 3 rows, got %v", ids)
	}
}

func TestWhere_LessThan(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId < 3")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("expected [1,2], got %v", ids)
	}
}

func TestWhere_GreaterThan(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId > 2")
	if len(ids) != 2 || ids[0] != 3 || ids[1] != 4 {
		t.Errorf("expected [3,4], got %v", ids)
	}
}

func TestWhere_LessEqual(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId <= 2")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("expected [1,2], got %v", ids)
	}
}

func TestWhere_GreaterEqual(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId >= 3")
	if len(ids) != 2 || ids[0] != 3 || ids[1] != 4 {
		t.Errorf("expected [3,4], got %v", ids)
	}
}

func TestWhere_And(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId > 1 AND SingerId < 4")
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Errorf("expected [2,3], got %v", ids)
	}
}

func TestWhere_Or(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId = 1 OR SingerId = 4")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 4 {
		t.Errorf("expected [1,4], got %v", ids)
	}
}

func TestWhere_Not(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE NOT SingerId = 1")
	if len(ids) != 3 {
		t.Errorf("expected 3 rows, got %v", ids)
	}
}

func TestWhere_IsNull(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE LastName IS NULL")
	if len(ids) != 1 || ids[0] != 3 {
		t.Errorf("expected [3], got %v", ids)
	}
}

func TestWhere_IsNotNull(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE LastName IS NOT NULL")
	if len(ids) != 3 {
		t.Errorf("expected 3 rows, got %v", ids)
	}
}

func TestWhere_In(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId IN (1, 3)")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 3 {
		t.Errorf("expected [1,3], got %v", ids)
	}
}

func TestWhere_NotIn(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId NOT IN (1, 3)")
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 4 {
		t.Errorf("expected [2,4], got %v", ids)
	}
}

func TestWhere_Between(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId BETWEEN 2 AND 3")
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Errorf("expected [2,3], got %v", ids)
	}
}

func TestWhere_NotBetween(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId NOT BETWEEN 2 AND 3")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 4 {
		t.Errorf("expected [1,4], got %v", ids)
	}
}

func TestWhere_Like(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE FirstName LIKE 'Ma%'")
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 4 {
		t.Errorf("expected [1,4], got %v", ids)
	}
}

func TestWhere_NotLike(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE FirstName NOT LIKE 'Ma%'")
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Errorf("expected [2,3], got %v", ids)
	}
}

func TestWhere_ParenExpr(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE (SingerId = 1 OR SingerId = 2) AND SingerId < 2")
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("expected [1], got %v", ids)
	}
}

func TestWhere_NullComparisonEqualsNull(t *testing.T) {
	db := setupTestDB(t)
	// NULL = NULL should return unknown/false per SQL semantics
	// SingerId=3 has LastName=NULL, but this should NOT match
	result, err := Execute(db, "SELECT SingerId FROM Singers WHERE LastName = LastName")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	// Row 3 (LastName=NULL) should be excluded; rows 1,2,4 should match
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows (excluding NULL row), got %d", len(result.Rows))
	}
}

func TestWhere_LikeUnderscore(t *testing.T) {
	db := setupTestDB(t)
	// '_arc' matches 'Marc'
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE FirstName LIKE '_arc'")
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("expected [1], got %v", ids)
	}
}

func TestOrderBy_Asc(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers ORDER BY SingerId")
	want := []int64{1, 2, 3, 4}
	if len(ids) != len(want) {
		t.Fatalf("expected %v, got %v", want, ids)
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("expected ids %v, got %v", want, ids)
			break
		}
	}
}

func TestOrderBy_Desc(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers ORDER BY SingerId DESC")
	want := []int64{4, 3, 2, 1}
	if len(ids) != len(want) {
		t.Fatalf("expected %v, got %v", want, ids)
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("expected ids %v, got %v", want, ids)
			break
		}
	}
}

func TestOrderBy_StringColumn(t *testing.T) {
	db := setupTestDB(t)
	result, err := Execute(db, "SELECT FirstName FROM Singers WHERE LastName IS NOT NULL ORDER BY FirstName")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	want := []string{"Catalina", "Marc", "Maria"}
	if len(result.Rows) != len(want) {
		t.Fatalf("expected %d rows, got %d", len(want), len(result.Rows))
	}
	for i, row := range result.Rows {
		got := row.Values[0].GetStringValue()
		if got != want[i] {
			t.Errorf("row %d: expected %q, got %q", i, want[i], got)
		}
	}
}

func TestOrderBy_NullHandling(t *testing.T) {
	db := setupTestDB(t)
	// NULL should come first in ASC order
	result, err := Execute(db, "SELECT SingerId, LastName FROM Singers ORDER BY LastName")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
	// First row should be the NULL LastName (SingerId=3)
	firstID := result.Rows[0].Values[0].GetStringValue()
	if firstID != "3" {
		t.Errorf("expected NULL row (SingerId=3) first, got SingerId=%s", firstID)
	}
}

func TestOrderBy_Int64NumericComparison(t *testing.T) {
	// Verify that INT64 ORDER BY uses numeric comparison, not lexicographic.
	db := store.NewDatabase()
	ddls := []string{
		`CREATE TABLE Items (
			ItemId INT64 NOT NULL,
			Name STRING(1024),
		) PRIMARY KEY (ItemId)`,
	}
	for _, ddl := range ddls {
		if err := db.ApplyDDL(ddl); err != nil {
			t.Fatalf("failed to apply DDL: %v", err)
		}
	}
	table, _ := db.GetTable("Items")
	for _, id := range []int64{1, 2, 10, 20, 100} {
		if err := table.InsertRow([]string{"ItemId", "Name"}, []any{id, "item"}); err != nil {
			t.Fatal(err)
		}
	}

	ids := queryIDs(t, db, "SELECT ItemId FROM Items ORDER BY ItemId")
	want := []int64{1, 2, 10, 20, 100}
	if len(ids) != len(want) {
		t.Fatalf("expected %v, got %v", want, ids)
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("expected ids %v, got %v (numeric ORDER BY broken)", want, ids)
			break
		}
	}
}

func TestOrderBy_WithWhere(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers WHERE SingerId > 1 ORDER BY SingerId DESC")
	want := []int64{4, 3, 2}
	if len(ids) != len(want) {
		t.Fatalf("expected %v, got %v", want, ids)
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("expected ids %v, got %v", want, ids)
			break
		}
	}
}

// --- LIMIT / OFFSET tests ---

func TestLimit_Basic(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 2")
	if len(ids) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(ids), ids)
	}
	if ids[0] != 1 || ids[1] != 2 {
		t.Errorf("expected [1 2], got %v", ids)
	}
}

func TestLimit_WithOffset(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 2 OFFSET 1")
	if len(ids) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(ids), ids)
	}
	if ids[0] != 2 || ids[1] != 3 {
		t.Errorf("expected [2 3], got %v", ids)
	}
}

func TestLimit_OffsetBeyondEnd(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 10 OFFSET 100")
	if len(ids) != 0 {
		t.Errorf("expected 0 rows, got %d: %v", len(ids), ids)
	}
}

func TestLimit_Zero(t *testing.T) {
	db := setupTestDB(t)
	ids := queryIDs(t, db, "SELECT SingerId FROM Singers ORDER BY SingerId LIMIT 0")
	if len(ids) != 0 {
		t.Errorf("expected 0 rows, got %d: %v", len(ids), ids)
	}
}

// --- SELECT expression tests ---

func TestSelect_ExpressionInList(t *testing.T) {
	db := setupTestDB(t)
	result, err := Execute(db, `SELECT SingerId * 2 AS doubled FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	got := result.Rows[0].Values[0].GetStringValue()
	if got != "2" {
		t.Errorf("expected SingerId*2=2, got %q", got)
	}
}

func TestSelect_LiteralWithoutFrom(t *testing.T) {
	db := store.NewDatabase()
	result, err := Execute(db, `SELECT 42, 'hello'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got := result.Rows[0].Values[0].GetStringValue(); got != "42" {
		t.Errorf("expected '42', got %q", got)
	}
	if got := result.Rows[0].Values[1].GetStringValue(); got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}
