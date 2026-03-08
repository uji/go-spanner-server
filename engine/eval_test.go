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
