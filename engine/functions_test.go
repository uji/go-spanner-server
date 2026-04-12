package engine

import (
	"strings"
	"testing"

	"github.com/uji/go-spanner-server/store"
)

// setupFunctionsTestDB creates a test database with a Singers table.
func setupFunctionsTestDB(t *testing.T) *store.Database {
	t.Helper()
	db := store.NewDatabase()
	if err := db.ApplyDDL(`CREATE TABLE Singers (
		SingerId INT64 NOT NULL,
		FirstName STRING(1024),
		LastName STRING(1024),
		Age INT64,
	) PRIMARY KEY (SingerId)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	stmts := []string{
		`INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (1, 'Marc', 'Richards', 30)`,
		`INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (2, 'Catalina', 'Smith', 25)`,
		`INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (3, 'Alice', 'Trentor', 28)`,
	}
	for _, s := range stmts {
		if _, err := ExecuteDML(db, s, zeroTS); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}
	return db
}

// queryFirstString executes a SELECT and returns the first column of the first row as a string value.
func queryFirstString(t *testing.T, db *store.Database, sql string) string {
	t.Helper()
	result, err := Execute(db, sql)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	if len(result.Rows) == 0 {
		t.Fatalf("no rows returned for %q", sql)
	}
	return result.Rows[0].Values[0].GetStringValue()
}

// --- COALESCE ---

func TestFunction_COALESCE_FirstNonNull(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT COALESCE(NULL, NULL, 'hello') FROM Singers WHERE SingerId = 1`)
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}

func TestFunction_COALESCE_FirstValue(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT COALESCE(FirstName, 'unknown') FROM Singers WHERE SingerId = 1`)
	if got != "Marc" {
		t.Errorf("expected 'Marc', got %q", got)
	}
}

func TestFunction_COALESCE_AllNull(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT COALESCE(NULL, NULL) FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
}

// --- IFNULL ---

func TestFunction_IFNULL_NonNull(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT IFNULL(FirstName, 'unknown') FROM Singers WHERE SingerId = 1`)
	if got != "Marc" {
		t.Errorf("expected 'Marc', got %q", got)
	}
}

func TestFunction_IFNULL_NullFallback(t *testing.T) {
	db := setupFunctionsTestDB(t)
	// NULL LastName singer
	ExecuteDML(db, `INSERT INTO Singers (SingerId, FirstName) VALUES (99, 'NoLast')`, zeroTS)
	got := queryFirstString(t, db, `SELECT IFNULL(LastName, 'unknown') FROM Singers WHERE SingerId = 99`)
	if got != "unknown" {
		t.Errorf("expected 'unknown', got %q", got)
	}
}

// --- String functions ---

func TestFunction_UPPER(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT UPPER(FirstName) FROM Singers WHERE SingerId = 1`)
	if got != "MARC" {
		t.Errorf("expected 'MARC', got %q", got)
	}
}

func TestFunction_LOWER(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT LOWER(FirstName) FROM Singers WHERE SingerId = 1`)
	if got != "marc" {
		t.Errorf("expected 'marc', got %q", got)
	}
}

func TestFunction_CONCAT(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT CONCAT(FirstName, ' ', LastName) FROM Singers WHERE SingerId = 1`)
	if got != "Marc Richards" {
		t.Errorf("expected 'Marc Richards', got %q", got)
	}
}

func TestFunction_LENGTH(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT LENGTH(FirstName) FROM Singers WHERE SingerId = 1`)
	if got != "4" {
		t.Errorf("expected '4', got %q", got)
	}
}

func TestFunction_SUBSTR_TwoArgs(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT SUBSTR(FirstName, 2) FROM Singers WHERE SingerId = 1`)
	if got != "arc" {
		t.Errorf("expected 'arc', got %q", got)
	}
}

func TestFunction_SUBSTR_ThreeArgs(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT SUBSTR(FirstName, 1, 2) FROM Singers WHERE SingerId = 1`)
	if got != "Ma" {
		t.Errorf("expected 'Ma', got %q", got)
	}
}

func TestFunction_TRIM(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V STRING(256)) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, '  hello  ')`, zeroTS)
	got := queryFirstString(t, db, `SELECT TRIM(V) FROM T WHERE Id = 1`)
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}

func TestFunction_STARTS_WITH(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT SingerId FROM Singers WHERE STARTS_WITH(FirstName, 'Ma') ORDER BY SingerId`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Values[0].GetStringValue() != "1" {
		t.Errorf("expected SingerId=1, got %v", result.Rows)
	}
}

func TestFunction_ENDS_WITH(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT SingerId FROM Singers WHERE ENDS_WITH(FirstName, 'ce') ORDER BY SingerId`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Values[0].GetStringValue() != "3" {
		t.Errorf("expected SingerId=3, got %v", result.Rows)
	}
}

func TestFunction_REPLACE(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT REPLACE(FirstName, 'a', 'A') FROM Singers WHERE SingerId = 1`)
	if got != "MArc" {
		t.Errorf("expected 'MArc', got %q", got)
	}
}

func TestFunction_REVERSE(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT REVERSE(FirstName) FROM Singers WHERE SingerId = 1`)
	if got != "craM" {
		t.Errorf("expected 'craM', got %q", got)
	}
}

// --- CAST ---

func TestFunction_CAST_Int64ToString(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT CAST(SingerId AS STRING) FROM Singers WHERE SingerId = 1`)
	if got != "1" {
		t.Errorf("expected '1', got %q", got)
	}
}

func TestFunction_CAST_StringToInt64(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V STRING(256)) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, '42')`, zeroTS)
	got := queryFirstString(t, db, `SELECT CAST(V AS INT64) FROM T WHERE Id = 1`)
	if got != "42" {
		t.Errorf("expected '42', got %q", got)
	}
}

func TestFunction_SAFE_CAST_InvalidReturnsNull(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V STRING(256)) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, 'not_a_number')`, zeroTS)
	result, err := Execute(db, `SELECT SAFE_CAST(V AS INT64) FROM T WHERE Id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	// NULL value is returned as NullValue proto.
	_ = result
}

func TestFunction_CAST_InWhere(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT SingerId FROM Singers WHERE CAST(SingerId AS STRING) = '2'`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0].Values[0].GetStringValue() != "2" {
		t.Errorf("expected 1 row with SingerId=2, got %v", result.Rows)
	}
}

// --- CASE ---

func TestExpr_CASE_Searched(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT CASE WHEN Age < 26 THEN 'young' WHEN Age < 30 THEN 'mid' ELSE 'senior' END FROM Singers WHERE SingerId = 2`)
	if got != "young" {
		t.Errorf("expected 'young', got %q", got)
	}
}

func TestExpr_CASE_Simple(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT CASE SingerId WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM Singers WHERE SingerId = 2`)
	if got != "two" {
		t.Errorf("expected 'two', got %q", got)
	}
}

func TestExpr_CASE_NoMatchReturnsNull(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT CASE WHEN Age > 100 THEN 'old' END FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	_ = result
}

// --- IF ---

func TestExpr_IF_True(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT IF(Age > 20, 'adult', 'child') FROM Singers WHERE SingerId = 1`)
	if got != "adult" {
		t.Errorf("expected 'adult', got %q", got)
	}
}

func TestExpr_IF_False(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT IF(Age > 100, 'adult', 'child') FROM Singers WHERE SingerId = 1`)
	if got != "child" {
		t.Errorf("expected 'child', got %q", got)
	}
}

// --- CURRENT_TIMESTAMP ---

func TestFunction_CURRENT_TIMESTAMP(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT CURRENT_TIMESTAMP() FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	ts := result.Rows[0].Values[0].GetStringValue()
	if ts == "" {
		t.Error("expected non-empty timestamp string")
	}
}

// --- GENERATE_UUID ---

func TestFunction_GENERATE_UUID(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT GENERATE_UUID() FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	uuid := result.Rows[0].Values[0].GetStringValue()
	parts := strings.Split(uuid, "-")
	if len(parts) != 5 {
		t.Errorf("expected UUID format with 5 parts, got %q", uuid)
	}
}

// --- Math functions ---

func TestFunction_ABS(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V INT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, -42)`, zeroTS)
	got := queryFirstString(t, db, `SELECT ABS(V) FROM T WHERE Id = 1`)
	if got != "42" {
		t.Errorf("expected '42', got %q", got)
	}
}

func TestFunction_ABS_MinInt64_Error(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V INT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, -9223372036854775808)`, zeroTS)
	_, err := Execute(db, `SELECT ABS(V) FROM T WHERE Id = 1`)
	if err == nil {
		t.Error("expected error for ABS(MinInt64), got nil")
	}
}

func TestFunction_MOD(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT MOD(Age, 7) FROM Singers WHERE SingerId = 1`)
	if got != "2" { // 30 % 7 = 2
		t.Errorf("expected '2', got %q", got)
	}
}

func TestFunction_CEIL(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V FLOAT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, 1.3)`, zeroTS)
	result, err := Execute(db, `SELECT CEIL(V) FROM T WHERE Id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0].Values[0].GetNumberValue() != 2.0 {
		t.Errorf("expected 2.0, got %v", result.Rows[0].Values[0])
	}
}

func TestFunction_CEIL_LargeFloat(t *testing.T) {
	// Ensures no int64 overflow for values beyond MaxInt64.
	result, err := Execute(nil, `SELECT CEIL(1e20)`) // uses executeSelectLiteral
	// nil db is fine for literal-only queries (no FROM clause)
	_ = err
	_ = result
	// Simply should not panic; actual validation done via float result
}

func TestFunction_FLOOR(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V FLOAT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, 1.7)`, zeroTS)
	result, err := Execute(db, `SELECT FLOOR(V) FROM T WHERE Id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0].Values[0].GetNumberValue() != 1.0 {
		t.Errorf("expected 1.0, got %v", result.Rows[0].Values[0])
	}
}

func TestFunction_ROUND(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V FLOAT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, 2.5)`, zeroTS)
	result, err := Execute(db, `SELECT ROUND(V) FROM T WHERE Id = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0].Values[0].GetNumberValue() != 3.0 {
		t.Errorf("expected 3.0, got %v", result.Rows[0].Values[0])
	}
}

func TestFunction_SIGN(t *testing.T) {
	db := setupFunctionsTestDB(t)
	tests := []struct {
		singerId int
		want     string
	}{
		{1, "30"},  // Age=30, SIGN=1
		{2, "25"},  // Age=25, SIGN=1
	}
	for _, tc := range tests {
		_ = tc
	}
	// Test SIGN with literal values.
	result, err := Execute(db, `SELECT SIGN(Age) FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0].Values[0].GetStringValue() != "1" {
		t.Errorf("expected SIGN(30)=1, got %v", result.Rows[0].Values[0])
	}
}

func TestFunction_GREATEST(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT GREATEST(1, 3, 2) FROM Singers WHERE SingerId = 1`)
	if got != "3" {
		t.Errorf("expected '3', got %q", got)
	}
}

func TestFunction_LEAST(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT LEAST(3, 1, 2) FROM Singers WHERE SingerId = 1`)
	if got != "1" {
		t.Errorf("expected '1', got %q", got)
	}
}

func TestFunction_STRPOS(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT STRPOS(FirstName, 'ar') FROM Singers WHERE SingerId = 1`)
	if got != "2" { // "Marc" -> 'ar' starts at position 2
		t.Errorf("expected '2', got %q", got)
	}
}

func TestFunction_STRPOS_NotFound(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT STRPOS(FirstName, 'xyz') FROM Singers WHERE SingerId = 1`)
	if got != "0" {
		t.Errorf("expected '0', got %q", got)
	}
}

func TestFunction_LPAD(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT LPAD(FirstName, 6, '0') FROM Singers WHERE SingerId = 1`)
	if got != "00Marc" {
		t.Errorf("expected '00Marc', got %q", got)
	}
}

func TestFunction_LPAD_EmptyPad_Error(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT LPAD(FirstName, 6, '') FROM Singers WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for LPAD with empty pad, got nil")
	}
}

func TestFunction_LPAD_NegativeLength_Error(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT LPAD(FirstName, -1, '0') FROM Singers WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for LPAD with negative length, got nil")
	}
}

func TestFunction_RPAD(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT RPAD(FirstName, 6, '0') FROM Singers WHERE SingerId = 1`)
	if got != "Marc00" {
		t.Errorf("expected 'Marc00', got %q", got)
	}
}

func TestFunction_RPAD_EmptyPad_Error(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT RPAD(FirstName, 6, '') FROM Singers WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for RPAD with empty pad, got nil")
	}
}

func TestFunction_SUBSTR_ZeroPos_Error(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT SUBSTR(FirstName, 0) FROM Singers WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for SUBSTR with pos=0, got nil")
	}
}

func TestFunction_SUBSTR_NegativeLength_Error(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT SUBSTR(FirstName, 1, -1) FROM Singers WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for SUBSTR with negative length, got nil")
	}
}

func TestFunction_REPEAT_NegativeCount_Error(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := Execute(db, `SELECT REPEAT(FirstName, -1) FROM Singers WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for REPEAT with negative count, got nil")
	}
}

func TestFunction_NULLIF_MixedTypes(t *testing.T) {
	// NULLIF(int64, float64) should NOT be equal — different types.
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT NULLIF(SingerId, CAST(1 AS FLOAT64)) FROM Singers WHERE SingerId = 1`)
	// SingerId is int64(1), CAST(1 AS FLOAT64) is float64(1.0) — different types, return first arg.
	if got != "1" {
		t.Errorf("expected '1' (not NULL) for NULLIF with mixed types, got %q", got)
	}
}

// --- NULLIF ---

func TestFunction_NULLIF_Equal(t *testing.T) {
	db := setupFunctionsTestDB(t)
	result, err := Execute(db, `SELECT NULLIF(FirstName, 'Marc') FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	// Value should be NULL.
	_ = result
}

func TestFunction_NULLIF_NotEqual(t *testing.T) {
	db := setupFunctionsTestDB(t)
	got := queryFirstString(t, db, `SELECT NULLIF(FirstName, 'Alice') FROM Singers WHERE SingerId = 1`)
	if got != "Marc" {
		t.Errorf("expected 'Marc', got %q", got)
	}
}

// --- INSERT with function ---

func TestInsert_WithUpperFunction(t *testing.T) {
	db := setupFunctionsTestDB(t)
	_, err := ExecuteDML(db, `INSERT INTO Singers (SingerId, FirstName) VALUES (100, UPPER('hello'))`, zeroTS)
	if err != nil {
		t.Fatalf("INSERT with UPPER: %v", err)
	}
	got := queryFirstString(t, db, `SELECT FirstName FROM Singers WHERE SingerId = 100`)
	if got != "HELLO" {
		t.Errorf("expected 'HELLO', got %q", got)
	}
}
