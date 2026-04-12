package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/uji/go-spanner-server/store"
)

// zeroTS is a zero commit timestamp used in tests that do not exercise PENDING_COMMIT_TIMESTAMP().
var zeroTS = time.Time{}

func setupCommitTSTestDB(t *testing.T) *store.Database {
	t.Helper()
	db := store.NewDatabase()
	ddls := []string{
		`CREATE TABLE Events (
			EventId INT64 NOT NULL,
			Name STRING(256),
			CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true),
			UpdatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true),
		) PRIMARY KEY (EventId)`,
	}
	for _, ddl := range ddls {
		if err := db.ApplyDDL(ddl); err != nil {
			t.Fatalf("failed to apply DDL: %v", err)
		}
	}
	return db
}

func setupDMLTestDB(t *testing.T) *store.Database {
	t.Helper()
	db := store.NewDatabase()
	ddls := []string{
		`CREATE TABLE Singers (
			SingerId INT64 NOT NULL,
			FirstName STRING(1024),
			LastName STRING(1024),
			Age INT64,
		) PRIMARY KEY (SingerId)`,
	}
	for _, ddl := range ddls {
		if err := db.ApplyDDL(ddl); err != nil {
			t.Fatalf("failed to apply DDL: %v", err)
		}
	}
	return db
}

func insertTestRows(t *testing.T, db *store.Database) {
	t.Helper()
	stmts := []string{
		`INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (1, 'Marc', 'Richards', 30)`,
		`INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (2, 'Catalina', 'Smith', 25)`,
		`INSERT INTO Singers (SingerId, FirstName, LastName, Age) VALUES (3, 'Alice', 'Trentor', 28)`,
	}
	for _, s := range stmts {
		if _, err := ExecuteDML(db, s, zeroTS); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}
}

// --- UPDATE tests ---

func TestUpdate_Basic(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `UPDATE Singers SET LastName = 'Johnson' WHERE SingerId = 1`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 affected row, got %d", n)
	}

	result, err := Execute(db, `SELECT LastName FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0].Values[0].GetStringValue(); got != "Johnson" {
		t.Errorf("expected 'Johnson', got %q", got)
	}
}

func TestUpdate_MultipleColumns(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `UPDATE Singers SET FirstName = 'Bob', LastName = 'Williams' WHERE SingerId = 2`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// One row matched by WHERE SingerId = 2.
	if n != 1 {
		t.Errorf("expected 1 affected row, got %d", n)
	}

	result, err := Execute(db, `SELECT FirstName FROM Singers WHERE SingerId = 2`)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0].Values[0].GetStringValue(); got != "Bob" {
		t.Errorf("expected FirstName='Bob', got %q", got)
	}
}

func TestUpdate_ExpressionInSet(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age + 10 WHERE SingerId = 1`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := Execute(db, `SELECT Age FROM Singers WHERE SingerId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0].Values[0].GetStringValue(); got != "40" {
		t.Errorf("expected Age=40, got %q", got)
	}
}

func TestUpdate_ArithmeticSub(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age - 5 WHERE SingerId = 1`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := Execute(db, `SELECT Age FROM Singers WHERE SingerId = 1`)
	if got := result.Rows[0].Values[0].GetStringValue(); got != "25" {
		t.Errorf("expected Age=25, got %q", got)
	}
}

func TestUpdate_ArithmeticMul(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age * 2 WHERE SingerId = 1`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := Execute(db, `SELECT Age FROM Singers WHERE SingerId = 1`)
	if got := result.Rows[0].Values[0].GetStringValue(); got != "60" {
		t.Errorf("expected Age=60, got %q", got)
	}
}

func TestUpdate_ArithmeticDiv(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age / 3 WHERE SingerId = 3`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, _ := Execute(db, `SELECT Age FROM Singers WHERE SingerId = 3`)
	if got := result.Rows[0].Values[0].GetStringValue(); got != "9" {
		t.Errorf("expected Age=9, got %q", got)
	}
}

func TestUpdate_NoPKColumn(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET SingerId = 99 WHERE SingerId = 1`, zeroTS)
	if err == nil {
		t.Error("expected error when updating primary key column, got nil")
	}
	if !strings.Contains(err.Error(), "cannot update primary key column") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestUpdate_NullSet(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET LastName = NULL WHERE SingerId = 1`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the column is now NULL by querying with IS NULL.
	result, err := Execute(db, `SELECT SingerId FROM Singers WHERE LastName IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row with NULL LastName, got %d", len(result.Rows))
	}
}

func TestUpdate_WhereTrue_UpdatesAll(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `UPDATE Singers SET LastName = 'Updated' WHERE true`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 affected rows, got %d", n)
	}
}

// --- DELETE tests ---

func TestDelete_Basic(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `DELETE FROM Singers WHERE SingerId = 2`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 affected row, got %d", n)
	}

	result, err := Execute(db, `SELECT SingerId FROM Singers ORDER BY SingerId`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows after delete, got %d", len(result.Rows))
	}
}

func TestDelete_WhereTrue_DeletesAll(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `DELETE FROM Singers WHERE true`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 affected rows, got %d", n)
	}

	result, _ := Execute(db, `SELECT SingerId FROM Singers`)
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows after DELETE WHERE true, got %d", len(result.Rows))
	}
}

func TestDelete_NoWhere_ReturnsError(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `DELETE FROM Singers`, zeroTS)
	if err == nil {
		t.Error("expected error for DELETE without WHERE, got nil")
	}
	if !strings.Contains(err.Error(), "WHERE") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDelete_NoMatch(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `DELETE FROM Singers WHERE SingerId = 999`, zeroTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 affected rows, got %d", n)
	}
}

// --- PENDING_COMMIT_TIMESTAMP tests ---

func TestInsert_PendingCommitTimestamp(t *testing.T) {
	db := setupCommitTSTestDB(t)
	commitTS := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	_, err := ExecuteDML(db, `INSERT INTO Events (EventId, Name, CreatedAt) VALUES (1, 'Launch', PENDING_COMMIT_TIMESTAMP())`, commitTS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := Execute(db, `SELECT CreatedAt FROM Events WHERE EventId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	got := result.Rows[0].Values[0].GetStringValue()
	if got != commitTS.UTC().Format(time.RFC3339Nano) {
		t.Errorf("expected CreatedAt=%q, got %q", commitTS.UTC().Format(time.RFC3339Nano), got)
	}
}

func TestUpdate_PendingCommitTimestamp(t *testing.T) {
	db := setupCommitTSTestDB(t)
	insertTS := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	updateTS := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	_, err := ExecuteDML(db, `INSERT INTO Events (EventId, Name, CreatedAt) VALUES (1, 'Launch', PENDING_COMMIT_TIMESTAMP())`, insertTS)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	_, err = ExecuteDML(db, `UPDATE Events SET UpdatedAt = PENDING_COMMIT_TIMESTAMP() WHERE EventId = 1`, updateTS)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	result, err := Execute(db, `SELECT UpdatedAt FROM Events WHERE EventId = 1`)
	if err != nil {
		t.Fatal(err)
	}
	got := result.Rows[0].Values[0].GetStringValue()
	if got != updateTS.UTC().Format(time.RFC3339Nano) {
		t.Errorf("expected UpdatedAt=%q, got %q", updateTS.UTC().Format(time.RFC3339Nano), got)
	}
}

func TestInsert_PendingCommitTimestamp_ZeroTSError(t *testing.T) {
	db := setupCommitTSTestDB(t)

	// zeroTS means not inside a transaction; PENDING_COMMIT_TIMESTAMP() should fail.
	_, err := ExecuteDML(db, `INSERT INTO Events (EventId, Name, CreatedAt) VALUES (1, 'Test', PENDING_COMMIT_TIMESTAMP())`, zeroTS)
	if err == nil {
		t.Error("expected error when commitTS is zero, got nil")
	}
}

func TestUpdate_PendingCommitTimestamp_ZeroTSError(t *testing.T) {
	db := setupCommitTSTestDB(t)
	commitTS := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	_, err := ExecuteDML(db, `INSERT INTO Events (EventId, Name, CreatedAt) VALUES (1, 'Test', PENDING_COMMIT_TIMESTAMP())`, commitTS)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// UPDATE with zeroTS should fail because commitTS is zero.
	_, err = ExecuteDML(db, `UPDATE Events SET UpdatedAt = PENDING_COMMIT_TIMESTAMP() WHERE EventId = 1`, zeroTS)
	if err == nil {
		t.Error("expected error when commitTS is zero, got nil")
	}
}

func TestDDL_AlterColumn_AllowCommitTimestamp(t *testing.T) {
	db := store.NewDatabase()
	if err := db.ApplyDDL(`CREATE TABLE Items (
		ItemId INT64 NOT NULL,
		Name STRING(256),
		UpdatedAt TIMESTAMP,
	) PRIMARY KEY (ItemId)`); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Set allow_commit_timestamp = true.
	if err := db.ApplyDDL(`ALTER TABLE Items ALTER COLUMN UpdatedAt SET OPTIONS (allow_commit_timestamp = true)`); err != nil {
		t.Fatalf("ALTER COLUMN SET OPTIONS failed: %v", err)
	}
	table, _ := db.GetTable("Items")
	idx := table.ColIndex["UpdatedAt"]
	if !table.Cols[idx].AllowCommitTimestamp {
		t.Error("expected AllowCommitTimestamp=true after ALTER COLUMN SET OPTIONS")
	}

	// Clear with null.
	if err := db.ApplyDDL(`ALTER TABLE Items ALTER COLUMN UpdatedAt SET OPTIONS (allow_commit_timestamp = null)`); err != nil {
		t.Fatalf("ALTER COLUMN SET OPTIONS null failed: %v", err)
	}
	table, _ = db.GetTable("Items")
	if table.Cols[idx].AllowCommitTimestamp {
		t.Error("expected AllowCommitTimestamp=false after ALTER COLUMN SET OPTIONS null")
	}
}

func TestDDL_AlterColumn_AllowCommitTimestamp_OnNonTimestampColumn_Error(t *testing.T) {
	db := store.NewDatabase()
	if err := db.ApplyDDL(`CREATE TABLE Items (
		ItemId INT64 NOT NULL,
		Name STRING(256),
	) PRIMARY KEY (ItemId)`); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	err := db.ApplyDDL(`ALTER TABLE Items ALTER COLUMN Name SET OPTIONS (allow_commit_timestamp = true)`)
	if err == nil {
		t.Error("expected error for allow_commit_timestamp on non-TIMESTAMP column, got nil")
	}
	if !strings.Contains(err.Error(), "allow_commit_timestamp") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDDL_AllowCommitTimestamp_OnNonTimestampColumn_Error(t *testing.T) {
	db := store.NewDatabase()
	err := db.ApplyDDL(`CREATE TABLE Bad (
		Id INT64 NOT NULL,
		Name STRING(256) OPTIONS (allow_commit_timestamp = true),
	) PRIMARY KEY (Id)`)
	if err == nil {
		t.Error("expected error for allow_commit_timestamp on non-TIMESTAMP column, got nil")
	}
	if !strings.Contains(err.Error(), "allow_commit_timestamp") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --- Arithmetic operator tests ---

func TestArithmetic_DivisionByZero(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age / 0 WHERE SingerId = 1`, zeroTS)
	if err == nil {
		t.Error("expected error for division by zero, got nil")
	}
	if !strings.Contains(err.Error(), "division by zero") {
		t.Errorf("unexpected error message: %v", err)
	}
}
