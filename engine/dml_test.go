package engine

import (
	"strings"
	"testing"

	"github.com/uji/go-spanner-server/store"
)

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
		if _, err := ExecuteDML(db, s); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}
}

// --- UPDATE tests ---

func TestUpdate_Basic(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	n, err := ExecuteDML(db, `UPDATE Singers SET LastName = 'Johnson' WHERE SingerId = 1`)
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

	n, err := ExecuteDML(db, `UPDATE Singers SET FirstName = 'Bob', LastName = 'Williams' WHERE SingerId = 2`)
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

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age + 10 WHERE SingerId = 1`)
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

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age - 5 WHERE SingerId = 1`)
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

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age * 2 WHERE SingerId = 1`)
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

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age / 3 WHERE SingerId = 3`)
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

	_, err := ExecuteDML(db, `UPDATE Singers SET SingerId = 99 WHERE SingerId = 1`)
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

	_, err := ExecuteDML(db, `UPDATE Singers SET LastName = NULL WHERE SingerId = 1`)
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

	n, err := ExecuteDML(db, `UPDATE Singers SET LastName = 'Updated' WHERE true`)
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

	n, err := ExecuteDML(db, `DELETE FROM Singers WHERE SingerId = 2`)
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

	n, err := ExecuteDML(db, `DELETE FROM Singers WHERE true`)
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

	_, err := ExecuteDML(db, `DELETE FROM Singers`)
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

	n, err := ExecuteDML(db, `DELETE FROM Singers WHERE SingerId = 999`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 affected rows, got %d", n)
	}
}

// --- Arithmetic operator tests ---

func TestArithmetic_DivisionByZero(t *testing.T) {
	db := setupDMLTestDB(t)
	insertTestRows(t, db)

	_, err := ExecuteDML(db, `UPDATE Singers SET Age = Age / 0 WHERE SingerId = 1`)
	if err == nil {
		t.Error("expected error for division by zero, got nil")
	}
	if !strings.Contains(err.Error(), "division by zero") {
		t.Errorf("unexpected error message: %v", err)
	}
}
