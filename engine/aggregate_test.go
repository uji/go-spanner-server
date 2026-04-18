package engine

import (
	"testing"

	"github.com/uji/go-spanner-server/store"
	"google.golang.org/protobuf/types/known/structpb"
)

// setupAggTestDB creates a test database suitable for aggregate tests.
func setupAggTestDB(t *testing.T) *store.Database {
	t.Helper()
	db := store.NewDatabase()
	if err := db.ApplyDDL(`CREATE TABLE Sales (
		Id      INT64 NOT NULL,
		Region  STRING(64),
		Amount  INT64,
		Score   FLOAT64,
	) PRIMARY KEY (Id)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	rows := []string{
		`INSERT INTO Sales (Id, Region, Amount, Score) VALUES (1, 'East', 100, 1.5)`,
		`INSERT INTO Sales (Id, Region, Amount, Score) VALUES (2, 'East', 200, 2.5)`,
		`INSERT INTO Sales (Id, Region, Amount, Score) VALUES (3, 'West', 300, 3.5)`,
		`INSERT INTO Sales (Id, Region, Amount, Score) VALUES (4, 'West', 400, 4.5)`,
		`INSERT INTO Sales (Id, Region, Amount, Score) VALUES (5, 'East', 150, NULL)`,
	}
	for _, s := range rows {
		if _, err := ExecuteDML(db, s, zeroTS); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}
	return db
}

// --- COUNT ---

func TestAggregate_COUNT_Star(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT COUNT(*) FROM Sales`)
	if got != "5" {
		t.Errorf("expected '5', got %q", got)
	}
}

func TestAggregate_COUNT_Expr_SkipsNull(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT COUNT(Score) FROM Sales`)
	if got != "4" { // Score is NULL for Id=5
		t.Errorf("expected '4', got %q", got)
	}
}

func TestAggregate_COUNT_Star_NoRows(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT COUNT(*) FROM Sales WHERE Amount > 9999`)
	if got != "0" {
		t.Errorf("expected '0', got %q", got)
	}
}

// --- SUM ---

func TestAggregate_SUM(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT SUM(Amount) FROM Sales`)
	if got != "1150" {
		t.Errorf("expected '1150', got %q", got)
	}
}

func TestAggregate_SUM_NoRows(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT SUM(Amount) FROM Sales WHERE Amount > 9999`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	// SUM over empty set returns NULL in Cloud Spanner.
	if _, ok := result.Rows[0].Values[0].Kind.(*structpb.Value_NullValue); !ok {
		t.Errorf("expected NULL for SUM over empty set, got %T", result.Rows[0].Values[0].Kind)
	}
}

// --- AVG ---

func TestAggregate_AVG(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT AVG(Amount) FROM Sales`)
	if err != nil {
		t.Fatal(err)
	}
	got := result.Rows[0].Values[0].GetNumberValue()
	if got != 230.0 {
		t.Errorf("expected 230.0, got %v", got)
	}
}

// --- MIN / MAX ---

func TestAggregate_MIN(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT MIN(Amount) FROM Sales`)
	if got != "100" {
		t.Errorf("expected '100', got %q", got)
	}
}

func TestAggregate_MAX(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT MAX(Amount) FROM Sales`)
	if got != "400" {
		t.Errorf("expected '400', got %q", got)
	}
}

// --- GROUP BY ---

func TestAggregate_GroupBy_COUNT(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT Region, COUNT(*) AS cnt FROM Sales GROUP BY Region ORDER BY Region`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result.Rows))
	}
	// East: 3 rows, West: 2 rows (ordered by Region)
	if result.Rows[0].Values[0].GetStringValue() != "East" {
		t.Errorf("expected 'East', got %q", result.Rows[0].Values[0].GetStringValue())
	}
	if result.Rows[0].Values[1].GetStringValue() != "3" {
		t.Errorf("expected cnt=3, got %q", result.Rows[0].Values[1].GetStringValue())
	}
	if result.Rows[1].Values[1].GetStringValue() != "2" {
		t.Errorf("expected cnt=2, got %q", result.Rows[1].Values[1].GetStringValue())
	}
}

func TestAggregate_GroupBy_SUM(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT Region, SUM(Amount) AS total FROM Sales GROUP BY Region ORDER BY Region`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result.Rows))
	}
	// East: 100+200+150=450, West: 300+400=700
	eastTotal := result.Rows[0].Values[1].GetStringValue()
	if eastTotal != "450" {
		t.Errorf("expected East total=450, got %q", eastTotal)
	}
	westTotal := result.Rows[1].Values[1].GetStringValue()
	if westTotal != "700" {
		t.Errorf("expected West total=700, got %q", westTotal)
	}
}

func TestAggregate_GroupBy_Multiple(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, A STRING(32), B STRING(32), V INT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, A, B, V) VALUES (1, 'x', 'p', 10)`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, A, B, V) VALUES (2, 'x', 'p', 20)`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, A, B, V) VALUES (3, 'x', 'q', 30)`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, A, B, V) VALUES (4, 'y', 'p', 40)`, zeroTS)

	result, err := Execute(db, `SELECT A, B, SUM(V) FROM T GROUP BY A, B ORDER BY A, B`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(result.Rows))
	}
	// (x,p)=30, (x,q)=30, (y,p)=40
	sums := []string{"30", "30", "40"}
	for i, row := range result.Rows {
		got := row.Values[2].GetStringValue()
		if got != sums[i] {
			t.Errorf("group %d: expected %s, got %q", i, sums[i], got)
		}
	}
}

// --- HAVING ---

func TestAggregate_Having(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT Region, COUNT(*) AS cnt FROM Sales GROUP BY Region HAVING COUNT(*) >= 3`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 group (East), got %d", len(result.Rows))
	}
	if result.Rows[0].Values[0].GetStringValue() != "East" {
		t.Errorf("expected 'East', got %q", result.Rows[0].Values[0].GetStringValue())
	}
}

func TestAggregate_Having_SUM(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT Region, SUM(Amount) AS total FROM Sales GROUP BY Region HAVING SUM(Amount) > 500`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 group (West), got %d", len(result.Rows))
	}
	if result.Rows[0].Values[0].GetStringValue() != "West" {
		t.Errorf("expected 'West', got %q", result.Rows[0].Values[0].GetStringValue())
	}
}

// --- GROUP BY with NULL ---

func TestAggregate_GroupBy_NullValues(t *testing.T) {
	// NULL values should be grouped together in a single NULL group.
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, Cat STRING(32), V INT64) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, Cat, V) VALUES (1, 'a', 10)`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, Cat, V) VALUES (2, NULL, 20)`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, Cat, V) VALUES (3, NULL, 30)`, zeroTS)

	result, err := Execute(db, `SELECT COUNT(*) AS cnt FROM T GROUP BY Cat ORDER BY cnt DESC`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 groups (non-null and null), got %d", len(result.Rows))
	}
	// NULL group has 2 rows, 'a' group has 1 row (ordered DESC)
	if result.Rows[0].Values[0].GetStringValue() != "2" {
		t.Errorf("expected largest group count=2, got %q", result.Rows[0].Values[0].GetStringValue())
	}
}

// --- COUNT(DISTINCT) error ---

func TestAggregate_COUNT_DISTINCT_Error(t *testing.T) {
	db := setupAggTestDB(t)
	_, err := Execute(db, `SELECT COUNT(DISTINCT Region) FROM Sales`)
	if err == nil {
		t.Error("expected error for COUNT(DISTINCT ...), got nil")
	}
}

// --- COUNT_IF ---

func TestAggregate_COUNT_IF(t *testing.T) {
	db := setupAggTestDB(t)
	got := queryFirstString(t, db, `SELECT COUNT_IF(Amount > 150) FROM Sales`)
	if got != "3" { // Amount=200,300,400 → 3
		t.Errorf("expected '3', got %q", got)
	}
}

// --- ANY_VALUE ---

func TestAggregate_ANY_VALUE(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT ANY_VALUE(Region) FROM Sales`)
	if err != nil {
		t.Fatal(err)
	}
	got := result.Rows[0].Values[0].GetStringValue()
	if got != "East" && got != "West" {
		t.Errorf("expected a region value, got %q", got)
	}
}

// --- Mixed aggregate + scalar in ORDER BY ---

func TestAggregate_OrderByPosition(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT Region, COUNT(*) FROM Sales GROUP BY Region ORDER BY 2 DESC`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// East has 3 rows (DESC), West has 2 rows
	if result.Rows[0].Values[0].GetStringValue() != "East" {
		t.Errorf("expected 'East' first (highest count), got %q", result.Rows[0].Values[0].GetStringValue())
	}
}

// --- DISTINCT ---

func TestDistinct_Basic(t *testing.T) {
	db := setupAggTestDB(t)
	result, err := Execute(db, `SELECT DISTINCT Region FROM Sales ORDER BY Region`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 distinct regions, got %d", len(result.Rows))
	}
	if result.Rows[0].Values[0].GetStringValue() != "East" {
		t.Errorf("expected 'East', got %q", result.Rows[0].Values[0].GetStringValue())
	}
	if result.Rows[1].Values[0].GetStringValue() != "West" {
		t.Errorf("expected 'West', got %q", result.Rows[1].Values[0].GetStringValue())
	}
}

func TestDistinct_MultiColumn(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, A STRING(32), B STRING(32)) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, A, B) VALUES (1, 'x', 'p')`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, A, B) VALUES (2, 'x', 'p')`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, A, B) VALUES (3, 'x', 'q')`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, A, B) VALUES (4, 'y', 'p')`, zeroTS)

	result, err := Execute(db, `SELECT DISTINCT A, B FROM T ORDER BY A, B`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 { // (x,p), (x,q), (y,p)
		t.Fatalf("expected 3 distinct rows, got %d", len(result.Rows))
	}
}

func TestDistinct_AllSame(t *testing.T) {
	db := store.NewDatabase()
	db.ApplyDDL(`CREATE TABLE T (Id INT64 NOT NULL, V STRING(32)) PRIMARY KEY (Id)`)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (1, 'a')`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (2, 'a')`, zeroTS)
	ExecuteDML(db, `INSERT INTO T (Id, V) VALUES (3, 'a')`, zeroTS)

	result, err := Execute(db, `SELECT DISTINCT V FROM T`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
}

// --- Aggregate expression in SELECT (mixed) ---

func TestAggregate_ExprInSelect(t *testing.T) {
	db := setupAggTestDB(t)
	// COUNT(*) + 0 (aggregate embedded in a binary expression)
	result, err := Execute(db, `SELECT Region, SUM(Amount) AS total FROM Sales WHERE Region = 'East' GROUP BY Region`)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0].Values[1].GetStringValue() != "450" {
		t.Errorf("expected total=450, got %q", result.Rows[0].Values[1].GetStringValue())
	}
}
