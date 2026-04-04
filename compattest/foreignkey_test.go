package compattest

import (
	"context"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/uji/go-spanner-server/compattest/testutil"
)

var fkCascadeDDL = []string{
	`CREATE TABLE Customers (
		CustomerId INT64 NOT NULL,
		Name STRING(MAX),
	) PRIMARY KEY (CustomerId)`,
	`CREATE TABLE Orders (
		OrderId INT64 NOT NULL,
		CustomerId INT64,
		OrderName STRING(MAX),
		CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId)
			REFERENCES Customers (CustomerId) ON DELETE CASCADE,
	) PRIMARY KEY (OrderId)`,
}

var fkNoActionDDL = []string{
	`CREATE TABLE Customers (
		CustomerId INT64 NOT NULL,
		Name STRING(MAX),
	) PRIMARY KEY (CustomerId)`,
	`CREATE TABLE Orders (
		OrderId INT64 NOT NULL,
		CustomerId INT64,
		OrderName STRING(MAX),
		CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId)
			REFERENCES Customers (CustomerId),
	) PRIMARY KEY (OrderId)`,
}

// foreignKeyTestCase defines a single foreign key test case declaratively.
type foreignKeyTestCase struct {
	name      string
	ddl       []string
	ops       [][]*spanner.Mutation // applied in order; only the last op is checked for wantErr
	wantErr   bool                  // whether the last op is expected to fail
	readTable string                // used only when wantErr=false
	readCols  []string
	readKeys  spanner.KeySet
	wantRows  []string // expected rows in testutil.FormatRow format
}

var foreignKeyTests = []foreignKeyTestCase{
	{
		name: "InsertWithParent",
		ddl:  fkCascadeDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Customers", []string{"CustomerId", "Name"}, []any{int64(1), "Alice"}),
			},
			{
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(1), "Order One"}),
			},
		},
		wantErr:   false,
		readTable: "Orders",
		readCols:  []string{"OrderId", "CustomerId", "OrderName"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{`(10, 1, "Order One")`},
	},
	{
		name: "InsertOrphanFails",
		ddl:  fkCascadeDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(99), "Orphan Order"}),
			},
		},
		wantErr: true,
	},
	{
		name: "DeleteNoActionBlocked",
		ddl:  fkNoActionDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Customers", []string{"CustomerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(1), "Order One"}),
			},
			{
				spanner.Delete("Customers", spanner.Key{int64(1)}),
			},
		},
		wantErr: true,
	},
	{
		name: "DeleteNoActionAllowed",
		ddl:  fkNoActionDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Customers", []string{"CustomerId", "Name"}, []any{int64(1), "Alice"}),
			},
			{
				spanner.Delete("Customers", spanner.Key{int64(1)}),
			},
		},
		wantErr:   false,
		readTable: "Customers",
		readCols:  []string{"CustomerId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
	{
		name: "DeleteCascade",
		ddl:  fkCascadeDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Customers", []string{"CustomerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(1), "Order One"}),
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(11), int64(1), "Order Two"}),
			},
			{
				spanner.Delete("Customers", spanner.Key{int64(1)}),
			},
		},
		wantErr:   false,
		readTable: "Orders",
		readCols:  []string{"OrderId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
	{
		name: "NullBypass",
		ddl:  fkCascadeDDL,
		ops: [][]*spanner.Mutation{
			{
				// CustomerId is NULL - FK constraint should be bypassed
				spanner.Insert("Orders", []string{"OrderId", "OrderName"}, []any{int64(10), "No Customer"}),
			},
		},
		wantErr:   false,
		readTable: "Orders",
		readCols:  []string{"OrderId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{"(10)"},
	},
	{
		name: "CascadeDeleteAll",
		ddl:  fkCascadeDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Customers", []string{"CustomerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Customers", []string{"CustomerId", "Name"}, []any{int64(2), "Bob"}),
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(1), "Order One"}),
				spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(11), int64(2), "Order Two"}),
			},
			{
				spanner.Delete("Customers", spanner.AllKeys()),
			},
		},
		wantErr:   false,
		readTable: "Orders",
		readCols:  []string{"OrderId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
}

func runForeignKeyTest(ctx context.Context, t *testing.T, client *spanner.Client, tc foreignKeyTestCase) {
	t.Helper()

	// Apply all ops except the last, expecting success
	for i, mutations := range tc.ops[:len(tc.ops)-1] {
		if _, err := client.Apply(ctx, mutations); err != nil {
			t.Fatalf("ops[%d]: failed to apply mutations: %v", i, err)
		}
	}

	// Apply the last op and check error expectation
	lastIdx := len(tc.ops) - 1
	_, lastErr := client.Apply(ctx, tc.ops[lastIdx])
	if tc.wantErr {
		if lastErr == nil {
			t.Fatalf("ops[%d]: expected error but got nil", lastIdx)
		}
		return
	}
	if lastErr != nil {
		t.Fatalf("ops[%d]: unexpected error: %v", lastIdx, lastErr)
	}

	// Read and verify
	iter := client.Single().Read(ctx, tc.readTable, tc.readKeys, tc.readCols)
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

func TestCompat_ForeignKey(t *testing.T) {
	for _, tc := range foreignKeyTests {
		t.Run(tc.name, func(t *testing.T) {
			testutil.RunCompat(t, tc.ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
				runForeignKeyTest(ctx, t, client, tc)
			})
		})
	}
}

// TestCompat_ForeignKeyAlterTableAddConstraint verifies ALTER TABLE ADD CONSTRAINT adds FK enforcement.
func TestCompat_ForeignKeyAlterTableAddConstraint(t *testing.T) {
	baseDDL := []string{
		`CREATE TABLE Customers (
			CustomerId INT64 NOT NULL,
			Name STRING(MAX),
		) PRIMARY KEY (CustomerId)`,
		`CREATE TABLE Orders (
			OrderId INT64 NOT NULL,
			CustomerId INT64,
			OrderName STRING(MAX),
		) PRIMARY KEY (OrderId)`,
	}
	testutil.RunCompatWithAdmin(t, baseDDL, func(ctx context.Context, t *testing.T, client *spanner.Client, adminClient *database.DatabaseAdminClient, dbPath string) {
		// Add FK constraint via ALTER TABLE
		op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: dbPath,
			Statements: []string{
				`ALTER TABLE Orders ADD CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (CustomerId)`,
			},
		})
		if err != nil {
			t.Fatalf("UpdateDatabaseDdl request failed: %v", err)
		}
		if err := op.Wait(ctx); err != nil {
			t.Fatalf("ADD CONSTRAINT failed: %v", err)
		}

		// Inserting an orphan should now fail
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(99), "Orphan"}),
		})
		if err == nil {
			t.Fatal("expected error inserting orphan after ADD CONSTRAINT, got nil")
		}
	})
}

// TestCompat_ForeignKeyAlterTableDropConstraint verifies ALTER TABLE DROP CONSTRAINT removes FK enforcement.
func TestCompat_ForeignKeyAlterTableDropConstraint(t *testing.T) {
	testutil.RunCompatWithAdmin(t, fkNoActionDDL, func(ctx context.Context, t *testing.T, client *spanner.Client, adminClient *database.DatabaseAdminClient, dbPath string) {
		// Drop FK constraint
		op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   dbPath,
			Statements: []string{`ALTER TABLE Orders DROP CONSTRAINT FK_Orders_Customers`},
		})
		if err != nil {
			t.Fatalf("UpdateDatabaseDdl request failed: %v", err)
		}
		if err := op.Wait(ctx); err != nil {
			t.Fatalf("DROP CONSTRAINT failed: %v", err)
		}

		// Inserting an orphan should now succeed (constraint removed)
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Orders", []string{"OrderId", "CustomerId", "OrderName"}, []any{int64(10), int64(99), "No longer constrained"}),
		})
		if err != nil {
			t.Fatalf("expected success inserting after DROP CONSTRAINT, got: %v", err)
		}
	})
}

// TestCompat_ForeignKeyDropParentBlocked verifies dropping a referenced table fails.
func TestCompat_ForeignKeyDropParentBlocked(t *testing.T) {
	testutil.RunCompatWithAdmin(t, fkNoActionDDL, func(ctx context.Context, t *testing.T, client *spanner.Client, adminClient *database.DatabaseAdminClient, dbPath string) {
		op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   dbPath,
			Statements: []string{`DROP TABLE Customers`},
		})
		if err != nil {
			return // rejected immediately
		}
		if err := op.Wait(ctx); err == nil {
			t.Fatal("expected error dropping table referenced by foreign key, got nil")
		}
	})
}
