package compattest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	spannerserver "github.com/uji/go-spanner-server"
	"github.com/uji/go-spanner-server/compattest/testutil"
	"google.golang.org/api/option"
)

var singerAlbumDDL = []string{
	`CREATE TABLE Singers (
		SingerId INT64 NOT NULL,
		Name STRING(MAX),
	) PRIMARY KEY (SingerId)`,
	`CREATE TABLE Albums (
		SingerId INT64 NOT NULL,
		AlbumId INT64 NOT NULL,
		Title STRING(MAX),
	) PRIMARY KEY (SingerId, AlbumId),
	INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
}

var singerAlbumNoActionDDL = []string{
	`CREATE TABLE Singers (
		SingerId INT64 NOT NULL,
		Name STRING(MAX),
	) PRIMARY KEY (SingerId)`,
	`CREATE TABLE Albums (
		SingerId INT64 NOT NULL,
		AlbumId INT64 NOT NULL,
		Title STRING(MAX),
	) PRIMARY KEY (SingerId, AlbumId),
	INTERLEAVE IN PARENT Singers ON DELETE NO ACTION`,
}

var threeLevelDDL = []string{
	`CREATE TABLE Singers (
		SingerId INT64 NOT NULL,
		Name STRING(MAX),
	) PRIMARY KEY (SingerId)`,
	`CREATE TABLE Albums (
		SingerId INT64 NOT NULL,
		AlbumId INT64 NOT NULL,
		Title STRING(MAX),
	) PRIMARY KEY (SingerId, AlbumId),
	INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
	`CREATE TABLE Songs (
		SingerId INT64 NOT NULL,
		AlbumId INT64 NOT NULL,
		SongId INT64 NOT NULL,
		SongTitle STRING(MAX),
	) PRIMARY KEY (SingerId, AlbumId, SongId),
	INTERLEAVE IN PARENT Albums ON DELETE CASCADE`,
}

// interleaveTestCase defines a single interleave test case declaratively.
type interleaveTestCase struct {
	ddl          []string
	ops          [][]*spanner.Mutation
	wantApplyErr bool // if true, the last ops entry is expected to fail
	readTable    string
	readCols     []string
	readKeys     spanner.KeySet
	wantRows     []string // expected rows in testutil.FormatRow format; used only when wantApplyErr is false
}

// interleaveTests lists all table-driven interleave test cases.
var interleaveTests = map[string]interleaveTestCase{
	"InsertChildWithParent": {
		ddl: singerAlbumDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
			},
			{
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(1), "Album One"}),
			},
		},
		readTable: "Albums",
		readCols:  []string{"SingerId", "AlbumId", "Title"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{`(1, 1, "Album One")`},
	},
	"InsertOrphanFails": {
		ddl: singerAlbumDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(99), int64(1), "Orphan Album"}),
			},
		},
		wantApplyErr: true,
	},
	"CascadeDelete": {
		ddl: singerAlbumDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(1), "Album One"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(2), "Album Two"}),
			},
			{
				spanner.Delete("Singers", spanner.Key{int64(1)}),
			},
		},
		readTable: "Albums",
		readCols:  []string{"SingerId", "AlbumId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
	"NoActionDeleteBlocked": {
		ddl: singerAlbumNoActionDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(1), "Album One"}),
			},
			{
				spanner.Delete("Singers", spanner.Key{int64(1)}),
			},
		},
		wantApplyErr: true,
	},
	"NoActionDeleteAllowed": {
		ddl: singerAlbumNoActionDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
			},
			{
				spanner.Delete("Singers", spanner.Key{int64(1)}),
			},
		},
		readTable: "Singers",
		readCols:  []string{"SingerId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
	"CascadeDeleteByRange": {
		ddl: singerAlbumDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(2), "Bob"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(1), "Album 1-1"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(2), int64(1), "Album 2-1"}),
			},
			{
				spanner.Delete("Singers", spanner.KeyRange{
					Start: spanner.Key{int64(1)},
					End:   spanner.Key{int64(1)},
					Kind:  spanner.ClosedClosed,
				}),
			},
		},
		readTable: "Albums",
		readCols:  []string{"SingerId", "AlbumId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{"(2, 1)"},
	},
	"CascadeDeleteAll": {
		ddl: singerAlbumDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
				spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(2), "Bob"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(1), "Album 1-1"}),
				spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(2), int64(1), "Album 2-1"}),
			},
			{
				spanner.Delete("Singers", spanner.AllKeys()),
			},
		},
		readTable: "Albums",
		readCols:  []string{"SingerId"},
		readKeys:  spanner.AllKeys(),
		wantRows:  []string{},
	},
	"ReplaceOrphanFails": {
		ddl: singerAlbumDDL,
		ops: [][]*spanner.Mutation{
			{
				spanner.InsertOrUpdate("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(99), int64(1), "Orphan"}),
			},
		},
		wantApplyErr: true,
	},
}

// runInterleaveTest is the generic runner for interleave test cases.
func runInterleaveTest(ctx context.Context, t *testing.T, client *spanner.Client, tc interleaveTestCase) {
	t.Helper()

	// Apply all ops except the last (if we expect the last to fail).
	applyUntil := len(tc.ops)
	if tc.wantApplyErr {
		applyUntil = len(tc.ops) - 1
	}
	for i := range applyUntil {
		if _, err := client.Apply(ctx, tc.ops[i]); err != nil {
			t.Fatalf("ops[%d]: failed to apply mutations: %v", i, err)
		}
	}

	if tc.wantApplyErr {
		last := len(tc.ops) - 1
		_, err := client.Apply(ctx, tc.ops[last])
		if err == nil {
			t.Fatalf("ops[%d]: expected error, got nil", last)
		}
		return
	}

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

func TestCompat_Interleaves(t *testing.T) {
	for name, tc := range interleaveTests {
		t.Run(name, func(t *testing.T) {
			testutil.RunCompat(t, tc.ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
				runInterleaveTest(ctx, t, client, tc)
			})
		})
	}
}

// TestCompat_InterleaveCascadeDeleteRecursive verifies 3-level cascade deletion.
func TestCompat_InterleaveCascadeDeleteRecursive(t *testing.T) {
	testutil.RunCompat(t, threeLevelDDL, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		_, err := client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("Singers", []string{"SingerId", "Name"}, []any{int64(1), "Alice"}),
			spanner.Insert("Albums", []string{"SingerId", "AlbumId", "Title"}, []any{int64(1), int64(1), "Album One"}),
			spanner.Insert("Songs", []string{"SingerId", "AlbumId", "SongId", "SongTitle"}, []any{int64(1), int64(1), int64(1), "Song One"}),
			spanner.Insert("Songs", []string{"SingerId", "AlbumId", "SongId", "SongTitle"}, []any{int64(1), int64(1), int64(2), "Song Two"}),
		})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}

		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Delete("Singers", spanner.Key{int64(1)}),
		})
		if err != nil {
			t.Fatalf("delete singer: %v", err)
		}

		for _, tbl := range []string{"Albums", "Songs"} {
			iter := client.Single().Read(ctx, tbl, spanner.AllKeys(), []string{"SingerId"})
			defer iter.Stop()
			var count int
			if err := iter.Do(func(r *spanner.Row) error { count++; return nil }); err != nil {
				t.Fatalf("read %s: %v", tbl, err)
			}
			if count != 0 {
				t.Errorf("expected 0 rows in %s after recursive cascade delete, got %d", tbl, count)
			}
		}
	})
}

// TestCompat_InterleavePKValidation verifies that invalid child PK (not starting with parent PK) is rejected at DDL time.
func TestCompat_InterleavePKValidation(t *testing.T) {
	invalidDDL := []string{
		`CREATE TABLE Parents (
			ParentId INT64 NOT NULL,
		) PRIMARY KEY (ParentId)`,
		`CREATE TABLE Children (
			ChildId INT64 NOT NULL,
			ParentId INT64 NOT NULL,
		) PRIMARY KEY (ChildId),
		INTERLEAVE IN PARENT Parents ON DELETE CASCADE`,
	}
	for _, b := range testutil.Backends() {
		t.Run(b.Name(), func(t *testing.T) {
			ctx := context.Background()
			srv := spannerserver.New()
			conn, err := srv.Conn(ctx)
			if err != nil {
				t.Fatalf("failed to get connection: %v", err)
			}
			defer srv.Stop()
			defer conn.Close()

			adminClient, err := database.NewDatabaseAdminClient(ctx,
				option.WithGRPCConn(conn),
				option.WithoutAuthentication(),
			)
			if err != nil {
				t.Fatalf("failed to create admin client: %v", err)
			}
			defer adminClient.Close()

			instancePath := fmt.Sprintf("projects/%s/instances/%s", testutil.ServerProject, testutil.ServerInstance)
			op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
				Parent:          instancePath,
				CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testutil.ServerDatabase),
				ExtraStatements: invalidDDL,
			})
			if err != nil {
				// Expected: DDL rejected immediately
				return
			}
			_, err = op.Wait(ctx)
			if err == nil {
				t.Fatal("expected DDL error for invalid interleave PK, got nil")
			}
		})
	}
}

// TestCompat_InterleaveDropParentBlocked verifies dropping a parent table fails when child tables exist.
func TestCompat_InterleaveDropParentBlocked(t *testing.T) {
	testutil.RunCompatWithAdmin(t, singerAlbumDDL, func(ctx context.Context, t *testing.T, client *spanner.Client, adminClient *database.DatabaseAdminClient, dbPath string) {
		op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   dbPath,
			Statements: []string{"DROP TABLE Singers"},
		})
		if err != nil {
			// Some backends reject immediately
			return
		}
		if err := op.Wait(ctx); err == nil {
			t.Fatal("expected error dropping parent table with child interleaved table, got nil")
		}
	})
}

// TestCompat_InterleaveDropChildThenParent verifies dropping child then parent succeeds.
func TestCompat_InterleaveDropChildThenParent(t *testing.T) {
	testutil.RunCompatWithAdmin(t, singerAlbumDDL, func(ctx context.Context, t *testing.T, client *spanner.Client, adminClient *database.DatabaseAdminClient, dbPath string) {
		op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   dbPath,
			Statements: []string{"DROP TABLE Albums"},
		})
		if err != nil {
			t.Fatalf("drop albums DDL request failed: %v", err)
		}
		if err := op.Wait(ctx); err != nil {
			t.Fatalf("drop albums failed: %v", err)
		}

		op, err = adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   dbPath,
			Statements: []string{"DROP TABLE Singers"},
		})
		if err != nil {
			t.Fatalf("drop singers DDL request failed: %v", err)
		}
		if err := op.Wait(ctx); err != nil {
			t.Fatalf("drop singers failed: %v", err)
		}

		iter := client.Single().Read(ctx, "Singers", spanner.AllKeys(), []string{"SingerId"})
		defer iter.Stop()
		_, err = iter.Next()
		if err == nil || status.Code(err) == codes.OK {
			// iterator.Done means no rows but table exists; any other error means table gone
		}
		_ = iterator.Done // reference to avoid unused import
	})
}
