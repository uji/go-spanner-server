package spannerserver_test

import (
	"context"
	"fmt"
	"testing"

	spannerserver "github.com/uji/go-spanner-server"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	testProject  = "test-project"
	testInstance = "test-instance"
	testDatabase = "test-db"
)

func testDBPath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProject, testInstance, testDatabase)
}

func testInstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance)
}

func setupServer(t *testing.T) (*spannerserver.Server, *grpc.ClientConn) {
	t.Helper()
	ctx := context.Background()
	srv := spannerserver.New()
	t.Cleanup(srv.Stop)

	conn, err := srv.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return srv, conn
}

func createTestDatabase(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	ctx := context.Background()

	adminClient, err := database.NewDatabaseAdminClient(ctx,
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	// Don't close adminClient as it would close the shared grpc.ClientConn

	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          testInstancePath(),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabase),
		ExtraStatements: []string{
			`CREATE TABLE Singers (
				SingerId INT64 NOT NULL,
				FirstName STRING(1024),
				LastName STRING(1024),
			) PRIMARY KEY (SingerId)`,
		},
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for database creation: %v", err)
	}
}

func TestInsertAndRead(t *testing.T) {
	ctx := context.Background()
	_, conn := setupServer(t)

	// 1. Create database with table
	createTestDatabase(t, conn)

	// 2. Create Spanner client
	client, err := spanner.NewClient(ctx, testDBPath(),
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}
	defer client.Close()

	// 3. Insert rows using Mutations
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(1), "Marc", "Richards"},
		),
		spanner.InsertOrUpdate("Singers",
			[]string{"SingerId", "FirstName", "LastName"},
			[]interface{}{int64(2), "Catalina", "Smith"},
		),
	})
	if err != nil {
		t.Fatalf("failed to apply mutations: %v", err)
	}

	// 4. Read rows back
	iter := client.Single().Read(ctx, "Singers",
		spanner.AllKeys(),
		[]string{"SingerId", "FirstName", "LastName"},
	)
	defer iter.Stop()

	type Singer struct {
		SingerId  int64
		FirstName string
		LastName  string
	}

	var singers []Singer
	err = iter.Do(func(row *spanner.Row) error {
		var s Singer
		if err := row.Columns(&s.SingerId, &s.FirstName, &s.LastName); err != nil {
			return err
		}
		singers = append(singers, s)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to read rows: %v", err)
	}

	// 5. Verify
	if len(singers) != 2 {
		t.Fatalf("expected 2 singers, got %d", len(singers))
	}

	if singers[0].SingerId != 1 || singers[0].FirstName != "Marc" || singers[0].LastName != "Richards" {
		t.Errorf("unexpected singer[0]: %+v", singers[0])
	}
	if singers[1].SingerId != 2 || singers[1].FirstName != "Catalina" || singers[1].LastName != "Smith" {
		t.Errorf("unexpected singer[1]: %+v", singers[1])
	}

	t.Logf("Successfully inserted and read %d singers", len(singers))
}
