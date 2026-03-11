package compattest

import (
	"context"
	"fmt"
	"testing"

	spannerserver "github.com/uji/go-spanner-server"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/option"
)

const (
	serverProject  = "test-project"
	serverInstance = "test-instance"
	serverDatabase = "test-db"
)

type serverBackend struct{}

func (b *serverBackend) Name() string { return "go-spanner-server" }

func (b *serverBackend) Setup(ctx context.Context, t *testing.T, ddl []string) (*spanner.Client, func()) {
	t.Helper()

	srv := spannerserver.New()
	conn, err := srv.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}

	adminClient, err := database.NewDatabaseAdminClient(ctx,
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}

	instancePath := fmt.Sprintf("projects/%s/instances/%s", serverProject, serverInstance)
	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", serverDatabase),
		ExtraStatements: ddl,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for database creation: %v", err)
	}

	dbPath := fmt.Sprintf("%s/databases/%s", instancePath, serverDatabase)
	client, err := spanner.NewClient(ctx, dbPath,
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}

	cleanup := func() {
		client.Close()
		adminClient.Close()
		conn.Close()
		srv.Stop()
	}
	return client, cleanup
}
