package compattest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	emulatorProject  = "test-project"
	emulatorInstance = "test-instance"
)

var (
	emulatorOnce     sync.Once
	emulatorSetupErr error
	emulatorDBSeq    atomic.Int64
)

type emulatorBackend struct{}

func (b *emulatorBackend) Name() string { return "emulator" }

func (b *emulatorBackend) Setup(ctx context.Context, t *testing.T, ddl []string) (*spanner.Client, func()) {
	t.Helper()

	emulatorOnce.Do(func() {
		emulatorSetupErr = createEmulatorInstance(ctx)
	})
	if emulatorSetupErr != nil {
		t.Fatalf("failed to setup emulator instance: %v", emulatorSetupErr)
	}

	dbName := fmt.Sprintf("testdb%d", emulatorDBSeq.Add(1))
	instancePath := fmt.Sprintf("projects/%s/instances/%s", emulatorProject, emulatorInstance)

	adminClient, err := database.NewDatabaseAdminClient(ctx,
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}

	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
		ExtraStatements: ddl,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for database creation: %v", err)
	}

	dbPath := fmt.Sprintf("%s/databases/%s", instancePath, dbName)
	client, err := spanner.NewClient(ctx, dbPath,
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}

	cleanup := func() {
		client.Close()
		_ = adminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
			Database: dbPath,
		})
		adminClient.Close()
	}
	return client, cleanup
}

func createEmulatorInstance(ctx context.Context) error {
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx,
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return fmt.Errorf("create instance admin client: %w", err)
	}
	defer instanceAdmin.Close()

	op, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", emulatorProject),
		InstanceId: emulatorInstance,
		Instance: &instancepb.Instance{
			DisplayName: emulatorInstance,
		},
	})
	if err != nil {
		return fmt.Errorf("create instance: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("wait for instance creation: %w", err)
	}
	return nil
}
