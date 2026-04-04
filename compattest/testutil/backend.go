package testutil

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	spannerserver "github.com/uji/go-spanner-server"

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
	ServerProject  = "test-project"
	ServerInstance = "test-instance"
	ServerDatabase = "test-db"

	EmulatorProject  = "test-project"
	EmulatorInstance = "test-instance"
)

// TestBackend represents a Spanner-compatible backend for testing.
type TestBackend interface {
	Name() string
	Setup(ctx context.Context, t *testing.T, ddl []string) (client *spanner.Client, cleanup func())
}

// AdminBackend extends TestBackend with admin client access.
type AdminBackend interface {
	Name() string
	SetupWithAdmin(ctx context.Context, t *testing.T, ddl []string) (client *spanner.Client, adminClient *database.DatabaseAdminClient, dbPath string, cleanup func())
}

// Backends returns the list of available test backends.
func Backends() []TestBackend {
	bs := []TestBackend{
		&ServerBackend{},
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		bs = append(bs, &EmulatorBackend{})
	}
	return bs
}

// AdminBackends returns the list of available admin backends.
func AdminBackends() []AdminBackend {
	return []AdminBackend{&ServerBackend{}}
}

// RunCompat runs a test function against all available backends.
func RunCompat(t *testing.T, ddl []string, fn func(context.Context, *testing.T, *spanner.Client)) {
	t.Helper()
	for _, b := range Backends() {
		t.Run(b.Name(), func(t *testing.T) {
			ctx := context.Background()
			client, cleanup := b.Setup(ctx, t, ddl)
			defer cleanup()
			fn(ctx, t, client)
		})
	}
}

// RunCompatWithAdmin runs a test function against all available admin backends.
func RunCompatWithAdmin(t *testing.T, ddl []string, fn func(context.Context, *testing.T, *spanner.Client, *database.DatabaseAdminClient, string)) {
	t.Helper()
	for _, b := range AdminBackends() {
		t.Run(b.Name(), func(t *testing.T) {
			ctx := context.Background()
			client, adminClient, dbPath, cleanup := b.SetupWithAdmin(ctx, t, ddl)
			defer cleanup()
			fn(ctx, t, client, adminClient, dbPath)
		})
	}
}

// ServerBackend is a backend that starts an in-process go-spanner-server.
// It implements both TestBackend and AdminBackend.
type ServerBackend struct{}

func (b *ServerBackend) Name() string { return "go-spanner-server" }

func (b *ServerBackend) SetupWithAdmin(ctx context.Context, t *testing.T, ddl []string) (*spanner.Client, *database.DatabaseAdminClient, string, func()) {
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

	instancePath := fmt.Sprintf("projects/%s/instances/%s", ServerProject, ServerInstance)
	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", ServerDatabase),
		ExtraStatements: ddl,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to wait for database creation: %v", err)
	}

	dbPath := fmt.Sprintf("%s/databases/%s", instancePath, ServerDatabase)
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
	return client, adminClient, dbPath, cleanup
}

func (b *ServerBackend) Setup(ctx context.Context, t *testing.T, ddl []string) (*spanner.Client, func()) {
	t.Helper()
	client, _, _, cleanup := b.SetupWithAdmin(ctx, t, ddl)
	return client, cleanup
}

var (
	emulatorOnce     sync.Once
	emulatorSetupErr error
	emulatorDBSeq    atomic.Int64
)

// EmulatorBackend is a backend that connects to a Cloud Spanner emulator.
// Requires SPANNER_EMULATOR_HOST to be set.
type EmulatorBackend struct{}

func (b *EmulatorBackend) Name() string { return "emulator" }

func (b *EmulatorBackend) Setup(ctx context.Context, t *testing.T, ddl []string) (*spanner.Client, func()) {
	t.Helper()

	emulatorOnce.Do(func() {
		emulatorSetupErr = createEmulatorInstance(ctx)
	})
	if emulatorSetupErr != nil {
		t.Fatalf("failed to setup emulator instance: %v", emulatorSetupErr)
	}

	dbName := fmt.Sprintf("testdb%d", emulatorDBSeq.Add(1))
	instancePath := fmt.Sprintf("projects/%s/instances/%s", EmulatorProject, EmulatorInstance)

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
		Parent:     fmt.Sprintf("projects/%s", EmulatorProject),
		InstanceId: EmulatorInstance,
		Instance: &instancepb.Instance{
			DisplayName: EmulatorInstance,
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
