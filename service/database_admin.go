package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/uji/go-spanner-server/store"

	lropb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// DatabaseAdminServer implements databasepb.DatabaseAdminServer.
type DatabaseAdminServer struct {
	databasepb.UnimplementedDatabaseAdminServer

	mu         sync.Mutex
	databases  map[string]*store.Database
	operations map[string]*lropb.Operation
}

// NewDatabaseAdminServer creates a new DatabaseAdminServer.
func NewDatabaseAdminServer() *DatabaseAdminServer {
	return &DatabaseAdminServer{
		databases:  make(map[string]*store.Database),
		operations: make(map[string]*lropb.Operation),
	}
}

// GetDatabase returns the store.Database for use by other services.
func (s *DatabaseAdminServer) GetDB(name string) (*store.Database, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	db, ok := s.databases[name]
	return db, ok
}

func (s *DatabaseAdminServer) CreateDatabase(ctx context.Context, req *databasepb.CreateDatabaseRequest) (*lropb.Operation, error) {
	// Parse database name from create statement
	// Format: "CREATE DATABASE `dbname`"
	parts := strings.Fields(req.CreateStatement)
	if len(parts) < 3 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid create statement")
	}
	dbName := strings.Trim(parts[2], "`")
	fullName := req.Parent + "/databases/" + dbName

	s.mu.Lock()
	if _, exists := s.databases[fullName]; exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "database %q already exists", fullName)
	}
	db := store.NewDatabase()
	s.databases[fullName] = db
	s.mu.Unlock()

	// Apply extra DDL statements
	for _, ddl := range req.ExtraStatements {
		if err := db.ApplyDDL(ddl); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "apply DDL: %v", err)
		}
	}

	// Create a done operation
	meta := &databasepb.CreateDatabaseMetadata{Database: fullName}
	metaAny, _ := anypb.New(meta)

	resp := &databasepb.Database{
		Name:  fullName,
		State: databasepb.Database_READY,
	}
	respAny, _ := anypb.New(resp)

	opName := fmt.Sprintf("%s/operations/_auto_%s", fullName, dbName)
	op := &lropb.Operation{
		Name:     opName,
		Done:     true,
		Metadata: metaAny,
		Result:   &lropb.Operation_Response{Response: respAny},
	}

	s.mu.Lock()
	s.operations[opName] = op
	s.mu.Unlock()

	return op, nil
}

func (s *DatabaseAdminServer) UpdateDatabaseDdl(ctx context.Context, req *databasepb.UpdateDatabaseDdlRequest) (*lropb.Operation, error) {
	s.mu.Lock()
	db, ok := s.databases[req.Database]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "database %q not found", req.Database)
	}

	for _, stmt := range req.Statements {
		if err := db.ApplyDDL(stmt); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "apply DDL: %v", err)
		}
	}

	meta := &databasepb.UpdateDatabaseDdlMetadata{Database: req.Database}
	metaAny, _ := anypb.New(meta)

	opName := fmt.Sprintf("%s/operations/ddl_%d", req.Database, len(db.GetDDLs()))
	op := &lropb.Operation{
		Name:     opName,
		Done:     true,
		Metadata: metaAny,
		Result:   &lropb.Operation_Response{Response: func() *anypb.Any { a, _ := anypb.New(&emptypb.Empty{}); return a }()},
	}

	s.mu.Lock()
	s.operations[opName] = op
	s.mu.Unlock()

	return op, nil
}

func (s *DatabaseAdminServer) GetDatabase(ctx context.Context, req *databasepb.GetDatabaseRequest) (*databasepb.Database, error) {
	s.mu.Lock()
	_, ok := s.databases[req.Name]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "database %q not found", req.Name)
	}

	return &databasepb.Database{
		Name:  req.Name,
		State: databasepb.Database_READY,
	}, nil
}

func (s *DatabaseAdminServer) GetDatabaseDdl(ctx context.Context, req *databasepb.GetDatabaseDdlRequest) (*databasepb.GetDatabaseDdlResponse, error) {
	s.mu.Lock()
	db, ok := s.databases[req.Database]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "database %q not found", req.Database)
	}

	return &databasepb.GetDatabaseDdlResponse{
		Statements: db.GetDDLs(),
	}, nil
}

// OperationsServer implements longrunningpb.OperationsServer.
type OperationsServer struct {
	lropb.UnimplementedOperationsServer
	admin *DatabaseAdminServer
}

// NewOperationsServer creates a new OperationsServer.
func NewOperationsServer(admin *DatabaseAdminServer) *OperationsServer {
	return &OperationsServer{admin: admin}
}

func (s *OperationsServer) GetOperation(ctx context.Context, req *lropb.GetOperationRequest) (*lropb.Operation, error) {
	s.admin.mu.Lock()
	op, ok := s.admin.operations[req.Name]
	s.admin.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "operation %q not found", req.Name)
	}
	return op, nil
}
