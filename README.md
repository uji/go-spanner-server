# go-spanner-server

An in-process, in-memory Cloud Spanner compatible server written in pure Go.

This library provides a lightweight pure Go alternative that works directly with the `cloud.google.com/go/spanner` client library.

## Usage

```go
import (
    spannerserver "github.com/uji/go-spanner-server"

    "cloud.google.com/go/spanner"
    database "cloud.google.com/go/spanner/admin/database/apiv1"
    databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
    "google.golang.org/api/option"
)

func TestExample(t *testing.T) {
    ctx := context.Background()

    // Start server
    srv := spannerserver.New()
    defer srv.Stop()

    conn, _ := srv.Conn(ctx)
    defer conn.Close()

    // Create database
    adminClient, _ := database.NewDatabaseAdminClient(ctx,
        option.WithGRPCConn(conn),
        option.WithoutAuthentication(),
    )
    op, _ := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
        Parent:          "projects/test/instances/test",
        CreateStatement: "CREATE DATABASE `testdb`",
        ExtraStatements: []string{
            "CREATE TABLE Users (UserId INT64 NOT NULL, Name STRING(256)) PRIMARY KEY (UserId)",
        },
    })
    op.Wait(ctx)

    // Create Spanner client
    client, _ := spanner.NewClient(ctx,
        "projects/test/instances/test/databases/testdb",
        option.WithGRPCConn(conn),
        option.WithoutAuthentication(),
    )
    defer client.Close()

    // Insert
    client.Apply(ctx, []*spanner.Mutation{
        spanner.InsertOrUpdate("Users", []string{"UserId", "Name"}, []interface{}{int64(1), "Alice"}),
    })

    // Read
    iter := client.Single().Read(ctx, "Users", spanner.AllKeys(), []string{"UserId", "Name"})
    defer iter.Stop()
    // ...
}
```

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                  Spanner Client                      │
│          (cloud.google.com/go/spanner)               │
└──────────────┬───────────────────────────────────────┘
               │ gRPC (in-process / bufconn)
┌──────────────▼───────────────────────────────────────┐
│               gRPC Server Layer                      │
│  ┌─────────────────────┐ ┌─────────────────────────┐ │
│  │   Spanner Service   │ │ DatabaseAdmin Service   │ │
│  │  (google.spanner.v1)│ │(spanner.admin.database) │ │
│  └────────┬────────────┘ └────────┬────────────────┘ │
│           │                       │                  │
│  ┌────────▼───────────────────────▼────────────────┐ │
│  │              Engine (Query Processor)           │ │
│  │  ┌──────────┐ ┌──────────┐ ┌─────────────────┐  │ │
│  │  │  Parser  │ │ Planner  │ │    Executor     │  │ │
│  │  │(memefish)│ │          │ │                 │  │ │
│  │  └──────────┘ └──────────┘ └─────────────────┘  │ │
│  └─────────────────────┬───────────────────────────┘ │
│                        │                             │
│  ┌─────────────────────▼───────────────────────────┐ │
│  │           In-Memory Storage Engine              │ │
│  │  ┌──────────┐ ┌──────────┐ ┌─────────────────┐  │ │
│  │  │  Catalog │ │  Tables  │ │    Indexes      │  │ │
│  │  │ (Schema) │ │  (Rows)  │ │  (Secondary)    │  │ │
│  │  └──────────┘ └──────────┘ └─────────────────┘  │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

## Features

### Supported

- **DDL**: `CREATE TABLE` (INT64, STRING, BOOL, FLOAT64, BYTES, TIMESTAMP), `CREATE INDEX`, `DROP INDEX`
- **Mutations**: Insert, InsertOrUpdate, Update, Replace, Delete
- **Read**: `StreamingRead` (AllKeys, point lookups, key ranges), `ReadUsingIndex`, `ExecuteStreamingSql` (SELECT with WHERE clause, ORDER BY)
- **Secondary Indexes**: UNIQUE, NULL_FILTERED, STORING, ASC/DESC key ordering
- **Sessions**: CreateSession, BatchCreateSessions, GetSession, DeleteSession
- **Transactions**: BeginTransaction, Commit, Rollback
- **Admin**: CreateDatabase, UpdateDatabaseDdl, GetDatabase, GetDatabaseDdl

### Not yet supported

- UPDATE / DELETE (DML)
- ARRAY / STRUCT types
- Interleaved tables
- Optimistic locking (timestamps)
- SQL functions and expressions
