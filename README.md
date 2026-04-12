# go-spanner-server

An in-process, in-memory Cloud Spanner compatible server written in pure Go.
(inspired by https://github.com/dolthub/go-mysql-server)

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

- **DDL**: `CREATE TABLE` (INT64, STRING, BOOL, FLOAT64, BYTES, TIMESTAMP, ARRAY, STRUCT), `CREATE INDEX`, `DROP INDEX`, `DROP TABLE`, `ALTER TABLE` (ADD/DROP constraint, `ALTER COLUMN SET OPTIONS`)
- **Mutations**: Insert, InsertOrUpdate, Update, Replace, Delete; `spanner.CommitTimestamp` sentinel for commit-timestamp columns
- **Read**: `StreamingRead` (AllKeys, point lookups, key ranges), `ReadUsingIndex`, `ExecuteStreamingSql` (SELECT with WHERE clause, ORDER BY, LIMIT/OFFSET, expressions in SELECT list)
- **DML**: `INSERT INTO ... VALUES (...)`, `UPDATE ... SET ... WHERE ...`, `DELETE FROM ... WHERE ...` (via `ExecuteSql` / `ExecuteStreamingSql`); `PENDING_COMMIT_TIMESTAMP()` in INSERT/UPDATE
- **Commit Timestamps**: `OPTIONS (allow_commit_timestamp = true)` on TIMESTAMP columns; `CommitTimestamp` returned in `CommitResponse`
- **SQL Functions**: `COALESCE`, `IF`, `IFNULL`, `NULLIF`, `CAST`/`SAFE_CAST`, `CASE`/`WHEN`/`THEN`/`ELSE`; string functions (`CONCAT`, `UPPER`, `LOWER`, `LENGTH`, `SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `STARTS_WITH`, `ENDS_WITH`, `REPLACE`, `STRPOS`, `LPAD`, `RPAD`, `REVERSE`, `REPEAT`); math functions (`ABS`, `MOD`, `CEIL`, `FLOOR`, `ROUND`, `SIGN`, `GREATEST`, `LEAST`); `CURRENT_TIMESTAMP`, `GENERATE_UUID`, `PENDING_COMMIT_TIMESTAMP`
- **Secondary Indexes**: UNIQUE, NULL_FILTERED, STORING, ASC/DESC key ordering
- **Interleaved Tables**: `INTERLEAVE IN PARENT`, `ON DELETE CASCADE`, `ON DELETE NO ACTION`, referential integrity enforcement
- **Sessions**: CreateSession, BatchCreateSessions, GetSession, DeleteSession
- **Transactions**: BeginTransaction, Commit, Rollback
- **Admin**: CreateDatabase, UpdateDatabaseDdl, GetDatabase, GetDatabaseDdl

### Not yet supported

- Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ARRAY_AGG`, etc.) and `GROUP BY` / `HAVING`
- `JOIN` (INNER, LEFT, RIGHT, CROSS)
- Subqueries and CTEs (`WITH`)
- `DISTINCT`
- `UNION` / `INTERSECT` / `EXCEPT`
- Window functions
- Additional SQL functions (regex, JSON, date arithmetic, etc.)
