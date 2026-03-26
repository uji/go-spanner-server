# compattest - Compatibility Test Suite

This package provides a dual-backend compatibility testing framework for go-spanner-server. It runs the same test cases against both go-spanner-server (in-process) and the Cloud Spanner Emulator, ensuring behavioral parity.

## Architecture

- **`helper_test.go`** — `testBackend` interface, `runCompat` helper, and both backend implementations (go-spanner-server and Cloud Spanner Emulator).
- **`mutations_test.go`** — Mutation test cases (Insert, Update, Delete, Replace, InsertOrUpdate, etc.).
- **`queries_test.go`** — Query test cases (WHERE clause conditions, etc.).

## Running Tests

### go-spanner-server only (no external dependencies)

```bash
cd compattest && go test ./... -v
```

### With Cloud Spanner Emulator

```bash
# Start the emulator
docker run -d --name spanner-emulator -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

# Run tests against both backends
SPANNER_EMULATOR_HOST=localhost:9010 go test ./... -v

# Stop the emulator
docker stop spanner-emulator && docker rm spanner-emulator
```

Or use the Makefile from the project root:

```bash
make test-compat
```

## Adding a New Test Case

1. **Define DDL, test function, and Test function** in the appropriate `_test.go` file (`mutations_test.go` for mutation tests, `queries_test.go` for query tests):

```go
var myFeatureDDL = []string{
    `CREATE TABLE MyTable (
        Id INT64 NOT NULL,
        Value STRING(256),
    ) PRIMARY KEY (Id)`,
}

func runMyFeature(ctx context.Context, t *testing.T, client *spanner.Client) {
    t.Helper()
    // Use client to test your feature
}

func TestCompat_MyFeature(t *testing.T) {
    runCompat(t, myFeatureDDL, runMyFeature)
}
```

## Module Independence

This package is a separate Go module (`compattest/go.mod`) to keep test-only dependencies (e.g., instance admin API) out of the main `go.mod`. It references the parent module via a `replace` directive.
