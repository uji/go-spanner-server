# compattest - Compatibility Test Suite

This package provides a dual-backend compatibility testing framework for go-spanner-server. It runs the same test cases against both go-spanner-server (in-process) and the Cloud Spanner Emulator, ensuring behavioral parity.

## Architecture

- **`compattest.go`** — `TestBackend` interface and `RunCompat` helper that runs tests against all available backends.
- **`backend_server.go`** — In-process go-spanner-server backend using bufconn. Always available.
- **`backend_emulator.go`** — Cloud Spanner Emulator backend. Activated when `SPANNER_EMULATOR_HOST` is set.
- **`tests.go`** — Test case functions (DDL + test function pairs).
- **`compat_test.go`** — Test runner that wires test cases to `RunCompat`.

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

1. **Define DDL and test function** in `tests.go`:

```go
var MyFeatureDDL = []string{
    `CREATE TABLE MyTable (
        Id INT64 NOT NULL,
        Value STRING(256),
    ) PRIMARY KEY (Id)`,
}

func RunMyFeature(ctx context.Context, t *testing.T, client *spanner.Client) {
    t.Helper()
    // Use client to test your feature
}
```

2. **Add a test runner** in `compat_test.go`:

```go
func TestCompat_MyFeature(t *testing.T) {
    compattest.RunCompat(t, compattest.MyFeatureDDL, compattest.RunMyFeature)
}
```

## Module Independence

This package is a separate Go module (`compattest/go.mod`) to keep test-only dependencies (e.g., instance admin API) out of the main `go.mod`. It references the parent module via a `replace` directive.
