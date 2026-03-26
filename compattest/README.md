# compattest - Compatibility Test Suite

This package provides a dual-backend compatibility testing framework for go-spanner-server. It runs the same test cases against both go-spanner-server (in-process) and the Cloud Spanner Emulator, ensuring behavioral parity.

## Architecture

- **`helper_test.go`** — `testBackend` interface, `runCompat` helper, and both backend implementations (go-spanner-server and Cloud Spanner Emulator).
- **`testcase_test.go`** — Data-driven test framework: txtar parser, SQL-to-mutation converter, query runner, and result formatter.
- **`testdata/`** — Data-driven test case files in txtar format (`.sql` extension). Adding a file here automatically adds a test case.
- **`mutations_test.go`** — Mutation API test cases (Insert, Update, Delete, Replace, InsertOrUpdate, etc.).
- **`queries_test.go`** — Query test cases. Discovers and runs all files in `testdata/queries/*.sql`.

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

### Data-driven tests (query tests) — recommended

Create a `.sql` file in `testdata/queries/`. The file is picked up automatically without any Go code changes.

**File format (txtar):**

```
Any comment line

-- ddl --
CREATE TABLE MyTable (
    Id    INT64 NOT NULL,
    Value STRING(256),
) PRIMARY KEY (Id)

-- exec --
INSERT INTO MyTable (Id, Value) VALUES (1, 'hello');
INSERT INTO MyTable (Id, Value) VALUES (2, 'world')

-- query --
SELECT Id, Value FROM MyTable WHERE Id = 1

-- expect --
(1, "hello")

-- query --
SELECT Id FROM MyTable ORDER BY Id

-- expect --
(1)
(2)
```

**Sections:**

| Section | Description |
|---------|-------------|
| `-- ddl --` | DDL statements separated by `;`. Applied before the test runs. |
| `-- exec --` | INSERT statements separated by `;`. Converted to Spanner mutations and applied via `client.Apply()`. Supports `INSERT` and `INSERT OR UPDATE`. Each statement must have exactly one `VALUES` row. |
| `-- query --` | A SELECT statement to execute. |
| `-- expect --` | Expected rows for the preceding `query`, one row per line in `(val1, val2, ...)` format. Values: integers as `1`, strings as `"text"`, NULL as `NULL`. |

Multiple `query` / `expect` pairs can appear in one file, executed in order.

### Go-based tests (mutation API, index, interleave tests)

For tests that use the Mutation API directly, `ReadUsingIndex`, Admin API operations, or error assertions, add a Go test to the appropriate file:

```go
func TestCompat_MyFeature(t *testing.T) {
    runCompat(t, []string{`CREATE TABLE ...`}, func(ctx context.Context, t *testing.T, client *spanner.Client) {
        // Use client to test your feature
    })
}
```

## Module Independence

This package is a separate Go module (`compattest/go.mod`) to keep test-only dependencies (e.g., instance admin API) out of the main `go.mod`. It references the parent module via a `replace` directive.
