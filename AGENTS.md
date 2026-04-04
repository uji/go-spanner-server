# AGENTS.md

## Development Commands

### Format & Lint
```bash
go fmt ./...       # format code
go vet ./...       # static analysis
go fix ./...       # auto-fix deprecated API usage
```

### Test
```bash
go test ./... -v   # unit tests
make test-compat   # compatibility tests (requires Spanner Emulator, auto-started via Docker)
```
