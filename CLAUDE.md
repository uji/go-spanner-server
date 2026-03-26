# CLAUDE.md

## Individual Preferences
- @~/.claude/go-spanner-server.md

## Development Commands

### フォーマット・静的解析
```bash
go fmt ./...       # コードフォーマット
go vet ./...       # 静的解析
go fix ./...       # 古いAPIの自動修正
```

### テスト
```bash
go test ./... -v   # ユニットテスト
make test-compat   # 互換性テスト（Spanner Emulatorが必要、Dockerで自動起動）
```
