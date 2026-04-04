package testutil

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
)

// RunTestFile runs a single txtar test file against all available backends.
// Adding a new file automatically registers a new test case.
func RunTestFile(t *testing.T, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read test file %s: %v", path, err)
	}

	sections := ParseSections(string(data))
	ddl := ExtractDDL(sections)

	RunCompat(t, ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		RunSections(ctx, t, client, sections)
	})
}

// RunTestFiles runs all test files matching a glob pattern as subtests.
// Adding a new file automatically registers a new test case.
func RunTestFiles(t *testing.T, pattern string) {
	t.Helper()

	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %q failed: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("no test files matched: %s", pattern)
	}

	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
		t.Run(name, func(t *testing.T) {
			RunTestFile(t, f)
		})
	}
}
