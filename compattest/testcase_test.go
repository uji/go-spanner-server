package compattest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/uji/go-spanner-server/compattest/testutil"
)

// runTestFile runs a single txtar test file.
func runTestFile(t *testing.T, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read test file %s: %v", path, err)
	}

	sections := testutil.ParseSections(string(data))
	ddl := testutil.ExtractDDL(sections)

	runCompat(t, ddl, func(ctx context.Context, t *testing.T, client *spanner.Client) {
		testutil.RunSections(ctx, t, client, sections)
	})
}

// runTestFiles runs all test files matching a glob pattern as subtests.
// Adding a new file automatically registers a new test case.
func runTestFiles(t *testing.T, pattern string) {
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
			runTestFile(t, f)
		})
	}
}
