package compattest

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
)

// testBackend represents a Spanner-compatible backend for testing.
type testBackend interface {
	Name() string
	Setup(ctx context.Context, t *testing.T, ddl []string) (client *spanner.Client, cleanup func())
}

// backends returns the list of available test backends.
func backends() []testBackend {
	bs := []testBackend{
		&serverBackend{},
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		bs = append(bs, &emulatorBackend{})
	}
	return bs
}

// runCompat runs a test function against all available backends.
func runCompat(t *testing.T, ddl []string, fn func(context.Context, *testing.T, *spanner.Client)) {
	t.Helper()
	for _, b := range backends() {
		t.Run(b.Name(), func(t *testing.T) {
			ctx := context.Background()
			client, cleanup := b.Setup(ctx, t, ddl)
			defer cleanup()
			fn(ctx, t, client)
		})
	}
}
