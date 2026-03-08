package compattest

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
)

// TestBackend represents a Spanner-compatible backend for testing.
type TestBackend interface {
	Name() string
	Setup(ctx context.Context, t *testing.T, ddl []string) (client *spanner.Client, cleanup func())
}

// backends returns the list of available test backends.
func backends() []TestBackend {
	bs := []TestBackend{
		&serverBackend{},
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") != "" {
		bs = append(bs, &emulatorBackend{})
	}
	return bs
}

// RunCompat runs a test function against all available backends.
func RunCompat(t *testing.T, ddl []string, fn func(context.Context, *testing.T, *spanner.Client)) {
	t.Helper()
	for _, b := range backends() {
		b := b
		t.Run(b.Name(), func(t *testing.T) {
			ctx := context.Background()
			client, cleanup := b.Setup(ctx, t, ddl)
			defer cleanup()
			fn(ctx, t, client)
		})
	}
}
