package compattest

import (
	"testing"

	"github.com/uji/go-spanner-server/compattest/testutil"
)

func TestCompat_DMLs(t *testing.T) {
	testutil.RunTestFiles(t, "testdata/dmls/*.sql")
}
