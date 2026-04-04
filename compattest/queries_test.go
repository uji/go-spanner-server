package compattest

import (
	"testing"

	"github.com/uji/go-spanner-server/compattest/testutil"
)

func TestCompat_Queries(t *testing.T) {
	testutil.RunTestFiles(t, "testdata/queries/*.sql")
}
