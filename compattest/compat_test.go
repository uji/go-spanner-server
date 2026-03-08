package compattest_test

import (
	"testing"

	"github.com/uji/go-spanner-server/compattest"
)

func TestCompat_InsertAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.InsertAndReadDDL, compattest.RunInsertAndRead)
}
