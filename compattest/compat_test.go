package compattest_test

import (
	"testing"

	"github.com/uji/go-spanner-server/compattest"
)

func TestCompat_InsertAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.InsertAndReadDDL, compattest.RunInsertAndRead)
}

func TestCompat_UpdateAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.UpdateAndReadDDL, compattest.RunUpdateAndRead)
}

func TestCompat_DeleteAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.DeleteAndReadDDL, compattest.RunDeleteAndRead)
}

func TestCompat_InsertOrUpdateExisting(t *testing.T) {
	compattest.RunCompat(t, compattest.InsertOrUpdateExistingDDL, compattest.RunInsertOrUpdateExisting)
}

func TestCompat_ReplaceAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.ReplaceAndReadDDL, compattest.RunReplaceAndRead)
}

func TestCompat_DeleteAllAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.DeleteAllAndReadDDL, compattest.RunDeleteAllAndRead)
}

func TestCompat_DeleteByRangeAndRead(t *testing.T) {
	compattest.RunCompat(t, compattest.DeleteByRangeAndReadDDL, compattest.RunDeleteByRangeAndRead)
}
