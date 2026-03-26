package compattest

import "testing"

func TestCompat_Queries(t *testing.T) {
	runTestFiles(t, "testdata/queries/*.sql")
}
