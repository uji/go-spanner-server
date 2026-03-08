package store

import (
	"fmt"

	"github.com/google/btree"
)

// ColInfo describes a column.
type ColInfo struct {
	Name    string
	Type    string
	NotNull bool
}

// Table represents an in-memory Spanner table.
type Table struct {
	Name     string
	Cols     []ColInfo
	ColIndex map[string]int // column name → index
	PKCols   []int          // primary key column indexes
	Rows     *btree.BTreeG[Row]
}

// NewTable creates a new empty table.
func NewTable(name string, cols []ColInfo, pkCols []int) *Table {
	colIndex := make(map[string]int, len(cols))
	for i, c := range cols {
		colIndex[c.Name] = i
	}

	t := &Table{
		Name:     name,
		Cols:     cols,
		ColIndex: colIndex,
		PKCols:   pkCols,
	}

	t.Rows = btree.NewG(2, func(a, b Row) bool {
		return t.lessRow(a, b)
	})

	return t
}

func (t *Table) lessRow(a, b Row) bool {
	for _, pk := range t.PKCols {
		cmp := compareValues(a.Data[pk], b.Data[pk])
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
	}
	return false
}

// InsertRow inserts a row with the given column names and values.
func (t *Table) InsertRow(cols []string, vals []any) error {
	data := make([]any, len(t.Cols))
	for i, col := range cols {
		idx, ok := t.ColIndex[col]
		if !ok {
			return fmt.Errorf("column %q not found in table %q", col, t.Name)
		}
		data[idx] = vals[i]
	}

	row := Row{Data: data}

	// Check for duplicate primary key
	if _, ok := t.Rows.Get(row); ok {
		return fmt.Errorf("row already exists with given primary key in table %q", t.Name)
	}

	t.Rows.ReplaceOrInsert(row)
	return nil
}

// UpdateRow updates an existing row with the given column names and values.
// Only the specified columns are updated; other columns retain their existing values.
func (t *Table) UpdateRow(cols []string, vals []any) error {
	data := make([]any, len(t.Cols))
	for i, col := range cols {
		idx, ok := t.ColIndex[col]
		if !ok {
			return fmt.Errorf("column %q not found in table %q", col, t.Name)
		}
		data[idx] = vals[i]
	}

	probe := Row{Data: data}
	existing, ok := t.Rows.Get(probe)
	if !ok {
		return fmt.Errorf("row not found with given primary key in table %q", t.Name)
	}

	// Merge: keep existing values for columns not specified
	merged := make([]any, len(t.Cols))
	copy(merged, existing.Data)
	for i, col := range cols {
		idx := t.ColIndex[col]
		merged[idx] = vals[i]
	}

	t.Rows.ReplaceOrInsert(Row{Data: merged})
	return nil
}

// ReplaceRow replaces an existing row or inserts a new one.
func (t *Table) ReplaceRow(cols []string, vals []any) error {
	data := make([]any, len(t.Cols))
	for i, col := range cols {
		idx, ok := t.ColIndex[col]
		if !ok {
			return fmt.Errorf("column %q not found in table %q", col, t.Name)
		}
		data[idx] = vals[i]
	}

	t.Rows.ReplaceOrInsert(Row{Data: data})
	return nil
}

// DeleteAll removes all rows from the table.
func (t *Table) DeleteAll() {
	t.Rows.Clear(false)
}

// DeleteByKeys deletes rows matching the given keys.
// Non-existent keys are silently skipped.
func (t *Table) DeleteByKeys(keys [][]any) {
	for _, key := range keys {
		probe := Row{Data: make([]any, len(t.Cols))}
		for i, pk := range t.PKCols {
			if i < len(key) {
				probe.Data[pk] = key[i]
			}
		}
		t.Rows.Delete(probe)
	}
}

// DeleteByRange deletes rows where the primary key is within the specified range.
func (t *Table) DeleteByRange(startKey, endKey []any, startClosed, endClosed bool) {
	// Collect keys to delete first, then delete (avoid modifying during iteration)
	var toDelete []Row
	startProbe := Row{Data: make([]any, len(t.Cols))}
	for i, pk := range t.PKCols {
		if i < len(startKey) {
			startProbe.Data[pk] = startKey[i]
		}
	}

	endProbe := Row{Data: make([]any, len(t.Cols))}
	for i, pk := range t.PKCols {
		if i < len(endKey) {
			endProbe.Data[pk] = endKey[i]
		}
	}

	t.Rows.AscendGreaterOrEqual(startProbe, func(r Row) bool {
		cmp := 0
		for _, pk := range t.PKCols {
			cmp = compareValues(r.Data[pk], endProbe.Data[pk])
			if cmp != 0 {
				break
			}
		}
		if cmp > 0 || (cmp == 0 && !endClosed) {
			return false
		}
		if !startClosed {
			eqStart := true
			for _, pk := range t.PKCols {
				if compareValues(r.Data[pk], startProbe.Data[pk]) != 0 {
					eqStart = false
					break
				}
			}
			if eqStart {
				return true // skip start
			}
		}
		toDelete = append(toDelete, r)
		return true
	})

	for _, r := range toDelete {
		t.Rows.Delete(r)
	}
}

// ReadAll returns all rows, projecting only the specified column indexes.
func (t *Table) ReadAll(colIndexes []int) []Row {
	var result []Row
	t.Rows.Ascend(func(r Row) bool {
		projected := projectRow(r, colIndexes)
		result = append(result, projected)
		return true
	})
	return result
}

// ReadByKeys returns rows matching the given key sets.
// Each key is a slice of values for the primary key columns.
func (t *Table) ReadByKeys(keys [][]any, colIndexes []int) []Row {
	var result []Row
	for _, key := range keys {
		probe := Row{Data: make([]any, len(t.Cols))}
		for i, pk := range t.PKCols {
			if i < len(key) {
				probe.Data[pk] = key[i]
			}
		}
		if r, ok := t.Rows.Get(probe); ok {
			result = append(result, projectRow(r, colIndexes))
		}
	}
	return result
}

// ReadByRange returns rows where primary key is within [start, end].
func (t *Table) ReadByRange(startKey, endKey []any, startClosed, endClosed bool, colIndexes []int) []Row {
	startProbe := Row{Data: make([]any, len(t.Cols))}
	for i, pk := range t.PKCols {
		if i < len(startKey) {
			startProbe.Data[pk] = startKey[i]
		}
	}

	endProbe := Row{Data: make([]any, len(t.Cols))}
	for i, pk := range t.PKCols {
		if i < len(endKey) {
			endProbe.Data[pk] = endKey[i]
		}
	}

	var result []Row
	t.Rows.AscendGreaterOrEqual(startProbe, func(r Row) bool {
		cmp := 0
		for _, pk := range t.PKCols {
			cmp = compareValues(r.Data[pk], endProbe.Data[pk])
			if cmp != 0 {
				break
			}
		}
		if cmp > 0 || (cmp == 0 && !endClosed) {
			return false
		}
		if cmp == 0 || !t.lessRow(r, startProbe) {
			if !startClosed {
				// Check if exactly equal to start
				eqStart := true
				for _, pk := range t.PKCols {
					if compareValues(r.Data[pk], startProbe.Data[pk]) != 0 {
						eqStart = false
						break
					}
				}
				if eqStart {
					return true // skip this row
				}
			}
			result = append(result, projectRow(r, colIndexes))
		}
		return true
	})
	return result
}

func projectRow(r Row, colIndexes []int) Row {
	data := make([]any, len(colIndexes))
	for i, idx := range colIndexes {
		data[i] = r.Data[idx]
	}
	return Row{Data: data}
}

// ResolveColumnIndexes returns the indexes for the given column names.
func (t *Table) ResolveColumnIndexes(cols []string) ([]int, error) {
	indexes := make([]int, len(cols))
	for i, col := range cols {
		idx, ok := t.ColIndex[col]
		if !ok {
			return nil, fmt.Errorf("column %q not found in table %q", col, t.Name)
		}
		indexes[i] = idx
	}
	return indexes, nil
}
