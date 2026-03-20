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
	Indexes  map[string]*Index
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
		Indexes:  make(map[string]*Index),
	}

	t.Rows = btree.NewG(2, func(a, b Row) bool {
		return t.lessRow(a, b)
	})

	return t
}

func (t *Table) lessRow(a, b Row) bool {
	for _, pk := range t.PKCols {
		cmp := CompareValues(a.Data[pk], b.Data[pk])
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

	if err := t.updateIndexesOnInsert(row); err != nil {
		return err
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

	newRow := Row{Data: merged}
	if err := t.updateIndexesOnUpdate(existing, newRow); err != nil {
		return err
	}
	t.Rows.ReplaceOrInsert(newRow)
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

	newRow := Row{Data: data}
	if old, ok := t.Rows.Get(newRow); ok {
		if err := t.updateIndexesOnUpdate(old, newRow); err != nil {
			return err
		}
	} else {
		if err := t.updateIndexesOnInsert(newRow); err != nil {
			return err
		}
	}
	t.Rows.ReplaceOrInsert(newRow)
	return nil
}

// DeleteAll removes all rows from the table.
func (t *Table) DeleteAll() {
	for _, idx := range t.Indexes {
		idx.Rows.Clear(false)
	}
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
		if old, ok := t.Rows.Delete(probe); ok {
			t.updateIndexesOnDelete(old)
		}
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
			cmp = CompareValues(r.Data[pk], endProbe.Data[pk])
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
				if CompareValues(r.Data[pk], startProbe.Data[pk]) != 0 {
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
		t.updateIndexesOnDelete(r)
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
			cmp = CompareValues(r.Data[pk], endProbe.Data[pk])
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
					if CompareValues(r.Data[pk], startProbe.Data[pk]) != 0 {
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

func (t *Table) updateIndexesOnInsert(newRow Row) error {
	for _, idx := range t.Indexes {
		if err := idx.Insert(newRow); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) updateIndexesOnDelete(oldRow Row) {
	for _, idx := range t.Indexes {
		idx.Delete(oldRow)
	}
}

func (t *Table) updateIndexesOnUpdate(oldRow, newRow Row) error {
	// Pre-validate UNIQUE constraints before modifying any index.
	for _, idx := range t.Indexes {
		if !idx.Unique {
			continue
		}
		if idx.NullFiltered && idx.HasNullKey(newRow) {
			continue
		}
		indexRow := idx.BuildIndexRow(newRow)
		oldIndexRow := idx.BuildIndexRow(oldRow)
		// Temporarily remove the old row to check if the new row conflicts with other entries.
		idx.Rows.Delete(oldIndexRow)
		err := idx.checkUnique(indexRow)
		// Restore the old row regardless of the result.
		idx.Rows.ReplaceOrInsert(oldIndexRow)
		if err != nil {
			return err
		}
	}
	t.updateIndexesOnDelete(oldRow)
	return t.updateIndexesOnInsert(newRow)
}
