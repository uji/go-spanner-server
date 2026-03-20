package store

import (
	"fmt"

	"github.com/google/btree"
)

// IndexKeyCol describes a column in an index key.
type IndexKeyCol struct {
	ColIdx int  // column index in the parent table
	Desc   bool // true if DESC
}

// Index represents a secondary index on a table.
type Index struct {
	Name         string
	TableName    string
	KeyCols      []IndexKeyCol // index key columns
	PKCols       []int         // parent table primary key column indexes
	StoringCols  []int         // STORING column indexes
	Unique       bool
	NullFiltered bool
	Rows         *btree.BTreeG[Row]
}

// NewIndex creates a new empty index.
func NewIndex(name, tableName string, keyCols []IndexKeyCol, pkCols []int, storingCols []int, unique, nullFiltered bool) *Index {
	idx := &Index{
		Name:         name,
		TableName:    tableName,
		KeyCols:      keyCols,
		PKCols:       pkCols,
		StoringCols:  storingCols,
		Unique:       unique,
		NullFiltered: nullFiltered,
	}

	idx.Rows = btree.NewG(2, func(a, b Row) bool {
		return idx.lessRow(a, b)
	})

	return idx
}

// lessRow compares index rows: index key columns (ASC/DESC), then PK columns (ASC).
func (idx *Index) lessRow(a, b Row) bool {
	// Compare index key columns
	for i, kc := range idx.KeyCols {
		cmp := CompareValues(a.Data[i], b.Data[i])
		_ = kc
		if kc.Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
	}
	// Compare PK columns (always ASC)
	offset := len(idx.KeyCols)
	for i := range idx.PKCols {
		cmp := CompareValues(a.Data[offset+i], b.Data[offset+i])
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
	}
	return false
}

// BuildIndexRow constructs an index row from a full table row.
// Layout: [indexKey0, indexKey1, ..., pk0, pk1, ..., storing0, storing1, ...]
func (idx *Index) BuildIndexRow(tableRow Row) Row {
	data := make([]any, len(idx.KeyCols)+len(idx.PKCols)+len(idx.StoringCols))
	for i, kc := range idx.KeyCols {
		data[i] = tableRow.Data[kc.ColIdx]
	}
	offset := len(idx.KeyCols)
	for i, pk := range idx.PKCols {
		data[offset+i] = tableRow.Data[pk]
	}
	offset += len(idx.PKCols)
	for i, sc := range idx.StoringCols {
		data[offset+i] = tableRow.Data[sc]
	}
	return Row{Data: data}
}

// HasNullKey returns true if any index key column is NULL.
func (idx *Index) HasNullKey(tableRow Row) bool {
	for _, kc := range idx.KeyCols {
		if tableRow.Data[kc.ColIdx] == nil {
			return true
		}
	}
	return false
}

// extractPK extracts the primary key values from an index row.
func (idx *Index) extractPK(indexRow Row) []any {
	offset := len(idx.KeyCols)
	pk := make([]any, len(idx.PKCols))
	for i := range idx.PKCols {
		pk[i] = indexRow.Data[offset+i]
	}
	return pk
}

// checkUnique checks if inserting the given index row would violate a UNIQUE constraint.
// It compares only the index key columns (not PK columns).
func (idx *Index) checkUnique(indexRow Row) error {
	if !idx.Unique {
		return nil
	}

	// Check if any key column is NULL; NULLs don't violate UNIQUE.
	for i := range idx.KeyCols {
		if indexRow.Data[i] == nil {
			return nil
		}
	}

	// Build a minimum probe with only index key values (nil PKs) so
	// AscendGreaterOrEqual scans from the start of the matching key range.
	minProbe := Row{Data: make([]any, len(indexRow.Data))}
	for i := range idx.KeyCols {
		minProbe.Data[i] = indexRow.Data[i]
	}

	found := false
	idx.Rows.AscendGreaterOrEqual(minProbe, func(r Row) bool {
		// Stop if index key columns differ
		for i := range idx.KeyCols {
			if CompareValues(r.Data[i], indexRow.Data[i]) != 0 {
				return false
			}
		}
		// Same key columns - check if it's a different PK (real duplicate)
		offset := len(idx.KeyCols)
		samePK := true
		for i := range idx.PKCols {
			if CompareValues(r.Data[offset+i], indexRow.Data[offset+i]) != 0 {
				samePK = false
				break
			}
		}
		if !samePK {
			found = true
			return false
		}
		return true
	})
	if found {
		return fmt.Errorf("UNIQUE index %q violation on table %q", idx.Name, idx.TableName)
	}
	return nil
}

// Insert adds an index row. Returns error if UNIQUE violation.
func (idx *Index) Insert(tableRow Row) error {
	if idx.NullFiltered && idx.HasNullKey(tableRow) {
		return nil
	}
	indexRow := idx.BuildIndexRow(tableRow)
	if err := idx.checkUnique(indexRow); err != nil {
		return err
	}
	idx.Rows.ReplaceOrInsert(indexRow)
	return nil
}

// Delete removes an index row.
func (idx *Index) Delete(tableRow Row) {
	if idx.NullFiltered && idx.HasNullKey(tableRow) {
		return
	}
	indexRow := idx.BuildIndexRow(tableRow)
	idx.Rows.Delete(indexRow)
}

// ReadAll returns all index rows, looking up table rows for projection.
func (idx *Index) ReadAll(table *Table, colIndexes []int) []Row {
	var result []Row
	idx.Rows.Ascend(func(r Row) bool {
		tableRow := idx.lookupTableRow(table, r)
		if tableRow != nil {
			result = append(result, projectRow(*tableRow, colIndexes))
		}
		return true
	})
	return result
}

// ReadByKeys returns rows matching the given index key sets.
func (idx *Index) ReadByKeys(table *Table, keys [][]any, colIndexes []int) []Row {
	var result []Row
	for _, key := range keys {
		probe := idx.buildProbe(key)
		// Since index keys may map to multiple rows (non-unique), scan all matches.
		idx.Rows.AscendGreaterOrEqual(probe, func(r Row) bool {
			for i := range key {
				if CompareValues(r.Data[i], key[i]) != 0 {
					return false
				}
			}
			tableRow := idx.lookupTableRow(table, r)
			if tableRow != nil {
				result = append(result, projectRow(*tableRow, colIndexes))
			}
			return true
		})
	}
	return result
}

// ReadByRange returns rows where the index key is within the specified range.
func (idx *Index) ReadByRange(table *Table, startKey, endKey []any, startClosed, endClosed bool, colIndexes []int) []Row {
	startProbe := idx.buildProbe(startKey)
	endProbe := idx.buildProbe(endKey)

	var result []Row
	idx.Rows.AscendGreaterOrEqual(startProbe, func(r Row) bool {
		cmp := idx.compareKeyPrefix(r, endProbe, len(endKey))
		if cmp > 0 || (cmp == 0 && !endClosed) {
			return false
		}
		if !startClosed {
			if idx.compareKeyPrefix(r, startProbe, len(startKey)) == 0 {
				return true
			}
		}
		tableRow := idx.lookupTableRow(table, r)
		if tableRow != nil {
			result = append(result, projectRow(*tableRow, colIndexes))
		}
		return true
	})
	return result
}

// buildProbe builds a probe row from index key values.
func (idx *Index) buildProbe(key []any) Row {
	data := make([]any, len(idx.KeyCols)+len(idx.PKCols)+len(idx.StoringCols))
	copy(data, key)
	return Row{Data: data}
}

// compareKeyPrefix compares only the first n index key columns between two rows.
func (idx *Index) compareKeyPrefix(a, b Row, n int) int {
	for i := 0; i < n && i < len(idx.KeyCols); i++ {
		cmp := CompareValues(a.Data[i], b.Data[i])
		if idx.KeyCols[i].Desc {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

// lookupTableRow finds the table row from an index row using PK.
func (idx *Index) lookupTableRow(table *Table, indexRow Row) *Row {
	pk := idx.extractPK(indexRow)
	probe := Row{Data: make([]any, len(table.Cols))}
	for i, pkCol := range table.PKCols {
		if i < len(pk) {
			probe.Data[pkCol] = pk[i]
		}
	}
	if r, ok := table.Rows.Get(probe); ok {
		return &r
	}
	return nil
}

// IndexKeyCols returns the column indexes of the index key columns (for decoding key values).
func (idx *Index) IndexKeyCols() []int {
	cols := make([]int, len(idx.KeyCols))
	for i, kc := range idx.KeyCols {
		cols[i] = kc.ColIdx
	}
	return cols
}
