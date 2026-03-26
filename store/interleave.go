package store

import "fmt"

// findChildRows returns all rows in childTable whose PK prefix matches parentKey.
// parentKey contains the parent table's PK values (len == len(parent.PKCols)).
// Since child PK starts with parent PK columns, this is an efficient prefix scan on the B-Tree.
func findChildRows(childTable *Table, parentKey []any) []Row {
	parentPKLen := len(parentKey)

	// Build a probe row with parent key values in the child's PK positions
	probe := Row{Data: make([]any, len(childTable.Cols))}
	for i, val := range parentKey {
		probe.Data[childTable.PKCols[i]] = val
	}

	var rows []Row
	childTable.Rows.AscendGreaterOrEqual(probe, func(r Row) bool {
		// Stop if the PK prefix no longer matches
		for i := 0; i < parentPKLen; i++ {
			if CompareValues(r.Data[childTable.PKCols[i]], parentKey[i]) != 0 {
				return false
			}
		}
		rows = append(rows, r)
		return true
	})
	return rows
}

// checkParentExists verifies that the parent row exists for a child row being inserted.
func (db *Database) checkParentExists(table *Table, cols []string, vals []any) error {
	parentTable := db.Tables[table.ParentTableName]

	// Build a data array for the child row to extract parent PK values
	data := make([]any, len(table.Cols))
	for i, col := range cols {
		if idx, ok := table.ColIndex[col]; ok {
			data[idx] = vals[i]
		}
	}

	// Extract parent PK values from child row data (first N PK columns are shared)
	parentPKLen := len(parentTable.PKCols)
	parentProbe := Row{Data: make([]any, len(parentTable.Cols))}
	for i := 0; i < parentPKLen; i++ {
		childColIdx := table.PKCols[i]
		parentColIdx := parentTable.PKCols[i]
		parentProbe.Data[parentColIdx] = data[childColIdx]
	}

	if _, ok := parentTable.Rows.Get(parentProbe); !ok {
		return fmt.Errorf("parent row not found in table %q for insert into %q", table.ParentTableName, table.Name)
	}
	return nil
}

// cascadeDelete handles ON DELETE CASCADE / NO ACTION for all child tables of the given table.
// It first checks all NO ACTION constraints recursively, then performs CASCADE deletes.
func (db *Database) cascadeDelete(table *Table, key []any) error {
	// First pass: check NO ACTION constraints
	if err := db.checkNoActionConstraints(table, key); err != nil {
		return err
	}
	// Second pass: perform cascade deletes
	db.performCascadeDeletes(table, key)
	return nil
}

func (db *Database) checkNoActionConstraints(table *Table, key []any) error {
	for _, child := range table.ChildTables {
		childRows := findChildRows(child, key)
		if child.OnDelete == OnDeleteNoAction && len(childRows) > 0 {
			return fmt.Errorf("cannot delete from %q: child rows exist in %q (ON DELETE NO ACTION)", table.Name, child.Name)
		}
		if child.OnDelete == OnDeleteCascade {
			for _, row := range childRows {
				childKey := extractPKValues(child, row)
				if err := db.checkNoActionConstraints(child, childKey); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (db *Database) performCascadeDeletes(table *Table, key []any) {
	for _, child := range table.ChildTables {
		if child.OnDelete != OnDeleteCascade {
			continue
		}
		childRows := findChildRows(child, key)
		for _, row := range childRows {
			childKey := extractPKValues(child, row)
			db.performCascadeDeletes(child, childKey)
		}
		// Delete all child rows
		for _, row := range childRows {
			if old, ok := child.Rows.Delete(row); ok {
				child.updateIndexesOnDelete(old)
			}
		}
	}
}

func extractPKValues(table *Table, row Row) []any {
	key := make([]any, len(table.PKCols))
	for i, pkIdx := range table.PKCols {
		key[i] = row.Data[pkIdx]
	}
	return key
}

// InsertRow inserts a row into the table, enforcing interleave parent existence.
func (db *Database) InsertRow(table *Table, cols []string, vals []any) error {
	if table.ParentTableName != "" {
		if err := db.checkParentExists(table, cols, vals); err != nil {
			return err
		}
	}
	return table.InsertRow(cols, vals)
}

// ReplaceRow replaces a row in the table, enforcing interleave parent existence for new rows.
func (db *Database) ReplaceRow(table *Table, cols []string, vals []any) error {
	if table.ParentTableName != "" {
		// Check if row exists; if not, parent must exist
		data := make([]any, len(table.Cols))
		for i, col := range cols {
			if idx, ok := table.ColIndex[col]; ok {
				data[idx] = vals[i]
			}
		}
		probe := Row{Data: data}
		if _, ok := table.Rows.Get(probe); !ok {
			if err := db.checkParentExists(table, cols, vals); err != nil {
				return err
			}
		}
	}
	return table.ReplaceRow(cols, vals)
}

// DeleteByKeys deletes rows matching the given keys, enforcing interleave CASCADE/NO ACTION.
func (db *Database) DeleteByKeys(table *Table, keys [][]any) error {
	for _, key := range keys {
		if err := db.cascadeDelete(table, key); err != nil {
			return err
		}
	}
	table.DeleteByKeys(keys)
	return nil
}

// DeleteByRange deletes rows in the given range, enforcing interleave CASCADE/NO ACTION.
func (db *Database) DeleteByRange(table *Table, startKey, endKey []any, startClosed, endClosed bool) error {
	if len(table.ChildTables) > 0 {
		// Collect all column indexes to get full rows, then extract PK values
		allCols := make([]int, len(table.Cols))
		for i := range allCols {
			allCols[i] = i
		}
		rows := table.ReadByRange(startKey, endKey, startClosed, endClosed, allCols)
		for _, row := range rows {
			key := extractPKValues(table, row)
			if err := db.cascadeDelete(table, key); err != nil {
				return err
			}
		}
	}
	table.DeleteByRange(startKey, endKey, startClosed, endClosed)
	return nil
}

// DeleteAll deletes all rows from the table, enforcing interleave CASCADE/NO ACTION.
// It uses a two-pass approach: first validate all NO ACTION constraints recursively,
// then perform the actual deletions to ensure atomicity.
func (db *Database) DeleteAll(table *Table) error {
	if err := db.checkDeleteAllConstraints(table); err != nil {
		return err
	}
	db.performDeleteAll(table)
	return nil
}

func (db *Database) checkDeleteAllConstraints(table *Table) error {
	for _, child := range table.ChildTables {
		if child.OnDelete == OnDeleteNoAction {
			hasRows := false
			child.Rows.Ascend(func(r Row) bool {
				hasRows = true
				return false // stop after first
			})
			if hasRows {
				return fmt.Errorf("cannot delete all from %q: child rows exist in %q (ON DELETE NO ACTION)", table.Name, child.Name)
			}
		} else if child.OnDelete == OnDeleteCascade {
			if err := db.checkDeleteAllConstraints(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *Database) performDeleteAll(table *Table) {
	for _, child := range table.ChildTables {
		if child.OnDelete == OnDeleteCascade {
			db.performDeleteAll(child)
		}
	}
	table.DeleteAll()
}
