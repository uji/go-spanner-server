package store

import "fmt"

// checkForeignKeyOnInsert verifies that all enforced FK constraints in table
// are satisfied by the given row (cols/vals). NULL FK column values bypass the check.
func (db *Database) checkForeignKeyOnInsert(table *Table, cols []string, vals []any) error {
	if len(table.ForeignKeys) == 0 {
		return nil
	}

	// Build a full data array from cols/vals
	data := make([]any, len(table.Cols))
	for i, col := range cols {
		if idx, ok := table.ColIndex[col]; ok {
			data[idx] = vals[i]
		}
	}

	for _, fk := range table.ForeignKeys {
		if !fk.Enforced {
			continue
		}

		// Extract FK column values; skip if any is NULL
		fkVals := make([]any, len(fk.Columns))
		hasNull := false
		for i, colIdx := range fk.Columns {
			fkVals[i] = data[colIdx]
			if fkVals[i] == nil {
				hasNull = true
				break
			}
		}
		if hasNull {
			continue
		}

		refTable := db.Tables[fk.ReferenceTable]
		if !rowExistsWithColumnValues(refTable, fk.ReferenceColumns, fkVals) {
			return fmt.Errorf("foreign key constraint %q violation on table %q: referenced row not found in %q", fk.Name, table.Name, fk.ReferenceTable)
		}
	}
	return nil
}

// rowExistsWithColumnValues returns true if a row exists in t where the given column indexes
// have the given values.
func rowExistsWithColumnValues(t *Table, colIndexes []int, vals []any) bool {
	found := false
	t.Rows.Ascend(func(r Row) bool {
		for i, colIdx := range colIndexes {
			if CompareValues(r.Data[colIdx], vals[i]) != 0 {
				return true // continue scanning
			}
		}
		found = true
		return false // stop
	})
	return found
}

// checkForeignKeyNoActionOnDelete checks all FK NO ACTION constraints for references
// to the given row. Returns an error if any referencing rows exist.
func (db *Database) checkForeignKeyNoActionOnDelete(table *Table, rowData []any) error {
	for _, ref := range table.ReferencedBy {
		fk := ref.ChildTable.ForeignKeys[ref.ConstraintIdx]
		if !fk.Enforced || fk.OnDelete != OnDeleteNoAction {
			continue
		}

		// Build the referenced column values from the row being deleted
		refVals := make([]any, len(fk.ReferenceColumns))
		for i, colIdx := range fk.ReferenceColumns {
			refVals[i] = rowData[colIdx]
		}

		if rowExistsWithColumnValues(ref.ChildTable, fk.Columns, refVals) {
			return fmt.Errorf("foreign key constraint %q violation: cannot delete from %q: referencing rows exist in %q", fk.Name, table.Name, ref.ChildTable.Name)
		}
	}
	return nil
}

// performForeignKeyCascadeDeletes deletes all FK CASCADE referencing rows for the given row.
func (db *Database) performForeignKeyCascadeDeletes(table *Table, rowData []any) {
	for _, ref := range table.ReferencedBy {
		fk := ref.ChildTable.ForeignKeys[ref.ConstraintIdx]
		if !fk.Enforced || fk.OnDelete != OnDeleteCascade {
			continue
		}

		refVals := make([]any, len(fk.ReferenceColumns))
		for i, colIdx := range fk.ReferenceColumns {
			refVals[i] = rowData[colIdx]
		}

		// Find and collect matching child rows
		var toDelete []Row
		ref.ChildTable.Rows.Ascend(func(r Row) bool {
			for i, colIdx := range fk.Columns {
				if CompareValues(r.Data[colIdx], refVals[i]) != 0 {
					return true
				}
			}
			toDelete = append(toDelete, r)
			return true
		})

		for _, r := range toDelete {
			// Recursively handle cascade for the child row
			db.performForeignKeyCascadeDeletes(ref.ChildTable, r.Data)
			if old, ok := ref.ChildTable.Rows.Delete(r); ok {
				ref.ChildTable.updateIndexesOnDelete(old)
			}
		}
	}
}

// checkForeignKeyNoActionOnDeleteAll checks FK NO ACTION constraints when deleting all rows.
func (db *Database) checkForeignKeyNoActionOnDeleteAll(table *Table) error {
	for _, ref := range table.ReferencedBy {
		fk := ref.ChildTable.ForeignKeys[ref.ConstraintIdx]
		if !fk.Enforced || fk.OnDelete != OnDeleteNoAction {
			continue
		}
		hasRows := false
		ref.ChildTable.Rows.Ascend(func(r Row) bool {
			hasRows = true
			return false
		})
		if hasRows {
			return fmt.Errorf("foreign key constraint %q violation: cannot delete all from %q: referencing rows exist in %q", fk.Name, table.Name, ref.ChildTable.Name)
		}
	}
	return nil
}

// performForeignKeyCascadeDeleteAll cascade-deletes all FK CASCADE referencing rows.
func (db *Database) performForeignKeyCascadeDeleteAll(table *Table) {
	for _, ref := range table.ReferencedBy {
		fk := ref.ChildTable.ForeignKeys[ref.ConstraintIdx]
		if !fk.Enforced || fk.OnDelete != OnDeleteCascade {
			continue
		}
		db.performForeignKeyCascadeDeleteAll(ref.ChildTable)
		ref.ChildTable.DeleteAll()
	}
}
