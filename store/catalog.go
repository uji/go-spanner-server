package store

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// Database represents an in-memory Spanner database.
type Database struct {
	mu     sync.RWMutex
	Tables map[string]*Table
	DDLs   []string
}

// NewDatabase creates a new empty database.
func NewDatabase() *Database {
	return &Database{
		Tables: make(map[string]*Table),
	}
}

// ApplyDDL parses and applies a DDL statement.
func (db *Database) ApplyDDL(sql string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	stmt, err := memefish.ParseDDL("", sql)
	if err != nil {
		return fmt.Errorf("parse DDL: %w", err)
	}

	switch s := stmt.(type) {
	case *ast.CreateTable:
		return db.applyCreateTable(s, sql)
	case *ast.CreateIndex:
		return db.applyCreateIndex(s, sql)
	case *ast.DropIndex:
		return db.applyDropIndex(s, sql)
	case *ast.DropTable:
		return db.applyDropTable(s, sql)
	case *ast.AlterTable:
		return db.applyAlterTable(s, sql)
	default:
		return fmt.Errorf("unsupported DDL statement: %T", stmt)
	}
}

func (db *Database) applyCreateTable(ct *ast.CreateTable, rawSQL string) error {
	tableName := ct.Name.Idents[0].Name

	if _, exists := db.Tables[tableName]; exists {
		return fmt.Errorf("table %q already exists", tableName)
	}

	var cols []ColInfo
	for _, colDef := range ct.Columns {
		colType := extractColType(colDef.Type)
		notNull := colDef.NotNull
		allowCommitTS, err := extractAllowCommitTimestamp(colDef)
		if err != nil {
			return err
		}
		if allowCommitTS && colType.Name != TypeTimestamp {
			return fmt.Errorf("column %q: OPTIONS (allow_commit_timestamp = true) is only valid on TIMESTAMP columns", colDef.Name.Name)
		}
		cols = append(cols, ColInfo{
			Name:                 colDef.Name.Name,
			Type:                 colType,
			NotNull:              notNull,
			AllowCommitTimestamp: allowCommitTS,
		})
	}

	// Build column index for PK resolution
	colIndex := make(map[string]int, len(cols))
	for i, c := range cols {
		colIndex[c.Name] = i
	}

	// Extract primary key columns
	var pkCols []int
	if ct.PrimaryKeys != nil {
		for _, pk := range ct.PrimaryKeys {
			idx, ok := colIndex[pk.Name.Name]
			if !ok {
				return fmt.Errorf("primary key column %q not found", pk.Name.Name)
			}
			pkCols = append(pkCols, idx)
		}
	}

	table := NewTable(tableName, cols, pkCols)

	db.Tables[tableName] = table

	for _, tc := range ct.TableConstraints {
		fk, ok := tc.Constraint.(*ast.ForeignKey)
		if !ok {
			continue
		}
		if err := db.processForeignKey(table, tc.Name, fk); err != nil {
			delete(db.Tables, tableName)
			return err
		}
	}

	if ct.Cluster != nil {
		parentName := ct.Cluster.TableName.Idents[0].Name
		parentTable, ok := db.Tables[parentName]
		if !ok {
			delete(db.Tables, tableName)
			return fmt.Errorf("parent table %q not found", parentName)
		}
		if err := validateInterleave(parentTable, table); err != nil {
			delete(db.Tables, tableName)
			return err
		}
		table.ParentTableName = parentName
		if ct.Cluster.OnDelete == ast.OnDeleteCascade {
			table.OnDelete = OnDeleteCascade
		} else {
			table.OnDelete = OnDeleteNoAction
		}
		parentTable.ChildTables = append(parentTable.ChildTables, table)
	}

	db.DDLs = append(db.DDLs, rawSQL)
	return nil
}

func validateInterleave(parent, child *Table) error {
	if len(child.PKCols) <= len(parent.PKCols) {
		return fmt.Errorf("child table %q primary key must have more columns than parent %q", child.Name, parent.Name)
	}
	for i, parentPKIdx := range parent.PKCols {
		childPKIdx := child.PKCols[i]
		parentCol := parent.Cols[parentPKIdx]
		childCol := child.Cols[childPKIdx]
		if parentCol.Name != childCol.Name {
			return fmt.Errorf("child primary key column %d must be %q (same as parent), got %q", i, parentCol.Name, childCol.Name)
		}
		if parentCol.Type.Name != childCol.Type.Name {
			return fmt.Errorf("child primary key column %q type must match parent: parent=%s, child=%s", childCol.Name, parentCol.Type.Name, childCol.Type.Name)
		}
	}
	return nil
}

func (db *Database) applyCreateIndex(ci *ast.CreateIndex, rawSQL string) error {
	indexName := ci.Name.Idents[0].Name
	tableName := ci.TableName.Idents[0].Name

	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %q not found", tableName)
	}

	if _, exists := table.Indexes[indexName]; exists {
		if ci.IfNotExists {
			return nil
		}
		return fmt.Errorf("index %q already exists", indexName)
	}

	// Resolve index key columns
	keyCols := make([]IndexKeyCol, len(ci.Keys))
	for i, key := range ci.Keys {
		colIdx, ok := table.ColIndex[key.Name.Name]
		if !ok {
			return fmt.Errorf("column %q not found in table %q", key.Name.Name, tableName)
		}
		keyCols[i] = IndexKeyCol{
			ColIdx: colIdx,
			Desc:   key.Dir == ast.DirectionDesc,
		}
	}

	// Resolve STORING columns
	var storingCols []int
	if ci.Storing != nil {
		for _, col := range ci.Storing.Columns {
			colIdx, ok := table.ColIndex[col.Name]
			if !ok {
				return fmt.Errorf("STORING column %q not found in table %q", col.Name, tableName)
			}
			storingCols = append(storingCols, colIdx)
		}
	}

	idx := NewIndex(indexName, tableName, keyCols, table.PKCols, storingCols, ci.Unique, ci.NullFiltered)

	// Build index from existing rows
	var buildErr error
	table.Rows.Ascend(func(r Row) bool {
		if err := idx.Insert(r); err != nil {
			buildErr = err
			return false
		}
		return true
	})
	if buildErr != nil {
		return buildErr
	}

	table.Indexes[indexName] = idx
	db.DDLs = append(db.DDLs, rawSQL)
	return nil
}

func (db *Database) processForeignKey(table *Table, nameIdent *ast.Ident, fk *ast.ForeignKey) error {
	// Resolve referencing columns
	fkCols := make([]int, len(fk.Columns))
	for i, col := range fk.Columns {
		idx, ok := table.ColIndex[col.Name]
		if !ok {
			return fmt.Errorf("foreign key column %q not found in table %q", col.Name, table.Name)
		}
		fkCols[i] = idx
	}

	// Resolve referenced table
	refTableName := fk.ReferenceTable.Idents[0].Name
	refTable, ok := db.Tables[refTableName]
	if !ok {
		return fmt.Errorf("referenced table %q not found", refTableName)
	}

	// Resolve referenced columns
	refCols := make([]int, len(fk.ReferenceColumns))
	for i, col := range fk.ReferenceColumns {
		idx, ok := refTable.ColIndex[col.Name]
		if !ok {
			return fmt.Errorf("referenced column %q not found in table %q", col.Name, refTableName)
		}
		refCols[i] = idx
	}

	if len(fkCols) != len(refCols) {
		return fmt.Errorf("foreign key column count mismatch: %d referencing vs %d referenced", len(fkCols), len(refCols))
	}

	// Determine constraint name
	constraintName := ""
	if nameIdent != nil {
		constraintName = nameIdent.Name
	} else {
		constraintName = fmt.Sprintf("FK_%s_%s_%d", table.Name, refTableName, len(table.ForeignKeys)+1)
	}

	onDelete := OnDeleteNoAction
	if fk.OnDelete == ast.OnDeleteCascade {
		onDelete = OnDeleteCascade
	}

	enforced := fk.Enforcement != ast.NotEnforced

	constraintIdx := len(table.ForeignKeys)
	table.ForeignKeys = append(table.ForeignKeys, ForeignKeyConstraint{
		Name:             constraintName,
		Columns:          fkCols,
		ReferenceTable:   refTableName,
		ReferenceColumns: refCols,
		OnDelete:         onDelete,
		Enforced:         enforced,
	})

	refTable.ReferencedBy = append(refTable.ReferencedBy, &ForeignKeyRef{
		ConstraintName: constraintName,
		ChildTable:     table,
		ConstraintIdx:  constraintIdx,
	})

	return nil
}

func (db *Database) applyAlterTable(at *ast.AlterTable, rawSQL string) error {
	tableName := at.Name.Idents[0].Name
	table, ok := db.Tables[tableName]
	if !ok {
		return fmt.Errorf("table %q not found", tableName)
	}

	switch alt := at.TableAlteration.(type) {
	case *ast.AddTableConstraint:
		fk, ok := alt.TableConstraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return fmt.Errorf("unsupported constraint type: %T", alt.TableConstraint.Constraint)
		}
		if err := db.processForeignKey(table, alt.TableConstraint.Name, fk); err != nil {
			return err
		}
	case *ast.DropConstraint:
		if err := db.dropConstraint(table, alt.Name.Name); err != nil {
			return err
		}
	case *ast.AlterColumn:
		if err := db.applyAlterColumn(table, alt); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported ALTER TABLE operation: %T", at.TableAlteration)
	}

	db.DDLs = append(db.DDLs, rawSQL)
	return nil
}

func (db *Database) dropConstraint(table *Table, constraintName string) error {
	for i, fk := range table.ForeignKeys {
		if fk.Name != constraintName {
			continue
		}
		// Remove from referenced table's ReferencedBy
		refTable := db.Tables[fk.ReferenceTable]
		for j, ref := range refTable.ReferencedBy {
			if ref.ConstraintName == constraintName {
				refTable.ReferencedBy = append(refTable.ReferencedBy[:j], refTable.ReferencedBy[j+1:]...)
				break
			}
		}
		table.ForeignKeys = append(table.ForeignKeys[:i], table.ForeignKeys[i+1:]...)
		// Update ConstraintIdx for remaining refs pointing to this table
		for _, ref := range refTable.ReferencedBy {
			if ref.ChildTable == table && ref.ConstraintIdx > i {
				ref.ConstraintIdx--
			}
		}
		return nil
	}
	return fmt.Errorf("constraint %q not found on table %q", constraintName, table.Name)
}

func (db *Database) applyDropTable(dt *ast.DropTable, rawSQL string) error {
	tableName := dt.Name.Idents[0].Name

	table, ok := db.Tables[tableName]
	if !ok {
		if dt.IfExists {
			return nil
		}
		return fmt.Errorf("table %q not found", tableName)
	}

	if len(table.ChildTables) > 0 {
		childNames := make([]string, len(table.ChildTables))
		for i, c := range table.ChildTables {
			childNames[i] = c.Name
		}
		return fmt.Errorf("cannot drop table %q: interleaved child tables exist: %v", tableName, childNames)
	}

	if len(table.ReferencedBy) > 0 {
		constraintNames := make([]string, len(table.ReferencedBy))
		for i, ref := range table.ReferencedBy {
			constraintNames[i] = ref.ConstraintName
		}
		return fmt.Errorf("cannot drop table %q: referenced by foreign key constraints: %v", tableName, constraintNames)
	}

	// Remove from parent's ChildTables
	if table.ParentTableName != "" {
		if parent, ok := db.Tables[table.ParentTableName]; ok {
			for i, c := range parent.ChildTables {
				if c.Name == tableName {
					parent.ChildTables = append(parent.ChildTables[:i], parent.ChildTables[i+1:]...)
					break
				}
			}
		}
	}

	delete(db.Tables, tableName)
	db.DDLs = append(db.DDLs, rawSQL)
	return nil
}

func (db *Database) applyDropIndex(di *ast.DropIndex, rawSQL string) error {
	indexName := di.Name.Idents[0].Name

	for _, table := range db.Tables {
		if _, ok := table.Indexes[indexName]; ok {
			delete(table.Indexes, indexName)
			db.DDLs = append(db.DDLs, rawSQL)
			return nil
		}
	}

	if di.IfExists {
		return nil
	}
	return fmt.Errorf("index %q not found", indexName)
}

func extractColType(typ ast.SchemaType) ColType {
	switch t := typ.(type) {
	case *ast.ScalarSchemaType:
		return ScalarColType(strings.ToUpper(string(t.Name)))
	case *ast.SizedSchemaType:
		return ScalarColType(strings.ToUpper(string(t.Name)))
	case *ast.ArraySchemaType:
		elem := extractColType(t.Item)
		return ColType{Name: TypeArray, ArrayElem: &elem}
	default:
		return ScalarColType(TypeString)
	}
}

// applyAlterColumn applies an ALTER COLUMN operation to a table.
func (db *Database) applyAlterColumn(table *Table, alt *ast.AlterColumn) error {
	colName := alt.Name.Name
	colIdx, ok := table.ColIndex[colName]
	if !ok {
		return fmt.Errorf("column %q not found in table %q", colName, table.Name)
	}

	setOpts, ok := alt.Alteration.(*ast.AlterColumnSetOptions)
	if !ok {
		return fmt.Errorf("unsupported ALTER COLUMN operation: %T", alt.Alteration)
	}

	for _, rec := range setOpts.Options.Records {
		switch strings.ToLower(rec.Name.Name) {
		case "allow_commit_timestamp":
			switch v := rec.Value.(type) {
			case *ast.BoolLiteral:
				if v.Value && table.Cols[colIdx].Type.Name != TypeTimestamp {
					return fmt.Errorf("column %q: OPTIONS (allow_commit_timestamp = true) is only valid on TIMESTAMP columns", colName)
				}
				table.Cols[colIdx].AllowCommitTimestamp = v.Value
			case *ast.NullLiteral:
				table.Cols[colIdx].AllowCommitTimestamp = false
			default:
				return fmt.Errorf("column %q: allow_commit_timestamp must be a bool or null, got %T", colName, rec.Value)
			}
		default:
			return fmt.Errorf("unknown column option %q", rec.Name.Name)
		}
	}
	return nil
}

// extractAllowCommitTimestamp returns true if the column definition has OPTIONS (allow_commit_timestamp = true).
func extractAllowCommitTimestamp(colDef *ast.ColumnDef) (bool, error) {
	if colDef.Options == nil {
		return false, nil
	}
	for _, rec := range colDef.Options.Records {
		switch strings.ToLower(rec.Name.Name) {
		case "allow_commit_timestamp":
			switch v := rec.Value.(type) {
			case *ast.BoolLiteral:
				return v.Value, nil
			case *ast.NullLiteral:
				return false, nil
			default:
				return false, fmt.Errorf("column %q: allow_commit_timestamp must be a bool or null, got %T", colDef.Name.Name, rec.Value)
			}
		default:
			return false, fmt.Errorf("unknown column option %q", rec.Name.Name)
		}
	}
	return false, nil
}

// GetTable returns a table by name.
func (db *Database) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	t, ok := db.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	return t, nil
}

// GetDDLs returns all applied DDL statements.
func (db *Database) GetDDLs() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	result := make([]string, len(db.DDLs))
	copy(result, db.DDLs)
	return result
}
