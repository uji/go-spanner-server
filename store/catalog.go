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
		typeName := extractTypeName(colDef.Type)
		notNull := colDef.NotNull
		cols = append(cols, ColInfo{
			Name:    colDef.Name.Name,
			Type:    typeName,
			NotNull: notNull,
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

	db.Tables[tableName] = NewTable(tableName, cols, pkCols)
	db.DDLs = append(db.DDLs, rawSQL)
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

func extractTypeName(typ ast.SchemaType) string {
	switch t := typ.(type) {
	case *ast.ScalarSchemaType:
		return strings.ToUpper(string(t.Name))
	case *ast.SizedSchemaType:
		return strings.ToUpper(string(t.Name))
	default:
		return "STRING"
	}
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
