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
