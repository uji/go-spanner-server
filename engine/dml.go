package engine

import (
	"fmt"
	"strconv"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/uji/go-spanner-server/store"
)

// ExecuteDML executes an INSERT, UPDATE, or DELETE statement and returns the number of affected rows.
func ExecuteDML(db *store.Database, sql string) (int64, error) {
	dml, err := memefish.ParseDML("", sql)
	if err != nil {
		return 0, fmt.Errorf("parse DML: %w", err)
	}
	return executeDML(db, dml)
}

func executeDML(db *store.Database, dml ast.DML) (int64, error) {
	switch d := dml.(type) {
	case *ast.Insert:
		return executeInsert(db, d)
	default:
		return 0, fmt.Errorf("unsupported DML type: %T", dml)
	}
}

func executeInsert(db *store.Database, ins *ast.Insert) (int64, error) {
	tableName := ins.TableName.Idents[0].Name
	table, err := db.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	cols := make([]string, len(ins.Columns))
	for i, col := range ins.Columns {
		cols[i] = col.Name
	}

	input, ok := ins.Input.(*ast.ValuesInput)
	if !ok {
		return 0, fmt.Errorf("unsupported INSERT input type: %T", ins.Input)
	}

	var rowCount int64
	for _, row := range input.Rows {
		vals := make([]any, len(row.Exprs))
		for i, expr := range row.Exprs {
			v, err := evalLiteral(expr.Expr)
			if err != nil {
				return 0, fmt.Errorf("column %s: %w", cols[i], err)
			}
			vals[i] = v
		}

		var applyErr error
		if ins.InsertOrType == ast.InsertOrTypeUpdate {
			applyErr = db.ReplaceRow(table, cols, vals)
		} else {
			applyErr = db.InsertRow(table, cols, vals)
		}
		if applyErr != nil {
			return 0, applyErr
		}
		rowCount++
	}
	return rowCount, nil
}

// evalLiteral converts a memefish literal expression to a Go value.
func evalLiteral(expr ast.Expr) (any, error) {
	switch e := expr.(type) {
	case *ast.IntLiteral:
		v, err := strconv.ParseInt(e.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse int %q: %w", e.Value, err)
		}
		return v, nil
	case *ast.StringLiteral:
		return e.Value, nil
	case *ast.NullLiteral:
		return nil, nil
	case *ast.BoolLiteral:
		return e.Value, nil
	case *ast.FloatLiteral:
		v, err := strconv.ParseFloat(e.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("parse float %q: %w", e.Value, err)
		}
		return v, nil
	case *ast.UnaryExpr:
		inner, err := evalLiteral(e.Expr)
		if err != nil {
			return nil, err
		}
		if e.Op == ast.OpMinus {
			switch v := inner.(type) {
			case int64:
				return -v, nil
			case float64:
				return -v, nil
			}
		}
		return nil, fmt.Errorf("unsupported unary op %q on %T", e.Op, inner)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}
