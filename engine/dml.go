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
	case *ast.Update:
		return executeUpdate(db, d)
	case *ast.Delete:
		return executeDelete(db, d)
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

func executeUpdate(db *store.Database, upd *ast.Update) (int64, error) {
	tableName := upd.TableName.Idents[0].Name
	table, err := db.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Validate SET columns and prevent primary key updates.
	setCols := make([]string, len(upd.Updates))
	for i, item := range upd.Updates {
		col := item.Path[0].Name
		for _, pkIdx := range table.PKCols {
			if table.Cols[pkIdx].Name == col {
				return 0, fmt.Errorf("cannot update primary key column %q", col)
			}
		}
		setCols[i] = col
	}

	// Read all rows (full scan).
	allIndexes := make([]int, len(table.Cols))
	for i := range allIndexes {
		allIndexes[i] = i
	}
	allRows := table.ReadAll(allIndexes)

	ctx := &evalContext{colIndex: table.ColIndex, cols: table.Cols}
	var rowCount int64
	for _, row := range allRows {
		ctx.row = row

		// Apply WHERE filter if present; nil WHERE matches all rows.
		if upd.Where != nil {
			match, err := evalWhere(ctx, upd.Where.Expr)
			if err != nil {
				return 0, err
			}
			if !match {
				continue
			}
		}

		// Evaluate each SET expression against the original row values.
		setVals := make([]any, len(setCols))
		for i, item := range upd.Updates {
			if item.DefaultExpr.Default {
				setVals[i] = nil
			} else {
				v, err := evalExpr(ctx, item.DefaultExpr.Expr)
				if err != nil {
					return 0, fmt.Errorf("SET %s: %w", setCols[i], err)
				}
				setVals[i] = v
			}
		}

		// Build cols/vals with PK columns prepended so db.UpdateRow can locate the row.
		pkColNames := make([]string, len(table.PKCols))
		pkVals := make([]any, len(table.PKCols))
		for i, pkIdx := range table.PKCols {
			pkColNames[i] = table.Cols[pkIdx].Name
			pkVals[i] = row.Data[pkIdx]
		}
		allCols := append(pkColNames, setCols...)
		allVals := append(pkVals, setVals...)

		if err := db.UpdateRow(table, allCols, allVals); err != nil {
			return 0, err
		}
		rowCount++
	}
	return rowCount, nil
}

func executeDelete(db *store.Database, del *ast.Delete) (int64, error) {
	if del.Where == nil {
		return 0, fmt.Errorf("DELETE requires a WHERE clause; use WHERE true to delete all rows")
	}

	tableName := del.TableName.Idents[0].Name
	table, err := db.GetTable(tableName)
	if err != nil {
		return 0, err
	}

	// Read all rows (full scan).
	allIndexes := make([]int, len(table.Cols))
	for i := range allIndexes {
		allIndexes[i] = i
	}
	allRows := table.ReadAll(allIndexes)

	ctx := &evalContext{colIndex: table.ColIndex, cols: table.Cols}
	var keys [][]any
	for _, row := range allRows {
		ctx.row = row
		match, err := evalWhere(ctx, del.Where.Expr)
		if err != nil {
			return 0, err
		}
		if !match {
			continue
		}
		key := make([]any, len(table.PKCols))
		for i, pkIdx := range table.PKCols {
			key[i] = row.Data[pkIdx]
		}
		keys = append(keys, key)
	}

	if err := db.DeleteByKeys(table, keys); err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
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
