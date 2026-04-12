package engine

import (
	"fmt"
	"time"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/uji/go-spanner-server/store"
)

// ExecuteDML executes an INSERT, UPDATE, or DELETE statement and returns the number of affected rows.
// commitTS is the commit timestamp for the enclosing transaction; it is used by PENDING_COMMIT_TIMESTAMP().
// Pass time.Now() for auto-commit DML executed outside an explicit transaction.
func ExecuteDML(db *store.Database, sql string, commitTS time.Time) (int64, error) {
	dml, err := memefish.ParseDML("", sql)
	if err != nil {
		return 0, fmt.Errorf("parse DML: %w", err)
	}
	return executeDML(db, dml, commitTS)
}

func executeDML(db *store.Database, dml ast.DML, commitTS time.Time) (int64, error) {
	switch d := dml.(type) {
	case *ast.Insert:
		return executeInsert(db, d, commitTS)
	case *ast.Update:
		return executeUpdate(db, d, commitTS)
	case *ast.Delete:
		return executeDelete(db, d)
	default:
		return 0, fmt.Errorf("unsupported DML type: %T", dml)
	}
}

func executeInsert(db *store.Database, ins *ast.Insert, commitTS time.Time) (int64, error) {
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

	// Use a context with no current row (INSERT has no source row).
	ctx := &evalContext{
		colIndex: table.ColIndex,
		cols:     table.Cols,
		commitTS: commitTS,
	}

	var rowCount int64
	for _, row := range input.Rows {
		vals := make([]any, len(row.Exprs))
		for i, expr := range row.Exprs {
			v, err := evalExpr(ctx, expr.Expr)
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

func executeUpdate(db *store.Database, upd *ast.Update, commitTS time.Time) (int64, error) {
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

	ctx := &evalContext{colIndex: table.ColIndex, cols: table.Cols, commitTS: commitTS}
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

// executeDelete does not accept a commitTS because PENDING_COMMIT_TIMESTAMP() is not valid
// in DELETE statements — DELETE does not write column values.
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

