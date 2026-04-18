package engine

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/uji/go-spanner-server/store"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

var aggregateFuncNames = map[string]bool{
	"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	"COUNT_IF": true, "ANY_VALUE": true,
}

// isAggregateFunc reports whether name is an aggregate function.
func isAggregateFunc(name string) bool {
	return aggregateFuncNames[strings.ToUpper(name)]
}

// containsAggregate reports whether the expression tree contains any aggregate call.
func containsAggregate(expr ast.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *ast.CountStarExpr:
		return true
	case *ast.CallExpr:
		name := strings.ToUpper(e.Func.Idents[len(e.Func.Idents)-1].Name)
		if isAggregateFunc(name) {
			return true
		}
		for _, arg := range e.Args {
			if ea, ok := arg.(*ast.ExprArg); ok && containsAggregate(ea.Expr) {
				return true
			}
		}
	case *ast.BinaryExpr:
		return containsAggregate(e.Left) || containsAggregate(e.Right)
	case *ast.UnaryExpr:
		return containsAggregate(e.Expr)
	case *ast.CastExpr:
		return containsAggregate(e.Expr)
	case *ast.ParenExpr:
		return containsAggregate(e.Expr)
	case *ast.CaseExpr:
		if e.Expr != nil && containsAggregate(e.Expr) {
			return true
		}
		for _, w := range e.Whens {
			if containsAggregate(w.Cond) || containsAggregate(w.Then) {
				return true
			}
		}
		if e.Else != nil {
			return containsAggregate(e.Else.Expr)
		}
	case *ast.IfExpr:
		return containsAggregate(e.Expr) || containsAggregate(e.TrueResult) || containsAggregate(e.ElseResult)
	}
	return false
}

// hasAggregatesInResults returns true if any SELECT item contains an aggregate function.
func hasAggregatesInResults(items []ast.SelectItem) bool {
	for _, item := range items {
		var expr ast.Expr
		switch it := item.(type) {
		case *ast.Alias:
			expr = it.Expr
		case *ast.ExprSelectItem:
			expr = it.Expr
		}
		if expr != nil && containsAggregate(expr) {
			return true
		}
	}
	return false
}

// preComputeAggs walks expr and evaluates all aggregate calls over rows,
// storing results in aggValues keyed by the ast.Expr pointer.
func preComputeAggs(rows []store.Row, baseCtx *evalContext, expr ast.Expr, aggValues map[ast.Expr]any) error {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *ast.CountStarExpr:
		aggValues[e] = int64(len(rows))
	case *ast.CallExpr:
		if len(e.Func.Idents) == 0 {
			return fmt.Errorf("function call has no name")
		}
		name := strings.ToUpper(e.Func.Idents[len(e.Func.Idents)-1].Name)
		if isAggregateFunc(name) {
			val, err := evalAggCallExpr(rows, baseCtx, name, e)
			if err != nil {
				return err
			}
			aggValues[e] = val
			return nil // don't recurse into aggregate arguments
		}
		for _, arg := range e.Args {
			if ea, ok := arg.(*ast.ExprArg); ok {
				if err := preComputeAggs(rows, baseCtx, ea.Expr, aggValues); err != nil {
					return err
				}
			}
		}
	case *ast.BinaryExpr:
		if err := preComputeAggs(rows, baseCtx, e.Left, aggValues); err != nil {
			return err
		}
		return preComputeAggs(rows, baseCtx, e.Right, aggValues)
	case *ast.UnaryExpr:
		return preComputeAggs(rows, baseCtx, e.Expr, aggValues)
	case *ast.CastExpr:
		return preComputeAggs(rows, baseCtx, e.Expr, aggValues)
	case *ast.ParenExpr:
		return preComputeAggs(rows, baseCtx, e.Expr, aggValues)
	case *ast.CaseExpr:
		if e.Expr != nil {
			if err := preComputeAggs(rows, baseCtx, e.Expr, aggValues); err != nil {
				return err
			}
		}
		for _, w := range e.Whens {
			if err := preComputeAggs(rows, baseCtx, w.Cond, aggValues); err != nil {
				return err
			}
			if err := preComputeAggs(rows, baseCtx, w.Then, aggValues); err != nil {
				return err
			}
		}
		if e.Else != nil {
			return preComputeAggs(rows, baseCtx, e.Else.Expr, aggValues)
		}
	case *ast.IfExpr:
		if err := preComputeAggs(rows, baseCtx, e.Expr, aggValues); err != nil {
			return err
		}
		if err := preComputeAggs(rows, baseCtx, e.TrueResult, aggValues); err != nil {
			return err
		}
		return preComputeAggs(rows, baseCtx, e.ElseResult, aggValues)
	}
	return nil
}

// groupKeyString builds a unique, collision-resistant string key from a slice of group-by values.
// Each value is encoded with a type prefix character so that int64(1) and string("1") produce
// different keys, and length-prefixed strings prevent ambiguity between adjacent columns.
func groupKeyString(vals []any) string {
	var sb strings.Builder
	for _, v := range vals {
		if v == nil {
			sb.WriteString("N\x00")
			continue
		}
		switch t := v.(type) {
		case int64:
			sb.WriteString(fmt.Sprintf("I%d\x00", t))
		case float64:
			sb.WriteString(fmt.Sprintf("F%v\x00", t))
		case bool:
			sb.WriteString(fmt.Sprintf("B%v\x00", t))
		case string:
			sb.WriteString(fmt.Sprintf("S%d:%s\x00", len(t), t))
		default:
			sb.WriteString(fmt.Sprintf("?%v\x00", t))
		}
	}
	return sb.String()
}

// applyGroupByAndAggregates handles queries with GROUP BY, HAVING, and/or aggregate functions.
func applyGroupByAndAggregates(
	table *store.Table,
	rows []store.Row,
	sel *ast.Select,
	selectItems []selectItem,
) (*Result, error) {
	baseCtx := &evalContext{colIndex: table.ColIndex, cols: table.Cols}

	type group struct {
		rows []store.Row
	}

	var groups []group
	groupIndex := map[string]int{}

	if sel.GroupBy != nil {
		// Partition rows by group key.
		for _, row := range rows {
			baseCtx.row = row
			keyVals := make([]any, len(sel.GroupBy.Exprs))
			for i, expr := range sel.GroupBy.Exprs {
				v, err := evalExpr(baseCtx, expr)
				if err != nil {
					return nil, fmt.Errorf("GROUP BY: %w", err)
				}
				keyVals[i] = v
			}
			keyStr := groupKeyString(keyVals)
			if idx, ok := groupIndex[keyStr]; ok {
				groups[idx].rows = append(groups[idx].rows, row)
			} else {
				groupIndex[keyStr] = len(groups)
				groups = append(groups, group{rows: []store.Row{row}})
			}
		}
	} else {
		// No GROUP BY: treat all rows as a single group.
		groups = []group{{rows: rows}}
	}

	result := &Result{}
	columnsBuilt := false

	for _, g := range groups {
		// Pre-compute all aggregate calls appearing in SELECT items and HAVING.
		aggValues := make(map[ast.Expr]any)
		for _, si := range selectItems {
			if err := preComputeAggs(g.rows, baseCtx, si.expr, aggValues); err != nil {
				return nil, err
			}
		}
		if sel.Having != nil {
			if err := preComputeAggs(g.rows, baseCtx, sel.Having.Expr, aggValues); err != nil {
				return nil, fmt.Errorf("HAVING: %w", err)
			}
		}

		// Build a group eval context: first row provides non-aggregate column values.
		groupCtx := &evalContext{
			colIndex:  table.ColIndex,
			cols:      table.Cols,
			aggValues: aggValues,
		}
		if len(g.rows) > 0 {
			groupCtx.row = g.rows[0]
		}

		// Apply HAVING filter.
		if sel.Having != nil {
			match, err := evalWhere(groupCtx, sel.Having.Expr)
			if err != nil {
				return nil, fmt.Errorf("HAVING: %w", err)
			}
			if !match {
				continue
			}
		}

		// Project SELECT items for this group.
		vals := make([]*structpb.Value, len(selectItems))
		for i, si := range selectItems {
			val, err := evalExpr(groupCtx, si.expr)
			if err != nil {
				return nil, fmt.Errorf("SELECT expression %q: %w", si.name, err)
			}
			if !columnsBuilt {
				result.Columns = append(result.Columns, &sppb.StructType_Field{
					Name: si.name,
					Type: inferTypeFromValue(val),
				})
			}
			vals[i] = encodeComputedValue(val)
		}
		columnsBuilt = true
		result.Rows = append(result.Rows, &structpb.ListValue{Values: vals})
	}

	// Build column metadata when no group survived (HAVING filtered all, or no input rows).
	// For aggregate-only queries with no GROUP BY, still return one row with default values.
	if !columnsBuilt {
		aggValues := make(map[ast.Expr]any)
		for _, si := range selectItems {
			if err := preComputeAggs(nil, baseCtx, si.expr, aggValues); err != nil {
				return nil, err
			}
		}
		emptyCtx := &evalContext{
			colIndex:  table.ColIndex,
			cols:      table.Cols,
			aggValues: aggValues,
		}
		for _, si := range selectItems {
			val, err := evalExpr(emptyCtx, si.expr)
			if err != nil {
				return nil, err
			}
			result.Columns = append(result.Columns, &sppb.StructType_Field{
				Name: si.name,
				Type: inferTypeFromValue(val),
			})
		}
		// A plain aggregate with no GROUP BY and no rows returns one result row.
		if sel.GroupBy == nil {
			vals := make([]*structpb.Value, len(selectItems))
			for i, si := range selectItems {
				val, err := evalExpr(emptyCtx, si.expr)
				if err != nil {
					return nil, err
				}
				vals[i] = encodeComputedValue(val)
			}
			result.Rows = []*structpb.ListValue{{Values: vals}}
		}
	}

	return result, nil
}

// evalAggCallExpr evaluates an aggregate function call over a set of rows.
func evalAggCallExpr(rows []store.Row, baseCtx *evalContext, name string, e *ast.CallExpr) (any, error) {
	switch name {
	case "COUNT":
		return evalCountAgg(rows, baseCtx, e)
	case "SUM":
		return evalSumAgg(rows, baseCtx, e)
	case "AVG":
		return evalAvgAgg(rows, baseCtx, e)
	case "MIN":
		return evalMinMaxAgg(rows, baseCtx, e, false)
	case "MAX":
		return evalMinMaxAgg(rows, baseCtx, e, true)
	case "COUNT_IF":
		return evalCountIfAgg(rows, baseCtx, e)
	case "ANY_VALUE":
		return evalAnyValueAgg(rows, baseCtx, e)
	default:
		return nil, fmt.Errorf("unsupported aggregate function: %s", name)
	}
}

func evalCountAgg(rows []store.Row, baseCtx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("COUNT requires exactly 1 argument")
	}
	if e.Distinct {
		return nil, fmt.Errorf("COUNT(DISTINCT ...) is not yet supported")
	}
	ea, ok := e.Args[0].(*ast.ExprArg)
	if !ok {
		return nil, fmt.Errorf("COUNT: unsupported argument type %T", e.Args[0])
	}
	count := int64(0)
	for _, row := range rows {
		baseCtx.row = row
		val, err := evalExpr(baseCtx, ea.Expr)
		if err != nil {
			return nil, err
		}
		if val != nil {
			count++
		}
	}
	return count, nil
}

func evalSumAgg(rows []store.Row, baseCtx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("SUM requires exactly 1 argument")
	}
	ea, ok := e.Args[0].(*ast.ExprArg)
	if !ok {
		return nil, fmt.Errorf("SUM: unsupported argument type %T", e.Args[0])
	}
	var sumInt int64
	var sumFloat float64
	isFloat := false
	hasValue := false
	for _, row := range rows {
		baseCtx.row = row
		val, err := evalExpr(baseCtx, ea.Expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		hasValue = true
		switch v := val.(type) {
		case int64:
			sumInt += v
		case float64:
			isFloat = true
			sumFloat += v
		default:
			return nil, fmt.Errorf("SUM: non-numeric value %T", val)
		}
	}
	if !hasValue {
		return nil, nil
	}
	if isFloat {
		return sumFloat + float64(sumInt), nil
	}
	return sumInt, nil
}

func evalAvgAgg(rows []store.Row, baseCtx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("AVG requires exactly 1 argument")
	}
	ea, ok := e.Args[0].(*ast.ExprArg)
	if !ok {
		return nil, fmt.Errorf("AVG: unsupported argument type %T", e.Args[0])
	}
	var sum float64
	count := int64(0)
	for _, row := range rows {
		baseCtx.row = row
		val, err := evalExpr(baseCtx, ea.Expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		switch v := val.(type) {
		case int64:
			sum += float64(v)
			count++
		case float64:
			sum += v
			count++
		default:
			return nil, fmt.Errorf("AVG: non-numeric value %T", val)
		}
	}
	if count == 0 {
		return nil, nil
	}
	return sum / float64(count), nil
}

func evalMinMaxAgg(rows []store.Row, baseCtx *evalContext, e *ast.CallExpr, isMax bool) (any, error) {
	funcName := "MIN"
	if isMax {
		funcName = "MAX"
	}
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("%s requires exactly 1 argument", funcName)
	}
	ea, ok := e.Args[0].(*ast.ExprArg)
	if !ok {
		return nil, fmt.Errorf("%s: unsupported argument type %T", funcName, e.Args[0])
	}
	var result any
	for _, row := range rows {
		baseCtx.row = row
		val, err := evalExpr(baseCtx, ea.Expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		if result == nil {
			result = val
			continue
		}
		cmp := store.CompareValues(result, val)
		if (isMax && cmp < 0) || (!isMax && cmp > 0) {
			result = val
		}
	}
	return result, nil
}

func evalCountIfAgg(rows []store.Row, baseCtx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("COUNT_IF requires exactly 1 argument")
	}
	ea, ok := e.Args[0].(*ast.ExprArg)
	if !ok {
		return nil, fmt.Errorf("COUNT_IF: unsupported argument type %T", e.Args[0])
	}
	count := int64(0)
	for _, row := range rows {
		baseCtx.row = row
		val, err := evalExpr(baseCtx, ea.Expr)
		if err != nil {
			return nil, err
		}
		if b, ok := val.(bool); ok && b {
			count++
		}
	}
	return count, nil
}

func evalAnyValueAgg(rows []store.Row, baseCtx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 1 {
		return nil, fmt.Errorf("ANY_VALUE requires exactly 1 argument")
	}
	ea, ok := e.Args[0].(*ast.ExprArg)
	if !ok {
		return nil, fmt.Errorf("ANY_VALUE: unsupported argument type %T", e.Args[0])
	}
	if len(rows) == 0 {
		return nil, nil
	}
	baseCtx.row = rows[0]
	return evalExpr(baseCtx, ea.Expr)
}
