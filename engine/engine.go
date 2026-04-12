package engine

import (
	"cmp"
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/uji/go-spanner-server/store"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Result holds query execution results.
type Result struct {
	Columns []*sppb.StructType_Field
	Rows    []*structpb.ListValue
}

// Execute runs a SQL query against the database.
func Execute(db *store.Database, sql string) (*Result, error) {
	stmt, err := memefish.ParseQuery("", sql)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	return executeQuery(db, stmt)
}

func executeQuery(db *store.Database, qs *ast.QueryStatement) (*Result, error) {
	var sel *ast.Select
	var orderBy *ast.OrderBy
	var limit *ast.Limit

	switch q := qs.Query.(type) {
	case *ast.Select:
		sel = q
	case *ast.Query:
		var ok bool
		sel, ok = q.Query.(*ast.Select)
		if !ok {
			return nil, fmt.Errorf("unsupported query type: %T", q.Query)
		}
		orderBy = q.OrderBy
		limit = q.Limit
	default:
		return nil, fmt.Errorf("unsupported query type: %T", qs.Query)
	}

	// Handle SELECT without FROM (e.g., SELECT 1)
	if sel.From == nil {
		return executeSelectLiteral(sel)
	}

	result, err := executeSelectFrom(db, sel)
	if err != nil {
		return nil, err
	}

	if orderBy != nil {
		if err := applyOrderBy(result, orderBy); err != nil {
			return nil, err
		}
	}

	if limit != nil {
		if err := applyLimit(result, limit); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// applyLimit applies LIMIT [OFFSET] to result rows.
func applyLimit(result *Result, limit *ast.Limit) error {
	count, err := evalIntValue(limit.Count)
	if err != nil {
		return fmt.Errorf("LIMIT: %w", err)
	}

	offset := int64(0)
	if limit.Offset != nil {
		offset, err = evalIntValue(limit.Offset.Value)
		if err != nil {
			return fmt.Errorf("OFFSET: %w", err)
		}
	}

	total := int64(len(result.Rows))
	if offset >= total {
		result.Rows = nil
		return nil
	}
	end := offset + count
	if end > total {
		end = total
	}
	result.Rows = result.Rows[offset:end]
	return nil
}

// evalIntValue evaluates an IntValue AST node (e.g., a literal integer in LIMIT/OFFSET).
func evalIntValue(iv ast.IntValue) (int64, error) {
	switch v := iv.(type) {
	case *ast.IntLiteral:
		n, err := strconv.ParseInt(v.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid integer literal %q: %w", v.Value, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported IntValue type: %T (only integer literals are supported)", iv)
	}
}

// extractExpr extracts the underlying Expr from a SelectItem.
func extractExpr(item ast.SelectItem) (ast.Expr, string) {
	switch it := item.(type) {
	case *ast.Alias:
		alias := ""
		if it.As != nil {
			alias = it.As.Alias.Name
		}
		return it.Expr, alias
	case *ast.ExprSelectItem:
		return it.Expr, ""
	default:
		return nil, ""
	}
}

func executeSelectLiteral(sel *ast.Select) (*Result, error) {
	// Use a minimal evalContext with no row data for literal/function evaluation.
	ctx := &evalContext{}

	result := &Result{}
	var rowVals []*structpb.Value

	for i, item := range sel.Results {
		alias := fmt.Sprintf("_col%d", i)
		expr, explicitAlias := extractExpr(item)
		if explicitAlias != "" {
			alias = explicitAlias
		}
		if expr == nil {
			return nil, fmt.Errorf("unsupported select item: %T", item)
		}

		val, err := evalExpr(ctx, expr)
		if err != nil {
			return nil, fmt.Errorf("SELECT expression %q: %w", alias, err)
		}

		result.Columns = append(result.Columns, &sppb.StructType_Field{
			Name: alias,
			Type: inferTypeFromValue(val),
		})
		rowVals = append(rowVals, encodeComputedValue(val))
	}

	result.Rows = []*structpb.ListValue{{Values: rowVals}}
	return result, nil
}

// applyOrderBy sorts result rows by the ORDER BY clause.
func applyOrderBy(result *Result, orderBy *ast.OrderBy) error {
	// Resolve column indexes for ORDER BY items
	type orderKey struct {
		colIdx   int
		typeCode sppb.TypeCode
		desc     bool
	}
	var keys []orderKey
	for _, item := range orderBy.Items {
		ident, ok := item.Expr.(*ast.Ident)
		if !ok {
			return fmt.Errorf("unsupported ORDER BY expression: %T", item.Expr)
		}
		colIdx := -1
		for i, col := range result.Columns {
			if col.Name == ident.Name {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return fmt.Errorf("column %q not found in ORDER BY", ident.Name)
		}
		keys = append(keys, orderKey{
			colIdx:   colIdx,
			typeCode: result.Columns[colIdx].Type.Code,
			desc:     item.Dir == ast.DirectionDesc,
		})
	}

	slices.SortFunc(result.Rows, func(a, b *structpb.ListValue) int {
		for _, k := range keys {
			va := a.Values[k.colIdx]
			vb := b.Values[k.colIdx]
			c := compareValues(va, vb, k.typeCode)
			if c != 0 {
				if k.desc {
					return -c
				}
				return c
			}
		}
		return 0
	})
	return nil
}

// compareValues compares two protobuf values for sorting.
// typeCode is used to determine the correct comparison strategy.
func compareValues(a, b *structpb.Value, typeCode sppb.TypeCode) int {
	_, aNil := a.Kind.(*structpb.Value_NullValue)
	_, bNil := b.Kind.(*structpb.Value_NullValue)
	if aNil && bNil {
		return 0
	}
	if aNil {
		return -1
	}
	if bNil {
		return 1
	}

	aStr, aOk := a.Kind.(*structpb.Value_StringValue)
	bStr, bOk := b.Kind.(*structpb.Value_StringValue)
	if aOk && bOk {
		// INT64 is encoded as string in Spanner protobuf; compare numerically.
		if typeCode == sppb.TypeCode_INT64 {
			aInt, _ := strconv.ParseInt(aStr.StringValue, 10, 64)
			bInt, _ := strconv.ParseInt(bStr.StringValue, 10, 64)
			return cmp.Compare(aInt, bInt)
		}
		return cmp.Compare(aStr.StringValue, bStr.StringValue)
	}

	aNum, aOk := a.Kind.(*structpb.Value_NumberValue)
	bNum, bOk := b.Kind.(*structpb.Value_NumberValue)
	if aOk && bOk {
		return cmp.Compare(aNum.NumberValue, bNum.NumberValue)
	}
	return 0
}

// selectItem describes a single projected column in a SELECT list.
type selectItem struct {
	name  string   // output column name (alias or derived from expression)
	expr  ast.Expr // the expression to evaluate for each row (nil for *)
	isAll bool     // true when this item is SELECT *
}

// buildSelectItems resolves SELECT list items into selectItem descriptors.
func buildSelectItems(table *store.Table, items []ast.SelectItem) ([]selectItem, error) {
	var result []selectItem
	exprCount := 0
	for _, item := range items {
		switch it := item.(type) {
		case *ast.Star:
			result = append(result, selectItem{isAll: true})
		case *ast.Alias:
			alias := ""
			if it.As != nil {
				alias = it.As.Alias.Name
			}
			if alias == "" {
				if ident, ok := it.Expr.(*ast.Ident); ok {
					alias = ident.Name
				} else {
					alias = fmt.Sprintf("_expr%d", exprCount)
					exprCount++
				}
			}
			result = append(result, selectItem{name: alias, expr: it.Expr})
		case *ast.ExprSelectItem:
			name := ""
			if ident, ok := it.Expr.(*ast.Ident); ok {
				name = ident.Name
			} else {
				name = fmt.Sprintf("_expr%d", exprCount)
				exprCount++
			}
			result = append(result, selectItem{name: name, expr: it.Expr})
		default:
			return nil, fmt.Errorf("unsupported select item: %T", it)
		}
	}
	// Expand SELECT * into per-column items.
	var expanded []selectItem
	for _, si := range result {
		if si.isAll {
			for _, col := range table.Cols {
				c := col
				expanded = append(expanded, selectItem{
					name: c.Name,
					expr: &ast.Ident{Name: c.Name},
				})
			}
		} else {
			expanded = append(expanded, si)
		}
	}
	return expanded, nil
}

// inferSpannerType infers the Spanner protobuf type from a Go value.
func inferSpannerType(val any, table *store.Table, expr ast.Expr) *sppb.Type {
	// First try to derive from column type if expression is a simple identifier.
	if ident, ok := expr.(*ast.Ident); ok {
		if idx, ok := table.ColIndex[ident.Name]; ok {
			return store.SpannerType(table.Cols[idx].Type)
		}
	}
	// Fall back to the runtime type of the value.
	return inferTypeFromValue(val)
}

// inferTypeFromValue infers the Spanner type from the Go runtime type of a value.
func inferTypeFromValue(val any) *sppb.Type {
	switch val.(type) {
	case int64:
		return &sppb.Type{Code: sppb.TypeCode_INT64}
	case float64:
		return &sppb.Type{Code: sppb.TypeCode_FLOAT64}
	case bool:
		return &sppb.Type{Code: sppb.TypeCode_BOOL}
	case []byte:
		return &sppb.Type{Code: sppb.TypeCode_BYTES}
	case time.Time:
		return &sppb.Type{Code: sppb.TypeCode_TIMESTAMP}
	default:
		return &sppb.Type{Code: sppb.TypeCode_STRING}
	}
}

func executeSelectFrom(db *store.Database, sel *ast.Select) (*Result, error) {
	// Extract table name from FROM clause
	from, ok := sel.From.Source.(*ast.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported FROM source: %T", sel.From.Source)
	}
	tableName := from.Table.Name

	table, err := db.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	selectItems, err := buildSelectItems(table, sel.Results)
	if err != nil {
		return nil, err
	}

	// Always read all columns so that expressions referencing any column can be evaluated.
	allIndexes := make([]int, len(table.Cols))
	for i := range allIndexes {
		allIndexes[i] = i
	}

	var rows []store.Row
	evalCtx := &evalContext{colIndex: table.ColIndex, cols: table.Cols}

	if sel.Where != nil {
		allRows := table.ReadAll(allIndexes)
		for _, row := range allRows {
			evalCtx.row = row
			match, err := evalWhere(evalCtx, sel.Where.Expr)
			if err != nil {
				return nil, err
			}
			if match {
				rows = append(rows, row)
			}
		}
	} else {
		rows = table.ReadAll(allIndexes)
	}

	// Determine column metadata from the first row (or infer from expression type).
	// We must build metadata before encoding rows.
	result := &Result{}

	// Build columns metadata using the first row for type inference when needed.
	columnsBuilt := false
	if len(rows) == 0 {
		// No rows: infer column types from identifiers or use STRING as fallback.
		for _, si := range selectItems {
			colType := inferSpannerType(nil, table, si.expr)
			// For identifier expressions with no rows, resolve from schema.
			result.Columns = append(result.Columns, &sppb.StructType_Field{
				Name: si.name,
				Type: colType,
			})
		}
		columnsBuilt = true
	}

	for _, row := range rows {
		evalCtx.row = row
		vals := make([]*structpb.Value, len(selectItems))

		for i, si := range selectItems {
			val, err := evalExpr(evalCtx, si.expr)
			if err != nil {
				return nil, fmt.Errorf("SELECT expression %q: %w", si.name, err)
			}

			if !columnsBuilt {
				colType := inferSpannerType(val, table, si.expr)
				result.Columns = append(result.Columns, &sppb.StructType_Field{
					Name: si.name,
					Type: colType,
				})
			}

			// Encode the value. For identifier expressions, use the column's own type.
			if ident, ok := si.expr.(*ast.Ident); ok {
				if idx, ok := table.ColIndex[ident.Name]; ok {
					vals[i] = store.EncodeValue(val, table.Cols[idx].Type)
					continue
				}
			}
			// For computed expressions, infer type from value.
			vals[i] = encodeComputedValue(val)
		}
		columnsBuilt = true
		result.Rows = append(result.Rows, &structpb.ListValue{Values: vals})
	}

	return result, nil
}

// encodeComputedValue encodes a Go value returned by an expression into a protobuf Value.
func encodeComputedValue(val any) *structpb.Value {
	if val == nil {
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}
	switch v := val.(type) {
	case int64:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: strconv.FormatInt(v, 10)}}
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: v}}
	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: v}}
	case string:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v}}
	case []byte:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(v)}}
	case time.Time:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v.UTC().Format(time.RFC3339Nano)}}
	default:
		// Use string representation as fallback.
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}
