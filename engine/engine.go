package engine

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

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

	return result, nil
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

		switch e := expr.(type) {
		case *ast.IntLiteral:
			result.Columns = append(result.Columns, &sppb.StructType_Field{
				Name: alias,
				Type: &sppb.Type{Code: sppb.TypeCode_INT64},
			})
			rowVals = append(rowVals, &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: e.Value},
			})
		case *ast.StringLiteral:
			result.Columns = append(result.Columns, &sppb.StructType_Field{
				Name: alias,
				Type: &sppb.Type{Code: sppb.TypeCode_STRING},
			})
			rowVals = append(rowVals, &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: e.Value},
			})
		default:
			return nil, fmt.Errorf("unsupported literal expression: %T", e)
		}
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

	// Resolve column names from SELECT list
	var colNames []string
	var selectAll bool
	for _, item := range sel.Results {
		switch it := item.(type) {
		case *ast.Star:
			selectAll = true
		case *ast.Alias:
			ident, ok := it.Expr.(*ast.Ident)
			if !ok {
				return nil, fmt.Errorf("unsupported select expression: %T", it.Expr)
			}
			colNames = append(colNames, ident.Name)
		case *ast.ExprSelectItem:
			ident, ok := it.Expr.(*ast.Ident)
			if !ok {
				return nil, fmt.Errorf("unsupported select expression: %T", it.Expr)
			}
			colNames = append(colNames, ident.Name)
		default:
			return nil, fmt.Errorf("unsupported select item: %T", it)
		}
	}

	if selectAll {
		colNames = make([]string, len(table.Cols))
		for i, c := range table.Cols {
			colNames[i] = c.Name
		}
	}

	colIndexes, err := table.ResolveColumnIndexes(colNames)
	if err != nil {
		return nil, err
	}

	// Build result metadata
	result := &Result{}
	for i, idx := range colIndexes {
		col := table.Cols[idx]
		result.Columns = append(result.Columns, &sppb.StructType_Field{
			Name: colNames[i],
			Type: store.SpannerType(col.Type),
		})
	}

	if sel.Where != nil {
		// Read all columns, filter by WHERE, then project
		allIndexes := make([]int, len(table.Cols))
		for i := range allIndexes {
			allIndexes[i] = i
		}
		allRows := table.ReadAll(allIndexes)

		ctx := &evalContext{colIndex: table.ColIndex, cols: table.Cols}
		for _, row := range allRows {
			ctx.row = row
			match, err := evalWhere(ctx, sel.Where.Expr)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
			vals := make([]*structpb.Value, len(colIndexes))
			for i, idx := range colIndexes {
				vals[i] = store.EncodeValue(row.Data[idx], table.Cols[idx].Type)
			}
			result.Rows = append(result.Rows, &structpb.ListValue{Values: vals})
		}
	} else {
		rows := table.ReadAll(colIndexes)
		for _, row := range rows {
			vals := make([]*structpb.Value, len(colIndexes))
			for i, idx := range colIndexes {
				vals[i] = store.EncodeValue(row.Data[i], table.Cols[idx].Type)
			}
			result.Rows = append(result.Rows, &structpb.ListValue{Values: vals})
		}
	}

	return result, nil
}
