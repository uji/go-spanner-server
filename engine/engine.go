package engine

import (
	"fmt"

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
	sel, ok := qs.Query.(*ast.Select)
	if !ok {
		return nil, fmt.Errorf("unsupported query type: %T", qs.Query)
	}

	// Handle SELECT without FROM (e.g., SELECT 1)
	if sel.From == nil {
		return executeSelectLiteral(sel)
	}

	return executeSelectFrom(db, sel)
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
			Type: &sppb.Type{Code: store.TypeCodeFromDDL(col.Type)},
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
