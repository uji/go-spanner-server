package testutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

// Section is a parsed txtar section.
type Section struct {
	Name    string
	Content string
}

// ParseSections splits txtar-formatted text into sections.
// Lines matching "-- name --" are treated as section delimiters; leading comment lines are ignored.
func ParseSections(data string) []Section {
	var sections []Section
	var currentName string
	var contentLines []string
	inSection := false

	for _, line := range strings.Split(data, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "-- ") && strings.HasSuffix(line, " --") {
			if inSection {
				sections = append(sections, Section{
					Name:    currentName,
					Content: strings.TrimSpace(strings.Join(contentLines, "\n")),
				})
			}
			currentName = strings.TrimSpace(line[3 : len(line)-3])
			contentLines = nil
			inSection = true
		} else if inSection {
			contentLines = append(contentLines, line)
		}
	}
	if inSection {
		sections = append(sections, Section{
			Name:    currentName,
			Content: strings.TrimSpace(strings.Join(contentLines, "\n")),
		})
	}
	return sections
}

// SplitStatements splits a semicolon-delimited SQL text into individual statements.
func SplitStatements(s string) []string {
	var stmts []string
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part != "" {
			stmts = append(stmts, part)
		}
	}
	return stmts
}

// ExtractDDL returns all DDL statements from sections named "ddl.sql".
func ExtractDDL(sections []Section) []string {
	var ddl []string
	for _, sec := range sections {
		if sec.Name == "ddl.sql" {
			ddl = append(ddl, SplitStatements(sec.Content)...)
		}
	}
	return ddl
}

// SQLToMutation converts an INSERT statement into a spanner.Mutation.
// INSERT OR UPDATE becomes an InsertOrUpdate mutation.
func SQLToMutation(sql string) (*spanner.Mutation, error) {
	dml, err := memefish.ParseDML("", sql)
	if err != nil {
		return nil, fmt.Errorf("parse DML: %w", err)
	}
	ins, ok := dml.(*ast.Insert)
	if !ok {
		return nil, fmt.Errorf("only INSERT is supported in exec.sql section, got %T", dml)
	}

	tableName := ins.TableName.Idents[0].Name
	cols := make([]string, len(ins.Columns))
	for i, col := range ins.Columns {
		cols[i] = col.Name
	}

	input, ok := ins.Input.(*ast.ValuesInput)
	if !ok {
		return nil, fmt.Errorf("only VALUES input is supported in exec.sql section")
	}
	if len(input.Rows) != 1 {
		return nil, fmt.Errorf("each INSERT in exec.sql section must have exactly one VALUES row; got %d", len(input.Rows))
	}

	vals := make([]any, len(input.Rows[0].Exprs))
	for i, expr := range input.Rows[0].Exprs {
		v, err := evalExprLiteral(expr.Expr)
		if err != nil {
			return nil, fmt.Errorf("col %s: %w", cols[i], err)
		}
		vals[i] = v
	}

	if ins.InsertOrType == ast.InsertOrTypeUpdate {
		return spanner.InsertOrUpdate(tableName, cols, vals), nil
	}
	return spanner.Insert(tableName, cols, vals), nil
}

func evalExprLiteral(expr ast.Expr) (any, error) {
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
		inner, err := evalExprLiteral(e.Expr)
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

// FormatRow formats a spanner.Row as a "(val1, val2, ...)" string.
func FormatRow(row *spanner.Row) string {
	vals := make([]string, row.Size())
	for i := range vals {
		var gcv spanner.GenericColumnValue
		if err := row.Column(i, &gcv); err != nil {
			vals[i] = fmt.Sprintf("<error:%v>", err)
			continue
		}
		vals[i] = formatValue(gcv)
	}
	return "(" + strings.Join(vals, ", ") + ")"
}

func formatValue(gcv spanner.GenericColumnValue) string {
	if gcv.Value == nil {
		return "NULL"
	}
	if _, ok := gcv.Value.Kind.(*structpb.Value_NullValue); ok {
		return "NULL"
	}
	switch gcv.Type.Code {
	case sppb.TypeCode_INT64:
		var v int64
		if err := gcv.Decode(&v); err != nil {
			return fmt.Sprintf("<error:%v>", err)
		}
		return strconv.FormatInt(v, 10)
	case sppb.TypeCode_STRING:
		var v string
		if err := gcv.Decode(&v); err != nil {
			return fmt.Sprintf("<error:%v>", err)
		}
		return `"` + v + `"`
	case sppb.TypeCode_FLOAT64:
		var v float64
		if err := gcv.Decode(&v); err != nil {
			return fmt.Sprintf("<error:%v>", err)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case sppb.TypeCode_BOOL:
		var v bool
		if err := gcv.Decode(&v); err != nil {
			return fmt.Sprintf("<error:%v>", err)
		}
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("<unsupported_type:%v>", gcv.Type.Code)
	}
}

// ParseExpectLines converts the content of an expect.out section into a list of lines.
func ParseExpectLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// RunSections executes parsed sections in order.
// The ddl.sql section is already handled by the caller; this function processes exec.sql/query.sql/expect.out.
func RunSections(ctx context.Context, t *testing.T, client *spanner.Client, sections []Section) {
	t.Helper()

	var pendingQuery string
	var pendingRows []string

	for _, sec := range sections {
		switch sec.Name {
		case "ddl.sql":
			// Passed as a DDL argument to the caller; no processing here.
		case "exec.sql":
			// Convert INSERT statements to Mutations and apply via client.Apply().
			stmts := SplitStatements(sec.Content)
			mutations := make([]*spanner.Mutation, 0, len(stmts))
			for _, stmt := range stmts {
				m, err := SQLToMutation(stmt)
				if err != nil {
					t.Fatalf("exec.sql: %v", err)
				}
				mutations = append(mutations, m)
			}
			if _, err := client.Apply(ctx, mutations); err != nil {
				t.Fatalf("exec.sql apply failed: %v", err)
			}
		case "query.sql":
			// Error if the previous query's expect has not been verified.
			if pendingRows != nil {
				t.Fatalf("query.sql section without following expect.out section (pending query: %q)", pendingQuery)
			}
			pendingQuery = strings.TrimSpace(sec.Content)
			iter := client.Single().Query(ctx, spanner.NewStatement(pendingQuery))
			var rows []string
			doErr := iter.Do(func(row *spanner.Row) error {
				rows = append(rows, FormatRow(row))
				return nil
			})
			iter.Stop()
			if doErr != nil {
				t.Fatalf("query.sql failed: %v\nSQL: %s", doErr, pendingQuery)
			}
			pendingRows = rows
			if pendingRows == nil {
				pendingRows = []string{} // distinguish empty result from not-yet-run
			}
		case "expect.out":
			if pendingRows == nil {
				t.Fatalf("expect.out section without preceding query.sql section")
			}
			want := ParseExpectLines(sec.Content)
			if len(want) == 0 {
				want = []string{}
			}
			if len(pendingRows) != len(want) {
				t.Errorf("query %q:\nrow count: got %d, want %d\ngot:\n%s\nwant:\n%s",
					pendingQuery,
					len(pendingRows), len(want),
					strings.Join(pendingRows, "\n"),
					strings.Join(want, "\n"),
				)
			} else {
				for i := range want {
					if pendingRows[i] != want[i] {
						t.Errorf("query %q row[%d]:\n got:  %s\n want: %s", pendingQuery, i, pendingRows[i], want[i])
					}
				}
			}
			pendingQuery = ""
			pendingRows = nil
		default:
			t.Fatalf("unknown section %q", sec.Name)
		}
	}

	if pendingRows != nil {
		t.Fatalf("query.sql section %q has no following expect.out section", pendingQuery)
	}
}
