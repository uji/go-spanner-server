package testutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
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
// The ddl.sql section is already handled by the caller; this function processes dml.sql/query.sql/expect.out.
func RunSections(ctx context.Context, t *testing.T, client *spanner.Client, sections []Section) {
	t.Helper()

	var pendingQuery string
	var pendingRows []string

	for _, sec := range sections {
		switch sec.Name {
		case "ddl.sql":
			// Passed as a DDL argument to the caller; no processing here.
		case "dml.sql":
			// Execute DML statements in a read-write transaction.
			stmts := SplitStatements(sec.Content)
			_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				for _, stmt := range stmts {
					if _, err := tx.Update(ctx, spanner.NewStatement(stmt)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("dml.sql failed: %v", err)
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
