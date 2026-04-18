package engine

import (
	"fmt"
	"strconv"
	"time"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/uji/go-spanner-server/store"
)

// evalContext holds the row and column information needed to evaluate expressions.
type evalContext struct {
	row      store.Row
	colIndex map[string]int
	cols     []store.ColInfo
	// commitTS is the commit timestamp for the enclosing read-write transaction.
	// A zero value signals that PENDING_COMMIT_TIMESTAMP() is not available (e.g. outside a
	// read-write transaction context). Use inReadWriteTx() to check before accessing commitTS.
	commitTS time.Time
	// aggValues holds pre-computed aggregate function results for GROUP BY evaluation.
	// Keys are pointers to aggregate AST nodes (CountStarExpr or CallExpr);
	// values are the computed results. Nil outside an aggregate context.
	aggValues map[ast.Expr]any
}

// inReadWriteTx reports whether this context is executing inside a read-write transaction
// that has a valid commit timestamp (i.e. PENDING_COMMIT_TIMESTAMP() is allowed).
func (c *evalContext) inReadWriteTx() bool {
	return !c.commitTS.IsZero()
}

// evalExpr evaluates an AST expression and returns the resulting Go value.
func evalExpr(ctx *evalContext, expr ast.Expr) (any, error) {
	switch e := expr.(type) {
	case *ast.Ident:
		idx, ok := ctx.colIndex[e.Name]
		if !ok {
			return nil, fmt.Errorf("column %q not found", e.Name)
		}
		return ctx.row.Data[idx], nil

	case *ast.IntLiteral:
		v, err := strconv.ParseInt(e.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int literal %q: %w", e.Value, err)
		}
		return v, nil

	case *ast.StringLiteral:
		return e.Value, nil

	case *ast.BoolLiteral:
		return e.Value, nil

	case *ast.FloatLiteral:
		v, err := strconv.ParseFloat(e.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float literal %q: %w", e.Value, err)
		}
		return v, nil

	case *ast.NullLiteral:
		return nil, nil

	case *ast.ParenExpr:
		return evalExpr(ctx, e.Expr)

	case *ast.BinaryExpr:
		return evalBinaryExpr(ctx, e)

	case *ast.UnaryExpr:
		return evalUnaryExpr(ctx, e)

	case *ast.IsNullExpr:
		left, err := evalExpr(ctx, e.Left)
		if err != nil {
			return nil, err
		}
		isNull := left == nil
		if e.Not {
			return !isNull, nil
		}
		return isNull, nil

	case *ast.InExpr:
		return evalInExpr(ctx, e)

	case *ast.BetweenExpr:
		return evalBetweenExpr(ctx, e)

	case *ast.CountStarExpr:
		// COUNT(*) is only valid inside an aggregate context.
		if ctx.aggValues != nil {
			if v, ok := ctx.aggValues[e]; ok {
				return v, nil
			}
		}
		return nil, fmt.Errorf("COUNT(*) is only valid in an aggregate context")

	case *ast.CallExpr:
		return evalCallExpr(ctx, e)

	case *ast.CastExpr:
		return evalCastExpr(ctx, e)

	case *ast.CaseExpr:
		return evalCaseExpr(ctx, e)

	case *ast.IfExpr:
		return evalIfExpr(ctx, e)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evalCastExpr evaluates CAST(expr AS type) and SAFE_CAST(expr AS type).
func evalCastExpr(ctx *evalContext, e *ast.CastExpr) (any, error) {
	val, err := evalExpr(ctx, e.Expr)
	if err != nil {
		return nil, err
	}
	result, err := castValue(val, e.Type)
	if err != nil {
		if e.Safe {
			return nil, nil // SAFE_CAST returns NULL on failure
		}
		return nil, err
	}
	return result, nil
}

// evalCaseExpr evaluates CASE [expr] WHEN ... THEN ... [ELSE ...] END expressions.
func evalCaseExpr(ctx *evalContext, e *ast.CaseExpr) (any, error) {
	if e.Expr != nil {
		// Simple CASE: CASE expr WHEN val THEN result ...
		base, err := evalExpr(ctx, e.Expr)
		if err != nil {
			return nil, err
		}
		for _, w := range e.Whens {
			cond, err := evalExpr(ctx, w.Cond)
			if err != nil {
				return nil, err
			}
			if base != nil && cond != nil && store.CompareValues(base, cond) == 0 {
				return evalExpr(ctx, w.Then)
			}
		}
	} else {
		// Searched CASE: CASE WHEN cond THEN result ...
		for _, w := range e.Whens {
			cond, err := evalExpr(ctx, w.Cond)
			if err != nil {
				return nil, err
			}
			if b, ok := cond.(bool); ok && b {
				return evalExpr(ctx, w.Then)
			}
		}
	}
	if e.Else != nil {
		return evalExpr(ctx, e.Else.Expr)
	}
	return nil, nil // NULL if no branch matched and no ELSE
}

// evalIfExpr evaluates IF(cond, true_result, else_result).
func evalIfExpr(ctx *evalContext, e *ast.IfExpr) (any, error) {
	cond, err := evalExpr(ctx, e.Expr)
	if err != nil {
		return nil, err
	}
	b, ok := cond.(bool)
	if ok && b {
		return evalExpr(ctx, e.TrueResult)
	}
	return evalExpr(ctx, e.ElseResult)
}

func evalBinaryExpr(ctx *evalContext, e *ast.BinaryExpr) (any, error) {
	// Handle logical operators first (short-circuit)
	switch e.Op {
	case ast.OpAnd:
		left, err := evalExpr(ctx, e.Left)
		if err != nil {
			return nil, err
		}
		// FALSE AND anything = FALSE (short-circuit)
		lb, lOk := left.(bool)
		if lOk && !lb {
			return false, nil
		}
		right, err := evalExpr(ctx, e.Right)
		if err != nil {
			return nil, err
		}
		rb, rOk := right.(bool)
		if rOk && !rb {
			return false, nil
		}
		// If either side is NULL (not bool), result is NULL
		if !lOk || !rOk {
			return nil, nil
		}
		return lb && rb, nil

	case ast.OpOr:
		left, err := evalExpr(ctx, e.Left)
		if err != nil {
			return nil, err
		}
		// TRUE OR anything = TRUE (short-circuit)
		lb, lOk := left.(bool)
		if lOk && lb {
			return true, nil
		}
		right, err := evalExpr(ctx, e.Right)
		if err != nil {
			return nil, err
		}
		rb, rOk := right.(bool)
		if rOk && rb {
			return true, nil
		}
		// If either side is NULL (not bool), result is NULL
		if !lOk || !rOk {
			return nil, nil
		}
		return lb || rb, nil

	case ast.OpLike, ast.OpNotLike:
		return evalLikeExpr(ctx, e)
	}

	// Comparison operators
	left, err := evalExpr(ctx, e.Left)
	if err != nil {
		return nil, err
	}
	right, err := evalExpr(ctx, e.Right)
	if err != nil {
		return nil, err
	}

	// NULL semantics: any comparison with NULL returns nil (= false in WHERE)
	if left == nil || right == nil {
		return nil, nil
	}

	cmp := store.CompareValues(left, right)

	switch e.Op {
	case ast.OpEqual:
		return cmp == 0, nil
	case ast.OpNotEqual:
		return cmp != 0, nil
	case ast.OpLess:
		return cmp < 0, nil
	case ast.OpGreater:
		return cmp > 0, nil
	case ast.OpLessEqual:
		return cmp <= 0, nil
	case ast.OpGreaterEqual:
		return cmp >= 0, nil
	case ast.OpAdd:
		return evalArithmetic(left, right,
			func(a, b int64) (any, error) { return a + b, nil },
			func(a, b float64) (any, error) { return a + b, nil })
	case ast.OpSub:
		return evalArithmetic(left, right,
			func(a, b int64) (any, error) { return a - b, nil },
			func(a, b float64) (any, error) { return a - b, nil })
	case ast.OpMul:
		return evalArithmetic(left, right,
			func(a, b int64) (any, error) { return a * b, nil },
			func(a, b float64) (any, error) { return a * b, nil })
	case ast.OpDiv:
		return evalArithmetic(left, right,
			func(a, b int64) (any, error) {
				if b == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return a / b, nil
			},
			func(a, b float64) (any, error) {
				if b == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				return a / b, nil
			})
	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", e.Op)
	}
}

// evalArithmetic performs arithmetic on two numeric values, promoting int64 to float64 when mixed.
func evalArithmetic(left, right any, intOp func(int64, int64) (any, error), floatOp func(float64, float64) (any, error)) (any, error) {
	switch l := left.(type) {
	case int64:
		switch r := right.(type) {
		case int64:
			return intOp(l, r)
		case float64:
			return floatOp(float64(l), r)
		}
	case float64:
		switch r := right.(type) {
		case int64:
			return floatOp(l, float64(r))
		case float64:
			return floatOp(l, r)
		}
	}
	return nil, fmt.Errorf("arithmetic not supported for types %T and %T", left, right)
}

func evalUnaryExpr(ctx *evalContext, e *ast.UnaryExpr) (any, error) {
	val, err := evalExpr(ctx, e.Expr)
	if err != nil {
		return nil, err
	}

	switch e.Op {
	case ast.OpNot:
		if val == nil {
			return nil, nil
		}
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("NOT requires a boolean operand, got %T", val)
		}
		return !b, nil

	case ast.OpMinus:
		if val == nil {
			return nil, nil
		}
		switch v := val.(type) {
		case int64:
			return -v, nil
		case float64:
			return -v, nil
		default:
			return nil, fmt.Errorf("unary minus not supported for type %T", val)
		}

	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", e.Op)
	}
}

func evalLikeExpr(ctx *evalContext, e *ast.BinaryExpr) (any, error) {
	left, err := evalExpr(ctx, e.Left)
	if err != nil {
		return nil, err
	}
	right, err := evalExpr(ctx, e.Right)
	if err != nil {
		return nil, err
	}

	if left == nil || right == nil {
		return nil, nil
	}

	str, ok := left.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE requires string operand, got %T", left)
	}
	pattern, ok := right.(string)
	if !ok {
		return nil, fmt.Errorf("LIKE requires string pattern, got %T", right)
	}

	matched := matchLike(str, pattern)
	if e.Op == ast.OpNotLike {
		return !matched, nil
	}
	return matched, nil
}

// matchLike implements SQL LIKE pattern matching.
// '%' matches any sequence of characters, '_' matches any single character.
// Operates on runes for correct Unicode handling.
func matchLike(str, pattern string) bool {
	sr := []rune(str)
	pr := []rune(pattern)
	return matchLikeRunes(sr, pr, 0, 0)
}

func matchLikeRunes(str, pattern []rune, si, pi int) bool {
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%':
			pi++
			// Skip consecutive '%'
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi == len(pattern) {
				return true
			}
			for i := si; i <= len(str); i++ {
				if matchLikeRunes(str, pattern, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(str) {
				return false
			}
			si++
			pi++
		default:
			if si >= len(str) || str[si] != pattern[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(str)
}

func evalInExpr(ctx *evalContext, e *ast.InExpr) (any, error) {
	left, err := evalExpr(ctx, e.Left)
	if err != nil {
		return nil, err
	}

	if left == nil {
		return nil, nil
	}

	values, ok := e.Right.(*ast.ValuesInCondition)
	if !ok {
		return nil, fmt.Errorf("unsupported IN condition type: %T", e.Right)
	}

	found := false
	hasNull := false
	for _, expr := range values.Exprs {
		val, err := evalExpr(ctx, expr)
		if err != nil {
			return nil, err
		}
		if val == nil {
			hasNull = true
			continue
		}
		if store.CompareValues(left, val) == 0 {
			found = true
			break
		}
	}

	if found {
		if e.Not {
			return false, nil
		}
		return true, nil
	}
	// Not found: if list had NULLs, result is NULL (unknown)
	if hasNull {
		return nil, nil
	}
	if e.Not {
		return true, nil
	}
	return false, nil
}

func evalBetweenExpr(ctx *evalContext, e *ast.BetweenExpr) (any, error) {
	left, err := evalExpr(ctx, e.Left)
	if err != nil {
		return nil, err
	}
	low, err := evalExpr(ctx, e.RightStart)
	if err != nil {
		return nil, err
	}
	high, err := evalExpr(ctx, e.RightEnd)
	if err != nil {
		return nil, err
	}

	if left == nil || low == nil || high == nil {
		return nil, nil
	}

	cmpLow := store.CompareValues(left, low)
	cmpHigh := store.CompareValues(left, high)
	between := cmpLow >= 0 && cmpHigh <= 0

	if e.Not {
		return !between, nil
	}
	return between, nil
}

// evalWhere evaluates a WHERE expression and returns a boolean result.
// nil values are treated as false.
func evalWhere(ctx *evalContext, expr ast.Expr) (bool, error) {
	val, err := evalExpr(ctx, expr)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	b, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("WHERE expression must evaluate to bool, got %T", val)
	}
	return b, nil
}
