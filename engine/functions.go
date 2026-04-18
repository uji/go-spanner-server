package engine

import (
	"crypto/rand"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cloudspannerecosystem/memefish/ast"
)

// evalCallExpr dispatches to built-in function implementations.
func evalCallExpr(ctx *evalContext, e *ast.CallExpr) (any, error) {
	// Return pre-computed aggregate value if available (set during GROUP BY evaluation).
	if ctx.aggValues != nil {
		if v, ok := ctx.aggValues[e]; ok {
			return v, nil
		}
	}

	name := strings.ToUpper(e.Func.Idents[len(e.Func.Idents)-1].Name)

	// Evaluate arguments eagerly for most functions.
	// Functions that need lazy evaluation (e.g. COALESCE, IF) handle args themselves.
	switch name {
	case "COALESCE":
		return evalCoalesce(ctx, e)
	case "IFNULL":
		return evalIfNull(ctx, e)
	case "NULLIF":
		return evalNullIf(ctx, e)
	case "PENDING_COMMIT_TIMESTAMP":
		if len(e.Args) != 0 {
			return nil, fmt.Errorf("PENDING_COMMIT_TIMESTAMP() takes no arguments")
		}
		if !ctx.inReadWriteTx() {
			return nil, fmt.Errorf("PENDING_COMMIT_TIMESTAMP() is only valid in a read-write transaction")
		}
		return ctx.commitTS, nil
	case "CURRENT_TIMESTAMP":
		return time.Now().UTC(), nil
	case "GENERATE_UUID":
		return generateUUID()
	}

	// Evaluate all arguments for the remaining functions.
	args, err := evalArgs(ctx, e.Args)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	switch name {
	case "CONCAT":
		return evalConcat(args)
	case "UPPER":
		return evalUpper(args)
	case "LOWER":
		return evalLower(args)
	case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH":
		return evalLength(args)
	case "BYTE_LENGTH":
		return evalByteLength(args)
	case "SUBSTR", "SUBSTRING":
		return evalSubstr(args)
	case "TRIM":
		return evalTrim(args)
	case "LTRIM":
		return evalLTrim(args)
	case "RTRIM":
		return evalRTrim(args)
	case "STARTS_WITH":
		return evalStartsWith(args)
	case "ENDS_WITH":
		return evalEndsWith(args)
	case "REPLACE":
		return evalReplace(args)
	case "STRPOS":
		return evalStrpos(args)
	case "LPAD":
		return evalLpad(args)
	case "RPAD":
		return evalRpad(args)
	case "REVERSE":
		return evalReverseStr(args)
	case "REPEAT":
		return evalRepeat(args)
	case "ABS":
		return evalAbs(args)
	case "MOD":
		return evalMod(args)
	case "CEIL", "CEILING":
		return evalCeil(args)
	case "FLOOR":
		return evalFloor(args)
	case "ROUND":
		return evalRound(args)
	case "SIGN":
		return evalSign(args)
	case "GREATEST":
		return evalGreatest(args)
	case "LEAST":
		return evalLeast(args)
	default:
		return nil, fmt.Errorf("unsupported function: %s", name)
	}
}

// evalArgs evaluates a list of function arguments.
func evalArgs(ctx *evalContext, rawArgs []ast.Arg) ([]any, error) {
	args := make([]any, len(rawArgs))
	for i, arg := range rawArgs {
		ea, ok := arg.(*ast.ExprArg)
		if !ok {
			return nil, fmt.Errorf("unsupported argument type: %T", arg)
		}
		val, err := evalExpr(ctx, ea.Expr)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i+1, err)
		}
		args[i] = val
	}
	return args, nil
}

// evalCoalesce returns the first non-NULL argument.
func evalCoalesce(ctx *evalContext, e *ast.CallExpr) (any, error) {
	for _, arg := range e.Args {
		ea, ok := arg.(*ast.ExprArg)
		if !ok {
			return nil, fmt.Errorf("unsupported argument type: %T", arg)
		}
		val, err := evalExpr(ctx, ea.Expr)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	return nil, nil
}

// evalIfNull returns a if non-nil, else b.
func evalIfNull(ctx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 2 {
		return nil, fmt.Errorf("IFNULL requires exactly 2 arguments")
	}
	args, err := evalArgs(ctx, e.Args)
	if err != nil {
		return nil, err
	}
	if args[0] != nil {
		return args[0], nil
	}
	return args[1], nil
}

// evalNullIf returns NULL if a == b, otherwise returns a.
func evalNullIf(ctx *evalContext, e *ast.CallExpr) (any, error) {
	if len(e.Args) != 2 {
		return nil, fmt.Errorf("NULLIF requires exactly 2 arguments")
	}
	args, err := evalArgs(ctx, e.Args)
	if err != nil {
		return nil, err
	}
	if args[0] == nil || args[1] == nil {
		return args[0], nil
	}
	if castComparableEq(args[0], args[1]) {
		return nil, nil
	}
	return args[0], nil
}

// castComparableEq compares two values for equality for NULLIF purposes.
// Values of different types are never equal.
func castComparableEq(a, b any) bool {
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	default:
		return false
	}
}

// --- String functions ---

func evalConcat(args []any) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("CONCAT requires at least 1 argument")
	}
	var sb strings.Builder
	for _, a := range args {
		if a == nil {
			return nil, nil // NULL propagation
		}
		s, ok := a.(string)
		if !ok {
			return nil, fmt.Errorf("CONCAT requires string arguments, got %T", a)
		}
		sb.WriteString(s)
	}
	return sb.String(), nil
}

func evalUpper(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("UPPER requires a string argument, got %T", args[0])
	}
	return strings.ToUpper(s), nil
}

func evalLower(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LOWER requires a string argument, got %T", args[0])
	}
	return strings.ToLower(s), nil
}

func evalLength(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LENGTH requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LENGTH requires a string argument, got %T", args[0])
	}
	return int64(utf8.RuneCountInString(s)), nil
}

func evalByteLength(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("BYTE_LENGTH requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case string:
		return int64(len(v)), nil
	case []byte:
		return int64(len(v)), nil
	default:
		return nil, fmt.Errorf("BYTE_LENGTH requires a string or bytes argument, got %T", args[0])
	}
}

// evalSubstr implements SUBSTR(str, pos [, length]) using 1-based indexing.
func evalSubstr(args []any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTR requires 2 or 3 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("SUBSTR requires a string as first argument, got %T", args[0])
	}
	runes := []rune(s)
	pos, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("SUBSTR: position: %w", err)
	}

	// Convert 1-based position to 0-based index (Spanner: negative pos counts from end).
	// pos == 0 is invalid in Cloud Spanner.
	if pos == 0 {
		return nil, fmt.Errorf("SUBSTR: start position must be non-zero")
	}
	var startIdx int
	if pos >= 1 {
		startIdx = int(pos) - 1
	} else {
		startIdx = len(runes) + int(pos)
		if startIdx < 0 {
			startIdx = 0
		}
	}
	if startIdx >= len(runes) {
		return "", nil
	}

	if len(args) == 2 {
		return string(runes[startIdx:]), nil
	}

	length, err := toInt64(args[2])
	if err != nil {
		return nil, fmt.Errorf("SUBSTR: length: %w", err)
	}
	if length < 0 {
		return nil, fmt.Errorf("SUBSTR: length must be non-negative")
	}
	if length == 0 {
		return "", nil
	}
	endIdx := startIdx + int(length)
	if endIdx > len(runes) {
		endIdx = len(runes)
	}
	return string(runes[startIdx:endIdx]), nil
}

func evalTrim(args []any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("TRIM requires 1 or 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("TRIM requires a string argument, got %T", args[0])
	}
	if len(args) == 1 {
		return strings.TrimSpace(s), nil
	}
	if args[1] == nil {
		return nil, nil
	}
	cutset, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("TRIM requires a string cutset, got %T", args[1])
	}
	return strings.Trim(s, cutset), nil
}

func evalLTrim(args []any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("LTRIM requires 1 or 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LTRIM requires a string argument, got %T", args[0])
	}
	if len(args) == 1 {
		return strings.TrimLeftFunc(s, unicode.IsSpace), nil
	}
	if args[1] == nil {
		return nil, nil
	}
	cutset, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("LTRIM requires a string cutset, got %T", args[1])
	}
	return strings.TrimLeft(s, cutset), nil
}

func evalRTrim(args []any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("RTRIM requires 1 or 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("RTRIM requires a string argument, got %T", args[0])
	}
	if len(args) == 1 {
		return strings.TrimRightFunc(s, unicode.IsSpace), nil
	}
	if args[1] == nil {
		return nil, nil
	}
	cutset, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("RTRIM requires a string cutset, got %T", args[1])
	}
	return strings.TrimRight(s, cutset), nil
}

func evalStartsWith(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STARTS_WITH requires exactly 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("STARTS_WITH: first argument must be STRING, got %T", args[0])
	}
	prefix, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("STARTS_WITH: second argument must be STRING, got %T", args[1])
	}
	return strings.HasPrefix(s, prefix), nil
}

func evalEndsWith(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("ENDS_WITH requires exactly 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("ENDS_WITH: first argument must be STRING, got %T", args[0])
	}
	suffix, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("ENDS_WITH: second argument must be STRING, got %T", args[1])
	}
	return strings.HasSuffix(s, suffix), nil
}

func evalReplace(args []any) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("REPLACE requires exactly 3 arguments")
	}
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("REPLACE: first argument must be STRING, got %T", args[0])
	}
	old, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("REPLACE: second argument must be STRING, got %T", args[1])
	}
	newStr, ok := args[2].(string)
	if !ok {
		return nil, fmt.Errorf("REPLACE: third argument must be STRING, got %T", args[2])
	}
	return strings.ReplaceAll(s, old, newStr), nil
}

func evalStrpos(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("STRPOS requires exactly 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("STRPOS: first argument must be STRING, got %T", args[0])
	}
	sub, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("STRPOS: second argument must be STRING, got %T", args[1])
	}
	idx := strings.Index(s, sub)
	if idx < 0 {
		return int64(0), nil
	}
	// Return 1-based rune position.
	return int64(utf8.RuneCountInString(s[:idx])) + 1, nil
}

func evalLpad(args []any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("LPAD requires 2 or 3 arguments")
	}
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("LPAD: first argument must be STRING, got %T", args[0])
	}
	length, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("LPAD: %w", err)
	}
	pad := " "
	if len(args) == 3 {
		pad, ok = args[2].(string)
		if !ok {
			return nil, fmt.Errorf("LPAD: third argument must be STRING, got %T", args[2])
		}
	}
	padRunes := []rune(pad)
	if len(padRunes) == 0 {
		return nil, fmt.Errorf("LPAD: pad string must not be empty")
	}
	runes := []rune(s)
	n := int(length)
	if n < 0 {
		return nil, fmt.Errorf("LPAD: length must be non-negative")
	}
	if n <= len(runes) {
		return string(runes[:n]), nil
	}
	needed := n - len(runes)
	var prefix strings.Builder
	for utf8.RuneCountInString(prefix.String()) < needed {
		for _, r := range padRunes {
			if utf8.RuneCountInString(prefix.String()) >= needed {
				break
			}
			prefix.WriteRune(r)
		}
	}
	return prefix.String() + s, nil
}

func evalRpad(args []any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("RPAD requires 2 or 3 arguments")
	}
	for _, a := range args {
		if a == nil {
			return nil, nil
		}
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("RPAD: first argument must be STRING, got %T", args[0])
	}
	length, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("RPAD: %w", err)
	}
	pad := " "
	if len(args) == 3 {
		pad, ok = args[2].(string)
		if !ok {
			return nil, fmt.Errorf("RPAD: third argument must be STRING, got %T", args[2])
		}
	}
	padRunes := []rune(pad)
	if len(padRunes) == 0 {
		return nil, fmt.Errorf("RPAD: pad string must not be empty")
	}
	runes := []rune(s)
	n := int(length)
	if n < 0 {
		return nil, fmt.Errorf("RPAD: length must be non-negative")
	}
	if n <= len(runes) {
		return string(runes[:n]), nil
	}
	needed := n - len(runes)
	var suffix strings.Builder
	for utf8.RuneCountInString(suffix.String()) < needed {
		for _, r := range padRunes {
			if utf8.RuneCountInString(suffix.String()) >= needed {
				break
			}
			suffix.WriteRune(r)
		}
	}
	return s + suffix.String(), nil
}

func evalReverseStr(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("REVERSE requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("REVERSE requires a string argument, got %T", args[0])
	}
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

func evalRepeat(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("REPEAT requires exactly 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("REPEAT: first argument must be STRING, got %T", args[0])
	}
	n, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("REPEAT: %w", err)
	}
	if n < 0 {
		return nil, fmt.Errorf("REPEAT: count must be non-negative")
	}
	if n == 0 {
		return "", nil
	}
	return strings.Repeat(s, int(n)), nil
}

// --- Math functions ---

func evalAbs(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("ABS requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		if v == math.MinInt64 {
			return nil, fmt.Errorf("ABS: integer overflow for value %d", v)
		}
		if v < 0 {
			return -v, nil
		}
		return v, nil
	case float64:
		if v < 0 {
			return -v, nil
		}
		return v, nil
	default:
		return nil, fmt.Errorf("ABS requires numeric argument, got %T", args[0])
	}
}

func evalMod(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("MOD requires exactly 2 arguments")
	}
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	a, err := toInt64(args[0])
	if err != nil {
		return nil, fmt.Errorf("MOD: %w", err)
	}
	b, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("MOD: %w", err)
	}
	if b == 0 {
		return nil, fmt.Errorf("MOD: division by zero")
	}
	return a % b, nil
}

func evalCeil(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CEIL requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	f, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("CEIL: %w", err)
	}
	return math.Ceil(f), nil
}

func evalFloor(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("FLOOR requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	f, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("FLOOR: %w", err)
	}
	return math.Floor(f), nil
}

func evalRound(args []any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("ROUND requires 1 or 2 arguments")
	}
	if args[0] == nil {
		return nil, nil
	}
	f, err := toFloat64(args[0])
	if err != nil {
		return nil, fmt.Errorf("ROUND: %w", err)
	}
	if len(args) == 1 {
		return math.Round(f), nil
	}
	if args[1] == nil {
		return nil, nil
	}
	digits, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("ROUND: decimal places: %w", err)
	}
	scale := math.Pow10(int(digits))
	return math.Round(f*scale) / scale, nil
}

func evalSign(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("SIGN requires exactly 1 argument")
	}
	if args[0] == nil {
		return nil, nil
	}
	switch v := args[0].(type) {
	case int64:
		switch {
		case v > 0:
			return int64(1), nil
		case v < 0:
			return int64(-1), nil
		default:
			return int64(0), nil
		}
	case float64:
		switch {
		case v > 0:
			return float64(1), nil
		case v < 0:
			return float64(-1), nil
		default:
			return float64(0), nil
		}
	default:
		return nil, fmt.Errorf("SIGN requires numeric argument, got %T", args[0])
	}
}

func evalGreatest(args []any) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("GREATEST requires at least 1 argument")
	}
	var greatest any
	for _, a := range args {
		if a == nil {
			return nil, nil // NULL propagation
		}
		if greatest == nil || compareAny(a, greatest) > 0 {
			greatest = a
		}
	}
	return greatest, nil
}

func evalLeast(args []any) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("LEAST requires at least 1 argument")
	}
	var least any
	for _, a := range args {
		if a == nil {
			return nil, nil // NULL propagation
		}
		if least == nil || compareAny(a, least) < 0 {
			least = a
		}
	}
	return least, nil
}

// compareAny compares two values of the same type for GREATEST/LEAST.
func compareAny(a, b any) int {
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
}

// --- CAST ---

// castValue converts val to the target Spanner type described by the AST Type node.
func castValue(val any, target ast.Type) (any, error) {
	if val == nil {
		return nil, nil
	}

	simple, ok := target.(*ast.SimpleType)
	if !ok {
		return nil, fmt.Errorf("CAST to non-scalar type %T not supported", target)
	}

	switch simple.Name {
	case ast.Int64TypeName:
		return castToInt64(val)
	case ast.Float64TypeName, ast.Float32TypeName:
		return castToFloat64(val)
	case ast.StringTypeName:
		return castToString(val)
	case ast.BoolTypeName:
		return castToBool(val)
	case ast.TimestampTypeName:
		return castToTimestamp(val)
	default:
		return nil, fmt.Errorf("CAST to %s not supported", simple.Name)
	}
}

func castToInt64(val any) (any, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case bool:
		if v {
			return int64(1), nil
		}
		return int64(0), nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot cast %q to INT64: %w", v, err)
		}
		return n, nil
	default:
		return nil, fmt.Errorf("cannot cast %T to INT64", val)
	}
}

func castToFloat64(val any) (any, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot cast %q to FLOAT64: %w", v, err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("cannot cast %T to FLOAT64", val)
	}
}

func castToString(val any) (any, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	default:
		return nil, fmt.Errorf("cannot cast %T to STRING", val)
	}
}

func castToBool(val any) (any, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("cannot cast %q to BOOL: %w", v, err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("cannot cast %T to BOOL", val)
	}
}

func castToTimestamp(val any) (any, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return nil, fmt.Errorf("cannot cast %q to TIMESTAMP: %w", v, err)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("cannot cast %T to TIMESTAMP", val)
	}
}

// --- Helpers ---

// toInt64 converts a value to int64.
func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("expected numeric, got %T", v)
	}
}

// toFloat64 converts a value to float64.
func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("expected numeric, got %T", v)
	}
}

// generateUUID generates a UUID v4 string using crypto/rand.
func generateUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	// Set version 4 and variant bits.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
