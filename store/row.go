package store

import (
	"fmt"
	"time"
)

// Row holds column values in order.
type Row struct {
	Data []any
}

func compareFloat64(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// CompareValues compares two values of the same type.
// Returns -1, 0, or 1.
func CompareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Cross-type numeric promotion: int64 <-> float64
	if av, ok := a.(int64); ok {
		if bv, ok := b.(float64); ok {
			return compareFloat64(float64(av), bv)
		}
	}
	if av, ok := a.(float64); ok {
		if bv, ok := b.(int64); ok {
			return compareFloat64(av, float64(bv))
		}
	}

	switch av := a.(type) {
	case int64:
		bv := b.(int64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv := b.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case bool:
		bv := b.(bool)
		if !av && bv {
			return -1
		}
		if av && !bv {
			return 1
		}
		return 0
	case float64:
		bv := b.(float64)
		return compareFloat64(av, bv)
	case []byte:
		bv := b.([]byte)
		as, bs := string(av), string(bv)
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	case time.Time:
		bv := b.(time.Time)
		if av.Before(bv) {
			return -1
		}
		if av.After(bv) {
			return 1
		}
		return 0
	default:
		// fallback: compare string representation
		as := fmt.Sprintf("%v", a)
		bs := fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
}
