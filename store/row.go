package store

import (
	"fmt"
	"time"
)

// Row holds column values in order.
type Row struct {
	Data []interface{}
}

// compareValues compares two values of the same type.
// Returns -1, 0, or 1.
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
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
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
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
