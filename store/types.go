package store

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// SpannerType represents a Spanner type code string used in DDL.
const (
	TypeBool      = "BOOL"
	TypeInt64     = "INT64"
	TypeFloat64   = "FLOAT64"
	TypeString    = "STRING"
	TypeBytes     = "BYTES"
	TypeTimestamp = "TIMESTAMP"
)

// TypeCodeFromDDL converts DDL type name to spannerpb.TypeCode.
func TypeCodeFromDDL(name string) sppb.TypeCode {
	switch name {
	case TypeBool:
		return sppb.TypeCode_BOOL
	case TypeInt64:
		return sppb.TypeCode_INT64
	case TypeFloat64:
		return sppb.TypeCode_FLOAT64
	case TypeString:
		return sppb.TypeCode_STRING
	case TypeBytes:
		return sppb.TypeCode_BYTES
	case TypeTimestamp:
		return sppb.TypeCode_TIMESTAMP
	default:
		return sppb.TypeCode_TYPE_CODE_UNSPECIFIED
	}
}

// DecodeValue decodes a structpb.Value into a Go value based on the Spanner type.
func DecodeValue(v *structpb.Value, typ string) (any, error) {
	if _, ok := v.GetKind().(*structpb.Value_NullValue); ok {
		return nil, nil
	}

	switch typ {
	case TypeInt64:
		s, ok := v.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("INT64 value must be a string, got %T", v.GetKind())
		}
		return strconv.ParseInt(s.StringValue, 10, 64)

	case TypeString:
		s, ok := v.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("STRING value must be a string, got %T", v.GetKind())
		}
		return s.StringValue, nil

	case TypeBool:
		b, ok := v.GetKind().(*structpb.Value_BoolValue)
		if !ok {
			return nil, fmt.Errorf("BOOL value must be a bool, got %T", v.GetKind())
		}
		return b.BoolValue, nil

	case TypeFloat64:
		n, ok := v.GetKind().(*structpb.Value_NumberValue)
		if !ok {
			return nil, fmt.Errorf("FLOAT64 value must be a number, got %T", v.GetKind())
		}
		return n.NumberValue, nil

	case TypeBytes:
		s, ok := v.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("BYTES value must be a base64 string, got %T", v.GetKind())
		}
		return base64.StdEncoding.DecodeString(s.StringValue)

	case TypeTimestamp:
		s, ok := v.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("TIMESTAMP value must be a string, got %T", v.GetKind())
		}
		return time.Parse(time.RFC3339Nano, s.StringValue)

	default:
		return nil, fmt.Errorf("unsupported type: %s", typ)
	}
}

// EncodeValue encodes a Go value into a structpb.Value based on the Spanner type.
func EncodeValue(v any, typ string) *structpb.Value {
	if v == nil {
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}

	switch typ {
	case TypeInt64:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("%d", v)}}
	case TypeString:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v.(string)}}
	case TypeBool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: v.(bool)}}
	case TypeFloat64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: v.(float64)}}
	case TypeBytes:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(v.([]byte))}}
	case TypeTimestamp:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: v.(time.Time).Format(time.RFC3339Nano)}}
	default:
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}
}
