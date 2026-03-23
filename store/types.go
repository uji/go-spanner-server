package store

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// SpannerType constants for DDL type names.
const (
	TypeBool      = "BOOL"
	TypeInt64     = "INT64"
	TypeFloat64   = "FLOAT64"
	TypeString    = "STRING"
	TypeBytes     = "BYTES"
	TypeTimestamp = "TIMESTAMP"
	TypeArray     = "ARRAY"
	TypeStruct    = "STRUCT"
)

// SpannerType converts a ColType to a full *sppb.Type including nested types.
func SpannerType(t ColType) *sppb.Type {
	switch t.Name {
	case TypeBool:
		return &sppb.Type{Code: sppb.TypeCode_BOOL}
	case TypeInt64:
		return &sppb.Type{Code: sppb.TypeCode_INT64}
	case TypeFloat64:
		return &sppb.Type{Code: sppb.TypeCode_FLOAT64}
	case TypeString:
		return &sppb.Type{Code: sppb.TypeCode_STRING}
	case TypeBytes:
		return &sppb.Type{Code: sppb.TypeCode_BYTES}
	case TypeTimestamp:
		return &sppb.Type{Code: sppb.TypeCode_TIMESTAMP}
	case TypeArray:
		if t.ArrayElem == nil {
			return &sppb.Type{Code: sppb.TypeCode_ARRAY}
		}
		return &sppb.Type{
			Code:             sppb.TypeCode_ARRAY,
			ArrayElementType: SpannerType(*t.ArrayElem),
		}
	case TypeStruct:
		fields := make([]*sppb.StructType_Field, len(t.StructFields))
		for i, f := range t.StructFields {
			fields[i] = &sppb.StructType_Field{
				Name: f.Name,
				Type: SpannerType(f.Type),
			}
		}
		return &sppb.Type{
			Code:       sppb.TypeCode_STRUCT,
			StructType: &sppb.StructType{Fields: fields},
		}
	default:
		return &sppb.Type{Code: sppb.TypeCode_TYPE_CODE_UNSPECIFIED}
	}
}

// TypeCodeFromDDL converts a scalar DDL type name to sppb.TypeCode.
// Deprecated: use SpannerType for full type info including ARRAY/STRUCT.
func TypeCodeFromDDL(name string) sppb.TypeCode {
	return SpannerType(ScalarColType(name)).Code
}

// DecodeValue decodes a structpb.Value into a Go value based on the Spanner ColType.
func DecodeValue(v *structpb.Value, typ ColType) (any, error) {
	if _, ok := v.GetKind().(*structpb.Value_NullValue); ok {
		return nil, nil
	}

	switch typ.Name {
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

	case TypeArray:
		lv, ok := v.GetKind().(*structpb.Value_ListValue)
		if !ok {
			return nil, fmt.Errorf("ARRAY value must be a list, got %T", v.GetKind())
		}
		if typ.ArrayElem == nil {
			return nil, fmt.Errorf("ARRAY type has no element type")
		}
		result := make([]any, len(lv.ListValue.Values))
		for i, elem := range lv.ListValue.Values {
			decoded, err := DecodeValue(elem, *typ.ArrayElem)
			if err != nil {
				return nil, fmt.Errorf("ARRAY element %d: %w", i, err)
			}
			result[i] = decoded
		}
		return result, nil

	case TypeStruct:
		lv, ok := v.GetKind().(*structpb.Value_ListValue)
		if !ok {
			return nil, fmt.Errorf("STRUCT value must be a list, got %T", v.GetKind())
		}
		if len(lv.ListValue.Values) != len(typ.StructFields) {
			return nil, fmt.Errorf("STRUCT value has %d fields, expected %d", len(lv.ListValue.Values), len(typ.StructFields))
		}
		result := make([]any, len(typ.StructFields))
		for i, field := range typ.StructFields {
			decoded, err := DecodeValue(lv.ListValue.Values[i], field.Type)
			if err != nil {
				return nil, fmt.Errorf("STRUCT field %q: %w", field.Name, err)
			}
			result[i] = decoded
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported type: %s", typ.Name)
	}
}

// EncodeValue encodes a Go value into a structpb.Value based on the Spanner ColType.
func EncodeValue(v any, typ ColType) *structpb.Value {
	if v == nil {
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}

	switch typ.Name {
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
	case TypeArray:
		if typ.ArrayElem == nil {
			return &structpb.Value{Kind: &structpb.Value_NullValue{}}
		}
		elems, ok := v.([]any)
		if !ok {
			return &structpb.Value{Kind: &structpb.Value_NullValue{}}
		}
		values := make([]*structpb.Value, len(elems))
		for i, elem := range elems {
			values[i] = EncodeValue(elem, *typ.ArrayElem)
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: values},
		}}
	case TypeStruct:
		fields, ok := v.([]any)
		if !ok {
			return &structpb.Value{Kind: &structpb.Value_NullValue{}}
		}
		values := make([]*structpb.Value, len(typ.StructFields))
		for i, field := range typ.StructFields {
			if i < len(fields) {
				values[i] = EncodeValue(fields[i], field.Type)
			} else {
				values[i] = &structpb.Value{Kind: &structpb.Value_NullValue{}}
			}
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{
			ListValue: &structpb.ListValue{Values: values},
		}}
	default:
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}
	}
}
