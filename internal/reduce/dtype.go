package reduce

import "fmt"

// DType is the internal dtype ID used by the C++ JACCL backend.
type DType int

const (
	Bool DType = iota
	Int8
	Int16
	Int32
	Int64
	Uint8
	Uint16
	Uint32
	Uint64
	Float16
	BFloat16
	Float32
	Float64
	Complex64
)

func (d DType) String() string {
	switch d {
	case Bool:
		return "bool"
	case Int8:
		return "int8"
	case Int16:
		return "int16"
	case Int32:
		return "int32"
	case Int64:
		return "int64"
	case Uint8:
		return "uint8"
	case Uint16:
		return "uint16"
	case Uint32:
		return "uint32"
	case Uint64:
		return "uint64"
	case Float16:
		return "float16"
	case BFloat16:
		return "bfloat16"
	case Float32:
		return "float32"
	case Float64:
		return "float64"
	case Complex64:
		return "complex64"
	default:
		return fmt.Sprintf("dtype(%d)", int(d))
	}
}

// DTypeFor maps a Go element type to a JACCL dtype.
func DTypeFor[T any]() (DType, error) {
	var zero T
	switch any(zero).(type) {
	case bool:
		return Bool, nil
	case int8:
		return Int8, nil
	case int16:
		return Int16, nil
	case int32:
		return Int32, nil
	case int64:
		return Int64, nil
	case uint8:
		return Uint8, nil
	case uint16:
		return Uint16, nil
	case uint32:
		return Uint32, nil
	case uint64:
		return Uint64, nil
	case float32:
		return Float32, nil
	case float64:
		return Float64, nil
	case complex64:
		return Complex64, nil
	default:
		return 0, fmt.Errorf("reduce: unsupported element type %T", zero)
	}
}

func ElementSize(dt DType) (int, error) {
	switch dt {
	case Bool, Int8, Uint8:
		return 1, nil
	case Int16, Uint16, Float16, BFloat16:
		return 2, nil
	case Int32, Uint32, Float32:
		return 4, nil
	case Int64, Uint64, Float64, Complex64:
		return 8, nil
	default:
		return 0, fmt.Errorf("reduce: unsupported dtype %v", dt)
	}
}
