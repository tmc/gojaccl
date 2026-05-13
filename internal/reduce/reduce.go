package reduce

import (
	"fmt"
	"unsafe"
)

// Sum applies dst[i] += src[i] for the supplied dtype.
func Sum(dt DType, dst, src []byte) error {
	return apply(dt, dst, src, sumOp)
}

// Max applies dst[i] = max(dst[i], src[i]) for the supplied dtype.
func Max(dt DType, dst, src []byte) error {
	return apply(dt, dst, src, maxOp)
}

// Min applies dst[i] = min(dst[i], src[i]) for the supplied dtype.
func Min(dt DType, dst, src []byte) error {
	return apply(dt, dst, src, minOp)
}

type op int

const (
	sumOp op = iota
	maxOp
	minOp
)

func apply(dt DType, dst, src []byte, op op) error {
	size, err := ElementSize(dt)
	if err != nil {
		return err
	}
	if len(src)%size != 0 {
		return fmt.Errorf("reduce: %d source bytes is not a multiple of %d", len(src), size)
	}
	if len(dst) < len(src) {
		return fmt.Errorf("reduce: destination has %d bytes, want at least %d", len(dst), len(src))
	}

	switch dt {
	case Bool:
		return applyBool(bytesAs[bool](dst, len(src)), bytesAs[bool](src, len(src)), op)
	case Int8:
		return applyOrdered(bytesAs[int8](dst, len(src)), bytesAs[int8](src, len(src)), op)
	case Int16:
		return applyOrdered(bytesAs[int16](dst, len(src)), bytesAs[int16](src, len(src)), op)
	case Int32:
		return applyOrdered(bytesAs[int32](dst, len(src)), bytesAs[int32](src, len(src)), op)
	case Int64:
		return applyOrdered(bytesAs[int64](dst, len(src)), bytesAs[int64](src, len(src)), op)
	case Uint8:
		return applyOrdered(bytesAs[uint8](dst, len(src)), bytesAs[uint8](src, len(src)), op)
	case Uint16:
		return applyOrdered(bytesAs[uint16](dst, len(src)), bytesAs[uint16](src, len(src)), op)
	case Uint32:
		return applyOrdered(bytesAs[uint32](dst, len(src)), bytesAs[uint32](src, len(src)), op)
	case Uint64:
		return applyOrdered(bytesAs[uint64](dst, len(src)), bytesAs[uint64](src, len(src)), op)
	case Float32:
		return applyOrdered(bytesAs[float32](dst, len(src)), bytesAs[float32](src, len(src)), op)
	case Float64:
		return applyOrdered(bytesAs[float64](dst, len(src)), bytesAs[float64](src, len(src)), op)
	case Complex64:
		return applyComplex64(bytesAs[complex64](dst, len(src)), bytesAs[complex64](src, len(src)), op)
	case Float16, BFloat16:
		return fmt.Errorf("reduce: %s is not implemented", dt)
	default:
		return fmt.Errorf("reduce: unsupported dtype %v", dt)
	}
}

type ordered interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

func applyOrdered[T ordered](dst, src []T, op op) error {
	for i, v := range src {
		switch op {
		case sumOp:
			dst[i] += v
		case maxOp:
			if v > dst[i] {
				dst[i] = v
			}
		case minOp:
			if v < dst[i] {
				dst[i] = v
			}
		}
	}
	return nil
}

func applyBool(dst, src []bool, op op) error {
	for i, v := range src {
		switch op {
		case sumOp, maxOp:
			dst[i] = dst[i] || v
		case minOp:
			dst[i] = dst[i] && v
		}
	}
	return nil
}

func applyComplex64(dst, src []complex64, op op) error {
	for i, v := range src {
		switch op {
		case sumOp:
			dst[i] += v
		case maxOp:
			if complexLess(dst[i], v) {
				dst[i] = v
			}
		case minOp:
			if complexLess(v, dst[i]) {
				dst[i] = v
			}
		}
	}
	return nil
}

func complexLess(a, b complex64) bool {
	ar, br := real(a), real(b)
	return ar < br || ar == br && imag(a) < imag(b)
}

func bytesAs[T any](b []byte, n int) []T {
	if n == 0 {
		return nil
	}
	var zero T
	size := int(unsafe.Sizeof(zero))
	return unsafe.Slice((*T)(unsafe.Pointer(&b[0])), n/size)
}
