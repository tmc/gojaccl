package reduce

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestDTypeForElement(t *testing.T) {
	tests := []struct {
		name string
		got  DType
		want DType
		err  error
	}{
		{"Bool", mustDType[bool](t), Bool, nil},
		{"Int8", mustDType[int8](t), Int8, nil},
		{"Int16", mustDType[int16](t), Int16, nil},
		{"Int32", mustDType[int32](t), Int32, nil},
		{"Int64", mustDType[int64](t), Int64, nil},
		{"Uint8", mustDType[uint8](t), Uint8, nil},
		{"Uint16", mustDType[uint16](t), Uint16, nil},
		{"Uint32", mustDType[uint32](t), Uint32, nil},
		{"Uint64", mustDType[uint64](t), Uint64, nil},
		{"Float32", mustDType[float32](t), Float32, nil},
		{"Float64", mustDType[float64](t), Float64, nil},
		{"Complex64", mustDType[complex64](t), Complex64, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("DTypeFor = %v, want %v", tt.got, tt.want)
			}
		})
	}
}

func TestDTypeForElementHalfTypesDeferred(t *testing.T) {
	type Float16 uint16
	type BFloat16 uint16
	t.Run("Float16", func(t *testing.T) {
		if _, err := DTypeFor[Float16](); err == nil {
			t.Fatal("DTypeFor[Float16] = nil error")
		}
	})
	t.Run("BFloat16", func(t *testing.T) {
		if _, err := DTypeFor[BFloat16](); err == nil {
			t.Fatal("DTypeFor[BFloat16] = nil error")
		}
	})
}

func TestSumKernel(t *testing.T) {
	t.Run("BoolMatchesCXX", func(t *testing.T) {
		dst := []bool{false, true}
		src := []bool{true, true}
		if err := Sum(Bool, bytesOf(dst), bytesOf(src)); err != nil {
			t.Fatal(err)
		}
		if want := []bool{true, true}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
	t.Run("Integer", func(t *testing.T) {
		checkSum(t, Int32, []int32{1, 2}, []int32{3, 4}, []int32{4, 6})
	})
	t.Run("UnsignedInteger", func(t *testing.T) {
		checkSum(t, Uint32, []uint32{1, 2}, []uint32{3, 4}, []uint32{4, 6})
	})
	t.Run("Float32", func(t *testing.T) {
		checkSum(t, Float32, []float32{1.5}, []float32{2.25}, []float32{3.75})
	})
	t.Run("Float64", func(t *testing.T) {
		checkSum(t, Float64, []float64{1.5}, []float64{2.25}, []float64{3.75})
	})
	t.Run("Complex64", func(t *testing.T) {
		checkSum(t, Complex64, []complex64{1 + 2i}, []complex64{3 + 4i}, []complex64{4 + 6i})
	})
	t.Run("InPlace", func(t *testing.T) {
		dst := []int32{1, 2}
		if err := Sum(Int32, bytesOf(dst), bytesOf(dst)); err != nil {
			t.Fatal(err)
		}
		if want := []int32{2, 4}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
}

func TestMaxKernel(t *testing.T) {
	t.Run("BoolMatchesCXX", func(t *testing.T) {
		dst := []bool{false, false}
		src := []bool{false, true}
		if err := Max(Bool, bytesOf(dst), bytesOf(src)); err != nil {
			t.Fatal(err)
		}
		if want := []bool{false, true}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
	t.Run("Integer", func(t *testing.T) {
		checkMax(t, Int32, []int32{1, 7}, []int32{3, 4}, []int32{3, 7})
	})
	t.Run("UnsignedInteger", func(t *testing.T) {
		checkMax(t, Uint32, []uint32{1, 7}, []uint32{3, 4}, []uint32{3, 7})
	})
	t.Run("Float32", func(t *testing.T) {
		checkMax(t, Float32, []float32{1, 7}, []float32{3, 4}, []float32{3, 7})
	})
	t.Run("Float64", func(t *testing.T) {
		checkMax(t, Float64, []float64{1, 7}, []float64{3, 4}, []float64{3, 7})
	})
	t.Run("Complex64Lexicographic", func(t *testing.T) {
		checkMax(t, Complex64, []complex64{2 + 0i, 1 + 5i}, []complex64{1 + 9i, 1 + 7i}, []complex64{2 + 0i, 1 + 7i})
	})
	t.Run("InPlace", func(t *testing.T) {
		dst := []int32{1, 2}
		if err := Max(Int32, bytesOf(dst), bytesOf(dst)); err != nil {
			t.Fatal(err)
		}
		if want := []int32{1, 2}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
}

func TestMinKernel(t *testing.T) {
	t.Run("BoolMatchesCXX", func(t *testing.T) {
		dst := []bool{true, false}
		src := []bool{false, true}
		if err := Min(Bool, bytesOf(dst), bytesOf(src)); err != nil {
			t.Fatal(err)
		}
		if want := []bool{false, false}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
	t.Run("Integer", func(t *testing.T) {
		checkMin(t, Int32, []int32{1, 7}, []int32{3, 4}, []int32{1, 4})
	})
	t.Run("UnsignedInteger", func(t *testing.T) {
		checkMin(t, Uint32, []uint32{1, 7}, []uint32{3, 4}, []uint32{1, 4})
	})
	t.Run("Float32", func(t *testing.T) {
		checkMin(t, Float32, []float32{1, 7}, []float32{3, 4}, []float32{1, 4})
	})
	t.Run("Float64", func(t *testing.T) {
		checkMin(t, Float64, []float64{1, 7}, []float64{3, 4}, []float64{1, 4})
	})
	t.Run("Complex64Lexicographic", func(t *testing.T) {
		checkMin(t, Complex64, []complex64{2 + 0i, 1 + 5i}, []complex64{1 + 9i, 1 + 7i}, []complex64{1 + 9i, 1 + 5i})
	})
	t.Run("InPlace", func(t *testing.T) {
		dst := []int32{1, 2}
		if err := Min(Int32, bytesOf(dst), bytesOf(dst)); err != nil {
			t.Fatal(err)
		}
		if want := []int32{1, 2}; !reflect.DeepEqual(dst, want) {
			t.Fatalf("dst = %v, want %v", dst, want)
		}
	})
}

func TestKernelRejectsBadLengths(t *testing.T) {
	t.Run("ByteCountNotMultipleOfElementSize", func(t *testing.T) {
		if err := Sum(Int32, make([]byte, 4), make([]byte, 3)); err == nil {
			t.Fatal("Sum with bad byte count = nil")
		}
	})
	t.Run("OutputShorterThanInput", func(t *testing.T) {
		if err := Sum(Int32, make([]byte, 4), make([]byte, 8)); err == nil {
			t.Fatal("Sum with short output = nil")
		}
	})
}

func mustDType[T any](t *testing.T) DType {
	t.Helper()
	dt, err := DTypeFor[T]()
	if err != nil {
		t.Fatal(err)
	}
	return dt
}

func checkSum[T comparable](t *testing.T, dt DType, dst, src, want []T) {
	t.Helper()
	if err := Sum(dt, bytesOf(dst), bytesOf(src)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("dst = %v, want %v", dst, want)
	}
}

func checkMax[T comparable](t *testing.T, dt DType, dst, src, want []T) {
	t.Helper()
	if err := Max(dt, bytesOf(dst), bytesOf(src)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("dst = %v, want %v", dst, want)
	}
}

func checkMin[T comparable](t *testing.T, dt DType, dst, src, want []T) {
	t.Helper()
	if err := Min(dt, bytesOf(dst), bytesOf(src)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("dst = %v, want %v", dst, want)
	}
}

func bytesOf[T any](s []T) []byte {
	if len(s) == 0 {
		return nil
	}
	var zero T
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s)*int(unsafe.Sizeof(zero)))
}
