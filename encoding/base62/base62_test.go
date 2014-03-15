package base62

import (
	"fmt"
	"testing"
)

func TestReciprocal(t *testing.T) {
	for _, start := range []int64{0, 1, 99, 999999999} {
		end := DecodeString(EncodeInt(start))
		if end != start {
			fmt.Printf("Start=%d End=%d\n", start, end)
			t.Fail()
		}
	}
}

func BenchmarkB62Encode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncodeInt(int64(i))
	}
}

func BenchmarkB62(b *testing.B) {
	for i := 0; i < b.N; i++ {
		DecodeString(EncodeInt(int64(i)))
	}
}

func TestEncode(t *testing.T) {
	if "0" != EncodeInt(0) {
		t.Fail()
	}
}

func TestDecode(t *testing.T) {
	if 0 != DecodeString("0") {
		t.Fail()
	}
	if 99 != DecodeString("1B") {
		t.Fail()
	}
}
