package chunker

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"
)

type sizedReader struct{ *bytes.Reader }

func (r sizedReader) Remaining() uint64 { return uint64(r.Len()) }

func TestBoundedSegmentsPreserveChunks(t *testing.T) {
	for _, mode := range []string{"cdc", "fixed"} {
		c, err := New(Config{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{0, 1, 4096, 131071, 131072, 524289, 1048576, 5*1024*1024 + 1} {
			data := make([]byte, size)
			rand.New(rand.NewSource(42)).Read(data)
			for _, zero := range []bool{false, true} {
				if zero {
					clear(data)
				}
				collect := func(r io.Reader) []ChunkResult {
					var out []ChunkResult
					if err := c.Chunk(r, func(x ChunkResult) error { out = append(out, x); return nil }); err != nil {
						t.Fatal(err)
					}
					return out
				}
				want := collect(bytes.NewReader(data))
				got := collect(sizedReader{bytes.NewReader(data)})
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("mode=%s size=%d zero=%t chunks changed", mode, size, zero)
				}
			}
		}
	}
}

func BenchmarkSparseSegmentBuffer(b *testing.B) {
	data := bytes.Repeat([]byte{13}, 4096)
	for _, bounded := range []bool{false, true} {
		name := "dense-buffer"
		if bounded {
			name = "bounded-buffer"
		}
		b.Run(name, func(b *testing.B) {
			c, _ := New(Config{})
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var r io.Reader = bytes.NewReader(data)
				if bounded {
					r = sizedReader{bytes.NewReader(data)}
				}
				if err := c.Chunk(r, func(ChunkResult) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
