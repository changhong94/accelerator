package chunker

import (
	"bytes"
	"io"
	"math/rand"
	"reflect"
	"testing"
)

type shortReader struct {
	r    io.Reader
	size int
}

func (r shortReader) Read(b []byte) (int, error) {
	if len(b) > r.size {
		b = b[:r.size]
	}
	return r.r.Read(b)
}
func TestCDCIndependentOfReaderFragmentation(t *testing.T) {
	data := make([]byte, 4<<20)
	_, _ = rand.New(rand.NewSource(17)).Read(data)
	clear(data[900000:1500000])
	for _, length := range []int{1, 32771, len(data)} {
		baseline := func(r io.Reader) []ChunkResult {
			var out []ChunkResult
			err := chunkCDC(r, 128<<10, 512<<10, 1<<20, func(c ChunkResult) error { out = append(out, c); return nil })
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
		want := baseline(bytes.NewReader(data[:length]))
		for _, step := range []int{7, 4096, 65536, 131071} {
			got := baseline(shortReader{bytes.NewReader(data[:length]), step})
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("length=%d read=%d: chunk boundaries/data depend on Reader fragmentation", length, step)
			}
		}
	}
}
