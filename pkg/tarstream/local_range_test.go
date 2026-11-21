package tarstream

import (
	"bytes"
	"os"
	"testing"
)

func TestLocalFileRangeMatchesSparseReads(t *testing.T) {
	logical, holes := fixture()
	artifact := mustWrite(t, "snapshot", logical, holes)
	f, err := os.CreateTemp(t.TempDir(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(artifact); err != nil {
		t.Fatal(err)
	}
	for _, off := range []uint64{0, 4096, 1 << 20} {
		s, _, err := SourceAt(f, int64(len(artifact)), "snapshot")
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.RunAt(off, 4096)
		if err != nil {
			t.Fatal(err)
		}
		file, physical, ok := r.(interface {
			LocalFileRange() (*os.File, uint64, bool)
		}).LocalFileRange()
		if !ok {
			t.Fatal("plain file range unavailable")
		}
		got := make([]byte, 4096)
		if _, err = file.ReadAt(got, int64(physical)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, logical[off:off+4096]) {
			t.Fatal("physical mapping differs")
		}
	}
	s, _, err := SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.RunAt(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.(interface {
		LocalFileRange() (*os.File, uint64, bool)
	}).LocalFileRange(); ok {
		t.Fatal("non-file reader exposed an fd")
	}
}
