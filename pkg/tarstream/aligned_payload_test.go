package tarstream

import (
	"bytes"
	"context"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"os"
	"testing"
)

func TestAlignedPayloadPreservesIdentityAndLocalRanges(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, size := range []int{4096, 16384, 1048576} {
			data := bytes.Repeat([]byte{43}, size)
			source, err := sparse.NewSource(bytes.NewReader(data), uint64(size), nil)
			if err != nil {
				t.Fatal(err)
			}
			opts := []WriteOption{}
			if compact {
				opts = append(opts, WithSparseIdentity())
			}
			var old bytes.Buffer
			scheme, digest, err := WriteTo(context.Background(), &old, "snapshot", source, opts...)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.CreateTemp(t.TempDir(), "aligned")
			if err != nil {
				t.Fatal(err)
			}
			opts = append(opts, WithPageAlignedPayload())
			ns, nd, err := WriteTo(context.Background(), f, "snapshot", source, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if ns != scheme || nd != digest {
				t.Fatal("padding changed logical identity")
			}
			info, _ := f.Stat()
			view, _, err := SourceAt(f, info.Size(), "snapshot", WithExpectedDigest(scheme, digest))
			if err != nil {
				t.Fatal(err)
			}
			run, err := view.RunAt(0, uint64(size))
			if err != nil {
				t.Fatal(err)
			}
			_, offset, ok := run.(interface {
				LocalFileRange() (*os.File, uint64, bool)
			}).LocalFileRange()
			if !ok || offset%4096 != 0 {
				t.Fatalf("unaligned offset %d", offset)
			}
			got := make([]byte, size)
			if _, err = run.ReadAt(context.Background(), got, 0); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatal("payload differs")
			}
			f.Close()
		}
	}
}

func TestPagePaddingCannotChangeExtractionMetadata(t *testing.T) {
	for _, pad := range []string{"", "x", "00", string(bytes.Repeat([]byte{'0'}, 4097))} {
		h := paddedPayloadHeaders(makePayloadHeaders("snapshot", 4096, 4608, 4096, true, true), pad)
		if canonicalPayloadHeaders(h.payloadHeader[:], h.paxHeader[:], h.records, "snapshot", 4096, 4608, 4096, true) {
			t.Fatalf("invalid padding accepted: len=%d", len(pad))
		}
	}
	h := paddedPayloadHeaders(makePayloadHeaders("snapshot", 4096, 4608, 4096, true, true), "0")
	h.records["uid"] = "123"
	if canonicalPayloadHeaders(h.payloadHeader[:], h.paxHeader[:], h.records, "snapshot", 4096, 4608, 4096, true) {
		t.Fatal("uncommitted ownership accepted")
	}
}
