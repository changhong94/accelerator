// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"math/rand"
	"strings"
	"testing"
)

type testStream struct{ sparse.Source }

func (testStream) Close() error { return nil }

type testGraphFetcher struct {
	data  map[string][]byte
	opens int
}

func (f *testGraphFetcher) OpenManifest(_ context.Context, key store.ContentKey) (fetch.Stream, error) {
	f.opens++
	b, ok := f.data[manifest.HexKey(key)]
	if !ok {
		return nil, errors.New("missing object")
	}
	src, err := sparse.NewSource(bytes.NewReader(b), uint64(len(b)), nil)
	return testStream{src}, err
}
func TestDiskGraphOverlayMatchesByteWrites(t *testing.T) {
	base, key := strings.Repeat("1", 64), strings.Repeat("2", 64)
	data := make([]byte, 8192)
	r := rand.New(rand.NewSource(7))
	r.Read(data)
	expected := bytes.Repeat([]byte{9}, 4096)
	g := diskGraph{Version: 1, Size: 4096, Base: base}
	f := &testGraphFetcher{data: map[string][]byte{base: append([]byte(nil), expected...), key: data}}
	for i := 0; i < 300; i++ {
		off := r.Intn(4096)
		n := 1 + r.Intn(4096-off)
		source := r.Intn(8192 - n)
		x := diskExtent{Offset: uint64(off), Length: uint64(n), Key: key, Source: uint64(source)}
		if i%3 == 0 {
			x.Key = ""
			clear(expected[off : off+n])
		} else {
			copy(expected[off:off+n], data[source:source+n])
		}
		g.Extents = overlayExtents(g.Extents, []diskExtent{x})
		if e := validateGraph(g); e != nil {
			t.Fatal(e)
		}
		s := &graphStream{graph: g, fetcher: f, streams: make(map[string]fetch.Stream)}
		got := make([]byte, 4096)
		if _, e := s.ReadAt(context.Background(), got, 0); e != nil {
			t.Fatal(e)
		}
		s.Close()
		if !bytes.Equal(got, expected) {
			t.Fatalf("wrong logical bytes after update %d", i)
		}
	}
}
func TestQCOWMapZerosFallthroughAndDeferredPayload(t *testing.T) {
	b := make([]byte, 5*512)
	be := binary.BigEndian
	copy(b, "QFI\xfb")
	be.PutUint32(b[4:], 3)
	be.PutUint32(b[20:], 9)
	be.PutUint64(b[24:], 2048)
	be.PutUint32(b[36:], 1)
	be.PutUint64(b[40:], 512)
	be.PutUint64(b[512:], 1024)
	be.PutUint64(b[1024:], 1536)
	be.PutUint64(b[1032:], 1)
	be.PutUint64(b[1048:], 2048)
	copy(b[1536:], bytes.Repeat([]byte{1}, 512))
	copy(b[2048:], bytes.Repeat([]byte{3}, 512))
	key, base := strings.Repeat("3", 64), strings.Repeat("4", 64)
	ext, e := qcowExtents(context.Background(), sparse.Dense(bytes.NewReader(b), uint64(len(b))), key, 2048)
	if e != nil {
		t.Fatal(e)
	}
	f := &testGraphFetcher{data: map[string][]byte{base: bytes.Repeat([]byte{2}, 2048), key: b}}
	s := &graphStream{graph: diskGraph{Version: 1, Size: 2048, Base: base, Extents: ext}, fetcher: f, streams: make(map[string]fetch.Stream)}
	if f.opens != 0 {
		t.Fatal("payload opened before first read")
	}
	got := make([]byte, 2048)
	if _, e = s.ReadAt(context.Background(), got, 0); e != nil {
		t.Fatal(e)
	}
	want := append(bytes.Repeat([]byte{1}, 512), make([]byte, 512)...)
	want = append(want, bytes.Repeat([]byte{2}, 512)...)
	want = append(want, bytes.Repeat([]byte{3}, 512)...)
	if !bytes.Equal(got, want) {
		t.Fatal("QCOW zero/inherit/data mapping corrupted")
	}
	be.PutUint64(b[1024:], 1<<62)
	if _, e = qcowExtents(context.Background(), sparse.Dense(bytes.NewReader(b), uint64(len(b))), key, 2048); e == nil {
		t.Fatal("unsupported compressed extent accepted")
	}
	delete(f.data, key)
	s = &graphStream{graph: s.graph, fetcher: f, streams: make(map[string]fetch.Stream)}
	if _, e = s.ReadAt(context.Background(), got, 0); e == nil {
		t.Fatal("missing chunk silently became zeros")
	}
}
