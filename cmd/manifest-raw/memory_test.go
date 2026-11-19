// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestMemoryDeltaKeepsInheritedWritesAndZeroOverwrite(t *testing.T) {
	ctx := context.Background()
	base := strings.Repeat("1", 64)
	delta := strings.Repeat("2", 64)
	next := strings.Repeat("3", 64)
	f := &testGraphFetcher{data: map[string][]byte{base: bytes.Repeat([]byte{7}, 3*4096), delta: append(bytes.Repeat([]byte{9}, 4096), make([]byte, 2*4096)...), next: bytes.Repeat([]byte{4}, 3*4096)}}
	spec := memoryDelta{Version: 1, Size: 3 * 4096, Base: diskRef{Manifest: base, LogicalBytes: 3 * 4096, Kind: "raw"}, Delta: diskRef{Manifest: delta, LogicalBytes: 3 * 4096}, Ranges: [][2]uint64{{0, 4096}, {8192, 4096}}}
	g, e := composeMemoryMap(ctx, f, spec)
	if e != nil {
		t.Fatal(e)
	}
	graphKey := strings.Repeat("4", 64)
	f.data[graphKey], _ = json.Marshal(g)
	// A later generation writes only the middle page; inherited first/last writes must survive.
	spec.Base = diskRef{Manifest: graphKey, LogicalBytes: 3 * 4096, Kind: "manifest-memory-graph"}
	spec.Delta.Manifest = next
	spec.Ranges = [][2]uint64{{4096, 4096}}
	g, e = composeMemoryMap(ctx, f, spec)
	if e != nil {
		t.Fatal(e)
	}
	s := &graphStream{graph: g, fetcher: f, streams: make(map[string]fetch.Stream)}
	defer s.Close()
	got := make([]byte, 3*4096)
	if _, e = s.ReadAt(ctx, got, 0); e != nil {
		t.Fatal(e)
	}
	want := append(bytes.Repeat([]byte{9}, 4096), bytes.Repeat([]byte{4}, 4096)...)
	want = append(want, make([]byte, 4096)...)
	if !bytes.Equal(got, want) {
		t.Fatal("inherited write or explicit zero lost")
	}
	if g.Base != base {
		t.Fatal("recursive base graph created")
	}
	for _, rs := range [][][2]uint64{{{0, 4096}, {0, 4096}}, {{1, 4096}}, {{8192, 8192}}} {
		spec.Ranges = rs
		if _, e = composeMemoryMap(ctx, f, spec); e == nil {
			t.Fatal("invalid dirty range accepted")
		}
	}
}

// Declared zeroes must mask nonzero base bytes, while undeclared sparse holes inherit.
func TestMemoryDeltaSparseZeroMask(t *testing.T) {
	base := strings.Repeat("1", 64)
	d := strings.Repeat("2", 64)
	f := &sparseMemoryFetcher{testGraphFetcher: testGraphFetcher{data: map[string][]byte{base: bytes.Repeat([]byte{7}, 8192), d: make([]byte, 8192)}}, delta: d}
	g, e := composeMemoryMap(context.Background(), f, memoryDelta{Version: 1, Size: 8192, Base: diskRef{Manifest: base, LogicalBytes: 8192, Kind: "raw"}, Delta: diskRef{Manifest: d, LogicalBytes: 8192}, Ranges: [][2]uint64{{4096, 4096}}})
	if e != nil {
		t.Fatal(e)
	}
	if len(g.Extents) != 1 || g.Extents[0].Key != "" || g.Extents[0].Offset != 4096 {
		t.Fatalf("wrong zero mask: %+v", g)
	}
}

type sparseMemoryFetcher struct {
	testGraphFetcher
	delta string
}

func (f *sparseMemoryFetcher) OpenManifest(ctx context.Context, k store.ContentKey) (fetch.Stream, error) {
	if manifest.HexKey(k) != f.delta {
		return f.testGraphFetcher.OpenManifest(ctx, k)
	}
	src, e := sparse.NewSource(bytes.NewReader(f.data[f.delta]), 8192, []sparse.Extent{{Offset: 0, Size: 8192}})
	return testStream{src}, e
}

// Composing a two-GiB memory map must not read its payload, even if the
// referenced base is entirely remote and unavailable until a future fault.
type unreadablePayload struct{ reads *int }

func (r unreadablePayload) ReadAt(p []byte, off int64) (int, error) {
	*r.reads++
	return 0, fmt.Errorf("unexpected payload read at %d", off)
}

type metadataOnlyFetcher struct{ reads int }

func (f *metadataOnlyFetcher) OpenManifest(_ context.Context, _ store.ContentKey) (fetch.Stream, error) {
	src, e := sparse.NewSource(unreadablePayload{&f.reads}, 2<<30, nil)
	return testStream{src}, e
}
func TestMemoryCompositionDoesNotFetchRAM(t *testing.T) {
	f := &metadataOnlyFetcher{}
	g, e := composeMemoryMap(context.Background(), f, memoryDelta{Version: 1, Size: 2 << 30,
		Base:  diskRef{Manifest: strings.Repeat("1", 64), LogicalBytes: 2 << 30, Kind: "raw"},
		Delta: diskRef{Manifest: strings.Repeat("2", 64), LogicalBytes: 2 << 30}, Ranges: [][2]uint64{{8192, 4096}}})
	if e != nil {
		t.Fatal(e)
	}
	if f.reads != 0 || len(g.Extents) != 1 {
		t.Fatalf("payload read while composing: %d %+v", f.reads, g)
	}
}
