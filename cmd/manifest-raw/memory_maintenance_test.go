// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestMemoryAdaptiveThresholds(t *testing.T) {
	g := diskGraph{Version: 1, Size: 1 << 30, Base: strings.Repeat("a", 64)}
	if shouldCompact(g) {
		t.Fatal("empty delta compacted")
	}
	g.Extents = []diskExtent{{Offset: 0, Length: 1 << 29}}
	if !shouldCompact(g) {
		t.Fatal("large coverage not compacted")
	}
	t.Setenv("KUASAR_MEMORY_MAX_OBJECTS", "2")
	g.Extents = []diskExtent{{Offset: 4096, Length: 4096, Key: strings.Repeat("b", 64)}}
	if !shouldCompact(g) {
		t.Fatal("object budget not enforced")
	}
}
func TestCompletedBaseAdoptionAndAtomicFailure(t *testing.T) {
	d := t.TempDir()
	t.Setenv("KUASAR_MEMORY_MAINTENANCE", d)
	key := strings.Repeat("a", 64)
	if _, ok, e := completedMemory(key); e != nil || ok {
		t.Fatal(ok, e)
	}
	ref := diskRef{Manifest: strings.Repeat("b", 64), Kind: "raw", LogicalBytes: 8192}
	if e := atomicJSON(filepath.Join(d, "complete", key), ref); e != nil {
		t.Fatal(e)
	}
	got, ok, e := completedMemory(key)
	if e != nil || !ok || got != ref {
		t.Fatal(got, ok, e)
	}
	// A temporary result never replaces the authoritative completion marker.
	os.WriteFile(filepath.Join(d, "complete", ".pending-test"), []byte("partial"), 0600)
	got, ok, e = completedMemory(key)
	if e != nil || !ok || got != ref {
		t.Fatal(got, ok, e)
	}
}
func TestCompactionStreamsZeroAndInheritedData(t *testing.T) {
	// The background source is the immutable chunk graph, not VM RAM.
	base := strings.Repeat("1", 64)
	delta := strings.Repeat("2", 64)
	key := strings.Repeat("3", 64)
	f := &testGraphFetcher{data: map[string][]byte{base: []byte(strings.Repeat("a", 8192)), delta: []byte(strings.Repeat("b", 8192))}}
	g := diskGraph{Version: 1, Size: 8192, Base: base, Extents: []diskExtent{{Offset: 4096, Length: 4096}}}
	f.data[key], _ = json.Marshal(g)
	s, e := newGraphStream(context.Background(), f, key)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	b := make([]byte, 8192)
	if _, e = s.ReadAt(context.Background(), b, 0); e != nil {
		t.Fatal(e)
	}
	if string(b[:4096]) != strings.Repeat("a", 4096) || string(b[4096:]) != strings.Repeat("\x00", 4096) {
		t.Fatal("zero or inherited bytes lost")
	}
}
func TestRebaseHandlesLaterWritesAndDivergentSibling(t *testing.T) {
	base, delta, packed := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	f := &testGraphFetcher{data: map[string][]byte{base: []byte(strings.Repeat("a", 12288)), delta: []byte(strings.Repeat("b", 12288))}}
	old := diskGraph{Version: 1, Size: 12288, Base: base, Extents: []diskExtent{{Offset: 0, Length: 4096, Key: delta}, {Offset: 8192, Length: 4096}}}
	read := func(g diskGraph) []byte {
		s := &graphStream{graph: g, fetcher: f, streams: make(map[string]fetch.Stream)}
		defer s.Close()
		b := make([]byte, g.Size)
		if _, e := s.ReadAt(context.Background(), b, 0); e != nil {
			t.Fatal(e)
		}
		return b
	}
	f.data[packed] = read(old)
	for _, ext := range [][]diskExtent{
		{{Offset: 0, Length: 4096, Key: delta}, {Offset: 4096, Length: 4096, Key: delta, Source: 4096}, {Offset: 8192, Length: 4096}},
		{{Offset: 4096, Length: 4096}}, // Sibling must undo the ancestor's first/last changes.
		{},
	} {
		current := diskGraph{Version: 1, Size: 12288, Base: base, Extents: ext}
		got, e := rebaseMemory(current, old, diskRef{Manifest: packed, Kind: "raw", LogicalBytes: 12288})
		if e != nil {
			t.Fatal(e)
		}
		if string(read(got)) != string(read(current)) {
			t.Fatal("rebase changed memory contents")
		}
	}
}

func TestMemoryMergeWorkerBound(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int
		bad   bool
	}{
		{"", 2, false}, {"1", 1, false}, {"2", 2, false}, {"4", 4, false},
		{"0", 0, true}, {"5", 0, true}, {"-1", 0, true}, {"invalid", 0, true},
	} {
		t.Setenv("KUASAR_MEMORY_MERGE_WORKERS", tc.value)
		n, e := memoryMergeWorkers()
		if (e != nil) != tc.bad || n != tc.want {
			t.Fatalf("%q: workers=%d error=%v", tc.value, n, e)
		}
	}
}

func TestMemorySchedulingGate(t *testing.T) {
	t.Setenv("KUASAR_MEMORY_MAINTENANCE", t.TempDir())
	t.Setenv("KUASAR_MEMORY_IO_GATE", "1")
	t.Setenv("KUASAR_MEMORY_TUNING", "1")
	a, e := foregroundMemoryIO()
	if e != nil {
		t.Fatal(e)
	}
	defer closeLock(a)
	b, e := foregroundMemoryIO()
	if e != nil {
		t.Fatal(e)
	}
	defer closeLock(b)
	bg, e := memoryIOGate()
	if e != nil {
		t.Fatal(e)
	}
	defer closeLock(bg)
	try := func() error { return syscall.Flock(int(bg.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
	if e = try(); !errors.Is(e, syscall.EWOULDBLOCK) {
		t.Fatalf("background admitted: %v", e)
	}
	closeLock(a)
	if e = try(); !errors.Is(e, syscall.EWOULDBLOCK) {
		t.Fatalf("second publisher unprotected: %v", e)
	}
	closeLock(b)
	if e = try(); e != nil {
		t.Fatalf("background cannot progress: %v", e)
	}
}

func TestMemoryBoundedReadsPreserveOffsetsAndErrors(t *testing.T) {
	c := &countedMemory{}
	data := bytes.Repeat([]byte{37}, 150000)
	got := make([]byte, len(data))
	calls := 0
	n, e := c.readBounded(context.Background(), got, 123, func(_ context.Context, p []byte, off uint64) (int, error) {
		calls++
		if len(p) > 64*1024 {
			t.Fatal("unbounded read")
		}
		return copy(p, data[off-123:]), nil
	})
	if e != nil || n != len(data) || !bytes.Equal(got, data) || calls != 3 || c.bytes != uint64(n) {
		t.Fatalf("n=%d e=%v calls=%d bytes=%d", n, e, calls, c.bytes)
	}
	sentinel := errors.New("read failure")
	n, e = c.readBounded(context.Background(), got, 0, func(_ context.Context, p []byte, _ uint64) (int, error) { return 3, sentinel })
	if n != 3 || !errors.Is(e, sentinel) {
		t.Fatalf("lost partial error: %d %v", n, e)
	}
	n, e = c.readBounded(context.Background(), got, 0, func(context.Context, []byte, uint64) (int, error) { return 0, nil })
	if n != 0 || !errors.Is(e, io.ErrNoProgress) {
		t.Fatalf("zero read: %d %v", n, e)
	}
}

func TestMemoryReadDoesNotHoldGateDuringStoreIO(t *testing.T) {
	t.Setenv("KUASAR_MEMORY_MAINTENANCE", t.TempDir())
	t.Setenv("KUASAR_MEMORY_IO_GATE", "1")
	t.Setenv("KUASAR_MEMORY_TUNING", "1")
	bg, e := memoryIOGate()
	if e != nil {
		t.Fatal(e)
	}
	defer closeLock(bg)
	fg, e := memoryIOGate()
	if e != nil {
		t.Fatal(e)
	}
	defer closeLock(fg)
	c := &countedMemory{gate: bg}
	_, e = c.readBounded(context.Background(), make([]byte, 1), 0, func(_ context.Context, p []byte, _ uint64) (int, error) {
		// Simulate a foreground request arriving during a background Store RPC.
		e := syscall.Flock(int(fg.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
		if e != nil {
			t.Fatalf("background read blocks foreground admission: %v", e)
		}
		syscall.Flock(int(fg.Fd()), syscall.LOCK_UN)
		p[0] = 1
		return 1, nil
	})
	if e != nil {
		t.Fatal(e)
	}
}
