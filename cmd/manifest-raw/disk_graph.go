// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// A graph is a flattened logical block map, not a chain of nested snapshots.
// Extents reference immutable, authenticated QCOW payload manifests. Restoring
// reads this small map; neither the old QCOW data nor the raw base is rebuilt.
type diskExtent struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	Key    string `json:"key,omitempty"` // empty means an explicit zero, not fallthrough
	Source uint64 `json:"source,omitempty"`
}
type diskGraph struct {
	Version int          `json:"version"`
	Size    uint64       `json:"size"`
	Base    string       `json:"base"`
	Extents []diskExtent `json:"extents"`
}
type diskRef struct {
	Manifest     string `json:"manifest"`
	LogicalBytes uint64 `json:"logical_bytes"`
	Kind         string `json:"kind"`
}
type diskCompose struct {
	Size   uint64    `json:"size"`
	Base   diskRef   `json:"base"`
	Layers []diskRef `json:"layers"`
}

func openRef(ctx context.Context, f fetch.Fetcher, key string) (fetch.Stream, error) {
	k, e := manifest.ParseKeyRef(key)
	if e != nil {
		return nil, e
	}
	return f.OpenManifest(ctx, k)
}
func loadGraph(ctx context.Context, f fetch.Fetcher, key string) (diskGraph, error) {
	var g diskGraph
	s, e := openRef(ctx, f, key)
	if e != nil {
		return g, e
	}
	defer s.Close()
	if s.Size() > 64<<20 {
		return g, errors.New("disk map exceeds metadata limit")
	}
	b := make([]byte, s.Size())
	if _, e = s.ReadAt(ctx, b, 0); e != nil {
		return g, e
	}
	if e = json.Unmarshal(b, &g); e != nil {
		return g, e
	}
	return g, validateGraph(g)
}
func validateGraph(g diskGraph) error {
	if g.Version != 1 || g.Size == 0 || g.Size%512 != 0 {
		return errors.New("invalid logical disk map")
	}
	if _, e := manifest.ParseKeyRef(g.Base); e != nil {
		return e
	}
	end := uint64(0)
	for _, x := range g.Extents {
		if x.Offset < end || x.Offset > g.Size || x.Length == 0 || x.Length > g.Size-x.Offset || x.Source > ^uint64(0)-x.Length {
			return errors.New("invalid disk extent")
		}
		if x.Key != "" {
			if _, e := manifest.ParseKeyRef(x.Key); e != nil {
				return e
			}
		}
		end = x.Offset + x.Length
	}
	return nil
}

// Overlay sorted extent lists in O((n+m) log(n+m)), splitting old intervals at
// write/zero boundaries. Repeated snapshots flatten the map instead of growing
// an unbounded recursive backing chain. Adjacent source ranges are coalesced.
func overlayExtents(old, updates []diskExtent) []diskExtent {
	points := make([]uint64, 0, 2*(len(old)+len(updates)))
	for _, xs := range [][]diskExtent{old, updates} {
		for _, x := range xs {
			points = append(points, x.Offset, x.Offset+x.Length)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i] < points[j] })
	var out []diskExtent
	i, j := 0, 0
	for n := 0; n+1 < len(points); n++ {
		lo, hi := points[n], points[n+1]
		if lo == hi {
			continue
		}
		for i < len(old) && old[i].Offset+old[i].Length <= lo {
			i++
		}
		for j < len(updates) && updates[j].Offset+updates[j].Length <= lo {
			j++
		}
		var x diskExtent
		if j < len(updates) && updates[j].Offset <= lo {
			x = updates[j]
		} else if i < len(old) && old[i].Offset <= lo {
			x = old[i]
		} else {
			continue
		}
		if x.Key != "" {
			x.Source += lo - x.Offset
		}
		x.Offset = lo
		x.Length = hi - lo
		if len(out) > 0 {
			p := &out[len(out)-1]
			if p.Offset+p.Length == lo && p.Key == x.Key && (x.Key == "" || p.Source+p.Length == x.Source) {
				p.Length += x.Length
				continue
			}
		}
		out = append(out, x)
	}
	return out
}
func qcowExtents(ctx context.Context, s sparse.Source, key string, size uint64) ([]diskExtent, error) {
	head := make([]byte, 104)
	if _, e := s.ReadAt(ctx, head, 0); e != nil {
		return nil, e
	}
	be := binary.BigEndian
	version := be.Uint32(head[4:8])
	bits := be.Uint32(head[20:24])
	if string(head[:4]) != "QFI\xfb" || (version != 2 && version != 3) || bits < 9 || bits > 21 || be.Uint64(head[24:32]) != size || be.Uint32(head[32:36]) != 0 {
		return nil, errors.New("unsupported QCOW snapshot")
	}
	if version == 3 && be.Uint64(head[72:80]) != 0 {
		return nil, errors.New("dirty or unsupported QCOW snapshot")
	}
	cluster := uint64(1) << bits
	per := cluster / 8
	l1n := uint64(be.Uint32(head[36:40]))
	l1off := be.Uint64(head[40:48])
	coverage := cluster * per
	if l1n > (size+coverage-1)/coverage || l1off > s.Size() || l1n*8 > s.Size()-l1off {
		return nil, errors.New("invalid QCOW L1 table")
	}
	l1 := make([]byte, l1n*8)
	if len(l1) > 0 {
		if _, e := s.ReadAt(ctx, l1, l1off); e != nil {
			return nil, e
		}
	}
	const mask = uint64(0x00fffffffffffe00)
	var out []diskExtent
	for i := uint64(0); i < l1n; i++ {
		l1entry := be.Uint64(l1[i*8 : i*8+8])
		if l1entry & ^(mask|uint64(1<<63)) != 0 {
			return nil, errors.New("invalid QCOW L1 flags")
		}
		l2off := l1entry & mask
		if l2off == 0 {
			continue
		}
		if l2off%cluster != 0 || l2off > s.Size() || cluster > s.Size()-l2off {
			return nil, errors.New("invalid QCOW L2 table")
		}
		l2 := make([]byte, cluster)
		if _, e := s.ReadAt(ctx, l2, l2off); e != nil {
			return nil, e
		}
		for j := uint64(0); j < per; j++ {
			pos := (i*per + j) * cluster
			if pos >= size {
				break
			}
			entry := be.Uint64(l2[j*8 : j*8+8])
			offset := entry & mask
			if entry & ^(mask|uint64(1<<63)|1) != 0 {
				return nil, errors.New("compressed QCOW snapshots are unsupported")
			}
			if offset == 0 && entry&1 == 0 {
				continue
			}
			x := diskExtent{Offset: pos, Length: min(cluster, size-pos)}
			if entry&1 == 0 {
				if offset%cluster != 0 || offset > s.Size() || x.Length > s.Size()-offset {
					return nil, errors.New("invalid QCOW data extent")
				}
				x.Key = key
				x.Source = offset
			}
			out = append(out, x)
		}
	}
	return out, nil
}
func composeDisk(cfg *manifest.Config, path string) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var spec diskCompose
	if e = json.Unmarshal(b, &spec); e != nil {
		return e
	}
	f, e := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
	if e != nil {
		return e
	}
	defer f.Close()
	ctx := context.Background()
	g := diskGraph{Version: 1, Size: spec.Size, Base: spec.Base.Manifest}
	if spec.Base.Kind == "manifest-disk-graph" {
		g, e = loadGraph(ctx, f, spec.Base.Manifest)
		if e != nil {
			return e
		}
		if g.Size != spec.Size {
			return errors.New("base disk size mismatch")
		}
	} else {
		base, e := openRef(ctx, f, g.Base)
		if e != nil {
			return e
		}
		size := base.Size()
		base.Close()
		if size != spec.Size {
			return errors.New("raw base size mismatch")
		}
	}
	for _, layer := range spec.Layers {
		s, e := openRef(ctx, f, layer.Manifest)
		if e != nil {
			return e
		}
		if s.Size() != layer.LogicalBytes {
			s.Close()
			return errors.New("QCOW payload size mismatch")
		}
		ext, e := qcowExtents(ctx, s, layer.Manifest, spec.Size)
		s.Close()
		if e != nil {
			return e
		}
		g.Extents = overlayExtents(g.Extents, ext)
	}
	if e = validateGraph(g); e != nil {
		return e
	}
	b, e = json.Marshal(g)
	if e != nil {
		return e
	}
	if len(b) > 64<<20 {
		return errors.New("disk map exceeds metadata limit")
	}
	ing, e := cfg.NewIngester(cfg.IngestKeyFunc(), nil)
	if e != nil {
		return e
	}
	defer ing.Close()
	result, e := ing.Ingest(ctx, sparse.Dense(bytes.NewReader(b), uint64(len(b))), manifest.IngestOption{})
	if e != nil {
		return e
	}
	objects := map[string]bool{g.Base: true}
	for _, x := range g.Extents {
		if x.Key != "" {
			objects[x.Key] = true
		}
	}
	keys := make([]string, 0, len(objects))
	for k := range objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if e = ownMemory(key, "protected"); e != nil {
			return e
		}
	}
	if e = ownMemory(manifest.HexKey(result.ManifestKey), "protected"); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"maintenance": maintenanceStats(), "manifest": manifest.HexKey(result.ManifestKey), "logical_bytes": g.Size, "stats": result, "extents": len(g.Extents), "objects": keys})
}

// One serialized broker connection owns this small LRU; the underlying fetch
// streams retain the existing bounded verified/decrypted-chunk caches.
type graphStream struct {
	graph         diskGraph
	fetcher       fetch.Fetcher
	streams       map[string]fetch.Stream
	order         []string
	cacheLimit    int
	metadataOpens uint64
}

func newGraphStream(ctx context.Context, f fetch.Fetcher, key string) (*graphStream, error) {
	g, e := loadGraph(ctx, f, key)
	if e != nil {
		return nil, e
	}
	return &graphStream{graph: g, fetcher: f, streams: make(map[string]fetch.Stream)}, nil
}
func (s *graphStream) Size() uint64 { return s.graph.Size }
func (s *graphStream) Close() error {
	var e error
	for _, v := range s.streams {
		e = errors.Join(e, v.Close())
	}
	return e
}
func (s *graphStream) stream(ctx context.Context, key string) (fetch.Stream, error) {
	if v, ok := s.streams[key]; ok {
		for i, k := range s.order {
			if k == key {
				s.order = append(append(s.order[:i], s.order[i+1:]...), key)
				break
			}
		}
		return v, nil
	}
	limit := s.cacheLimit
	if limit == 0 {
		limit = 4
	}
	if len(s.order) >= limit {
		k := s.order[0]
		if e := s.streams[k].Close(); e != nil {
			return nil, e
		}
		delete(s.streams, k)
		s.order = s.order[1:]
	}
	s.metadataOpens++
	v, e := openRef(ctx, s.fetcher, key)
	if e != nil {
		return nil, e
	}
	s.streams[key] = v
	s.order = append(s.order, key)
	return v, nil
}
func (s *graphStream) ReadAt(ctx context.Context, b []byte, offset uint64) (int, error) {
	if offset >= s.Size() {
		return 0, io.EOF
	}
	n := min(uint64(len(b)), s.Size()-offset)
	end := offset + n
	pos := offset
	i := sort.Search(len(s.graph.Extents), func(i int) bool { x := s.graph.Extents[i]; return x.Offset+x.Length > pos })
	for pos < end {
		key, source, limit := s.graph.Base, pos, end
		if i < len(s.graph.Extents) {
			x := s.graph.Extents[i]
			if pos >= x.Offset {
				key = x.Key
				source = x.Source + pos - x.Offset
				limit = min(end, x.Offset+x.Length)
				i++
			} else {
				limit = min(end, x.Offset)
			}
		}
		target := b[pos-offset : limit-offset]
		if key == "" {
			clear(target)
		} else {
			v, e := s.stream(ctx, key)
			if e != nil {
				return 0, e
			}
			got, e := v.ReadAt(ctx, target, source)
			if e != nil {
				return 0, e
			}
			if got != len(target) {
				return 0, io.ErrUnexpectedEOF
			}
		}
		pos = limit
	}
	if n < uint64(len(b)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

type graphRun struct {
	s          *graphStream
	start, end uint64
	kind       sparse.RunKind
}

func (r graphRun) Offset() uint64       { return r.start }
func (r graphRun) End() uint64          { return r.end }
func (r graphRun) Kind() sparse.RunKind { return r.kind }
func (r graphRun) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	if off > r.end-r.start || uint64(len(b)) > r.end-r.start-off {
		return 0, io.ErrUnexpectedEOF
	}
	return r.s.ReadAt(ctx, b, r.start+off)
}
func (s *graphStream) RunAt(off, limit uint64) (sparse.Run, error) {
	if off >= s.Size() {
		return nil, io.EOF
	}
	if limit == 0 {
		return nil, errors.New("zero run limit")
	}
	end := off + min(limit, s.Size()-off)
	key, source := s.graph.Base, off
	i := sort.Search(len(s.graph.Extents), func(i int) bool { x := s.graph.Extents[i]; return x.Offset+x.Length > off })
	if i < len(s.graph.Extents) {
		x := s.graph.Extents[i]
		if off >= x.Offset {
			key = x.Key
			source = x.Source + off - x.Offset
			end = min(end, x.Offset+x.Length)
		} else {
			end = min(end, x.Offset)
		}
	}
	kind := sparse.Zero
	if key != "" {
		stream, e := s.stream(context.Background(), key)
		if e != nil {
			return nil, e
		}
		r, e := stream.RunAt(source, end-off)
		if e != nil {
			return nil, e
		}
		if r == nil || r.Offset() != source || r.End() <= source {
			return nil, errors.New("invalid graph source run")
		}
		end = off + min(end-off, r.End()-source)
		kind = r.Kind()
	}
	return graphRun{s, off, end, kind}, nil
}
