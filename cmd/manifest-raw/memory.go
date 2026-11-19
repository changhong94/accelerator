// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// A memory delta describes writes, including writes of zero. Holes outside the
// declared ranges inherit the base; holes inside them replace it with zero.
// The composed map is flat: repeated saves do not introduce recursive layers.
type memoryDelta struct {
	Version int         `json:"version"`
	Size    uint64      `json:"size"`
	Base    diskRef     `json:"base"`
	Delta   diskRef     `json:"delta"`
	Ranges  [][2]uint64 `json:"ranges"`
}

func composeMemoryMap(ctx context.Context, f fetch.Fetcher, spec memoryDelta) (diskGraph, error) {
	g := diskGraph{Version: 1, Size: spec.Size, Base: spec.Base.Manifest}
	if spec.Version != 1 || spec.Size == 0 || spec.Size%4096 != 0 || spec.Base.LogicalBytes != spec.Size || spec.Delta.LogicalBytes != spec.Size {
		return g, errors.New("invalid memory delta layout")
	}
	if spec.Base.Kind == "manifest-memory-graph" {
		var err error
		g, err = loadGraph(ctx, f, spec.Base.Manifest)
		if err != nil {
			return g, err
		}
		if g.Size != spec.Size {
			return g, errors.New("memory base size mismatch")
		}
	} else if spec.Base.Kind == "raw" {
		base, err := openRef(ctx, f, g.Base)
		if err != nil {
			return g, err
		}
		size := base.Size()
		base.Close()
		if size != spec.Size {
			return g, errors.New("memory base size mismatch")
		}
	} else {
		return g, errors.New("unsupported memory base kind")
	}
	delta, err := openRef(ctx, f, spec.Delta.Manifest)
	if err != nil {
		return g, err
	}
	defer delta.Close()
	if delta.Size() != spec.Size {
		return g, errors.New("memory delta size mismatch")
	}
	var updates []diskExtent
	end := uint64(0)
	for _, r := range spec.Ranges {
		off, n := r[0], r[1]
		if off < end || n == 0 || off%4096 != 0 || n%4096 != 0 || off > spec.Size || n > spec.Size-off {
			return g, errors.New("invalid dirty memory range")
		}
		end = off + n
		for off < end {
			run, e := delta.RunAt(off, end-off)
			if e != nil {
				return g, e
			}
			if run == nil || run.Offset() > off || run.End() <= off {
				return g, errors.New("invalid delta extent")
			}
			hi := run.End()
			if hi > end {
				hi = end
			}
			x := diskExtent{Offset: off, Length: hi - off, Key: spec.Delta.Manifest, Source: off}
			if run.Kind() == sparse.Zero || run.Kind() == sparse.Hole {
				x.Key = ""
				x.Source = 0
			}
			updates = append(updates, x)
			off = hi
		}
	}
	g.Extents = overlayExtents(g.Extents, updates)
	return g, validateGraph(g)
}

func composeMemory(cfg *manifest.Config, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var spec memoryDelta
	if err = json.Unmarshal(b, &spec); err != nil {
		return err
	}
	// A completed immutable base is byte-identical to the old graph. The
	// capture's dirty ranges still apply, including explicit zero replacement.
	if spec.Base.Kind == "manifest-memory-graph" {
		if base, ok, e := completedMemory(spec.Base.Manifest); e != nil {
			return e
		} else if ok {
			if base.LogicalBytes != spec.Size {
				return errors.New("compacted baseline size mismatch")
			}
			spec.Base = base
		}
	}
	f, err := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
	if err != nil {
		return err
	}
	defer f.Close()
	ctx := context.Background()
	g, err := composeMemoryMap(ctx, f, spec)
	if err != nil {
		return err
	}
	if maintenanceDir() != "" {
		if c, ok, e := catalogMemory(g.Base); e != nil {
			return e
		} else if ok {
			old, e := loadGraph(ctx, f, c.Input)
			if e != nil {
				return e
			}
			g, e = rebaseMemory(g, old, c.Output)
			if e != nil {
				return e
			}
		}
	}
	b, err = json.Marshal(g)
	if err != nil {
		return err
	}
	if len(b) > 64<<20 {
		return errors.New("memory map exceeds metadata limit")
	}
	ing, err := cfg.NewIngester(cfg.IngestKeyFunc(), nil)
	if err != nil {
		return err
	}
	defer ing.Close()
	result, err := ing.Ingest(ctx, sparse.Dense(bytes.NewReader(b), uint64(len(b))), manifest.IngestOption{})
	if err != nil {
		return err
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
	key := manifest.HexKey(result.ManifestKey)
	if err = ownMemory(key, "manifest-memory-graph"); err != nil {
		return err
	}
	if err = queueMemory(key, g); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"maintenance": maintenanceStats(), "manifest": manifest.HexKey(result.ManifestKey), "logical_bytes": g.Size, "stats": result, "objects": keys, "extents": len(g.Extents), "maintenance_queued": maintenanceDir() != "" && shouldCompact(g)})
}
