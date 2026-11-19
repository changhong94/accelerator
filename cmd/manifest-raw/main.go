// SPDX-License-Identifier: Apache-2.0
// manifest-raw bridges immutable raw VM snapshot files and the existing
// authenticated Manifest/chunk pipeline. It never changes the source file.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("manifest-raw", flag.ContinueOnError)
	config := fs.String("manifest-config", "", "Manifest configuration (credentials must not be passed on the command line)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	a := fs.Args()
	if len(a) != 2 && len(a) != 3 {
		return errors.New("usage: manifest-raw --manifest-config FILE store RAW | load KEY OUTPUT | serve-pages KEY [SOCKET]")
	}
	cfg, err := manifest.LoadConfig(*config, "MANIFEST_CONFIG")
	if err != nil {
		return err
	}
	if a[0] == "memory-owner-init" {
		return initMemoryOwner(a[1], *config)
	}
	if a[0] == "maintain-memory" {
		return maintainMemory(cfg, *config, a[1] != "opportunistic")
	}
	if a[0] == "collect-memory" {
		return collectMemory(cfg, *config)
	}
	// A whole Kuasar publication also holds this shared lock across its commands.
	if a[0] == "store" || a[0] == "compose-memory" || a[0] == "compose-disk" {
		lock, e := maintenanceLock(false, false)
		if e != nil {
			return e
		}
		defer closeLock(lock)
	}
	if a[0] == "compose-memory" && len(a) == 2 {
		err := composeMemory(cfg, a[1])
		if err == nil {
			kickMaintenance(*config)
		}
		return err
	}
	if a[0] == "compose-disk" && len(a) == 2 {
		return composeDisk(cfg, a[1])
	}
	if a[0] == "serve-disks" && len(a) == 2 {
		return serveDisks(cfg, a[1])
	}
	if a[0] == "serve-pages" || a[0] == "serve-memory" {
		socket := ""
		if len(a) == 3 {
			socket = a[2]
		}
		return servePagesMode(cfg, a[1], socket, a[0] == "serve-memory")
	}
	ctx := context.Background()
	start := time.Now()
	if a[0] == "store" && len(a) == 2 {
		gate, err := foregroundMemoryIO()
		if err != nil {
			return err
		}
		defer closeLock(gate)
		f, err := os.Open(a[1])
		if err != nil {
			return err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() {
			return errors.New("source must be an immutable regular snapshot file")
		}
		holes, err := sparse.ProbeHoles(f)
		if err != nil {
			return err
		}
		src, err := sparse.NewSource(f, uint64(st.Size()), holes)
		if err != nil {
			return err
		}
		ing, err := cfg.NewIngester(cfg.IngestKeyFunc(), nil)
		if err != nil {
			return err
		}
		defer ing.Close()
		result, err := ing.Ingest(ctx, src, manifest.IngestOption{})
		if err != nil {
			return err
		}
		if filepath.Base(a[1]) == "memory-ranges" || filepath.Base(a[1]) == "memory-ranges.delta" {
			if err = ownMemory(manifest.HexKey(result.ManifestKey), "raw"); err != nil {
				return err
			}
		} else {
			if err = ownMemory(manifest.HexKey(result.ManifestKey), "protected"); err != nil {
				return err
			}
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"maintenance": maintenanceStats(), "manifest": manifest.HexKey(result.ManifestKey), "logical_bytes": st.Size(), "stats": result, "elapsed_ms": float64(time.Since(start).Nanoseconds()) / 1e6})
	}
	if !(((a[0] == "load" || a[0] == "load-disk" || a[0] == "load-memory") && len(a) == 3) || ((a[0] == "inspect" || a[0] == "inspect-disk" || a[0] == "inspect-memory") && len(a) == 2)) {
		return errors.New("invalid command")
	}
	key, err := manifest.ParseKeyRef(a[1])
	if err != nil {
		return err
	}
	fetcher, err := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
	if err != nil {
		return err
	}
	defer fetcher.Close()
	var src interface {
		sparse.Source
		io.Closer
	}
	if a[0] == "load-disk" || a[0] == "load-memory" || a[0] == "inspect-disk" || a[0] == "inspect-memory" {
		src, err = newGraphStream(ctx, fetcher, a[1])
	} else {
		src, err = fetcher.OpenManifest(ctx, key)
	}
	if err != nil {
		return err
	}
	defer src.Close()
	if a[0] == "inspect" || a[0] == "inspect-disk" || a[0] == "inspect-memory" {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"logical_bytes": src.Size()})
	}
	phases := &loadTimings{}
	phases.OpenMS = float64(time.Since(start).Nanoseconds()) / 1e6
	n, err := materialize(ctx, src, a[2], phases)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"logical_bytes": src.Size(), "materialized_data_bytes": n, "phases_ms": phases, "elapsed_ms": float64(time.Since(start).Nanoseconds()) / 1e6})
}

// materialize preserves holes and verifies all fetched content through Stream.
// The destination is published only after data is synced. Link provides
// no-replace atomic publication; existing templates are never overwritten.
type loadTimings struct {
	OpenMS    float64 `json:"open"`
	ReadMS    float64 `json:"fetch_decode_verify"`
	WriteMS   float64 `json:"write"`
	SyncMS    float64 `json:"file_sync"`
	PublishMS float64 `json:"publish_directory_sync"`
}

func materialize(ctx context.Context, src sparse.Source, destination string, timings ...*loadTimings) (uint64, error) {
	phases := &loadTimings{}
	if len(timings) > 0 {
		phases = timings[0]
	}
	f, err := os.CreateTemp(filepath.Dir(destination), ".manifest-raw-")
	if err != nil {
		return 0, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if src.Size() > uint64(1<<63-1) {
		return 0, errors.New("image too large")
	}
	if err = f.Truncate(int64(src.Size())); err != nil {
		return 0, err
	}
	buffer := make([]byte, 1<<20)
	var copied uint64
	for off := uint64(0); off < src.Size(); {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		run, err := src.RunAt(off, src.Size()-off)
		if err != nil {
			return 0, err
		}
		if run == nil || run.Offset() > off || run.End() <= off || run.End() > src.Size() {
			return 0, errors.New("invalid sparse run")
		}
		if run.Kind() == sparse.Data {
			for pos := off; pos < run.End(); {
				b := buffer[:min(uint64(len(buffer)), run.End()-pos)]
				readStart := time.Now()
				n, err := run.ReadAt(ctx, b, pos-run.Offset())
				phases.ReadMS += float64(time.Since(readStart).Nanoseconds()) / 1e6
				if err != nil {
					return 0, err
				}
				if n != len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				writeStart := time.Now()
				if _, err = f.WriteAt(b, int64(pos)); err != nil {
					return 0, err
				}
				phases.WriteMS += float64(time.Since(writeStart).Nanoseconds()) / 1e6
				pos += uint64(n)
				copied += uint64(n)
			}
		} else if run.Kind() != sparse.Hole && run.Kind() != sparse.Zero {
			return 0, errors.New("unknown sparse run kind")
		}
		off = run.End()
	}
	if err = f.Chmod(0400); err != nil {
		return 0, err
	}
	syncStart := time.Now()
	if err = f.Sync(); err != nil {
		return 0, err
	}
	phases.SyncMS = float64(time.Since(syncStart).Nanoseconds()) / 1e6
	publishStart := time.Now()
	if err = os.Link(f.Name(), destination); err != nil {
		return 0, err
	}
	d, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return 0, err
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return 0, err
	}
	phases.PublishMS = float64(time.Since(publishStart).Nanoseconds()) / 1e6
	return copied, nil
}
