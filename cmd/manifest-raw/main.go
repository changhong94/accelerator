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
		return errors.New("usage: manifest-raw --manifest-config FILE store RAW | load KEY OUTPUT")
	}
	cfg, err := manifest.LoadConfig(*config, "MANIFEST_CONFIG")
	if err != nil {
		return err
	}
	ctx := context.Background()
	start := time.Now()
	if a[0] == "store" && len(a) == 2 {
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
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"manifest": manifest.HexKey(result.ManifestKey), "logical_bytes": st.Size(), "stats": result, "elapsed_ms": float64(time.Since(start).Nanoseconds()) / 1e6})
	}
	if a[0] != "load" || len(a) != 3 {
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
	src, err := fetcher.OpenManifest(ctx, key)
	if err != nil {
		return err
	}
	defer src.Close()
	n, err := materialize(ctx, src, a[2])
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"logical_bytes": src.Size(), "materialized_data_bytes": n, "elapsed_ms": float64(time.Since(start).Nanoseconds()) / 1e6})
}

// materialize preserves holes and verifies all fetched content through Stream.
// The destination is published only after data is synced. Link provides
// no-replace atomic publication; existing templates are never overwritten.
func materialize(ctx context.Context, src sparse.Source, destination string) (uint64, error) {
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
				n, err := run.ReadAt(ctx, b, pos-run.Offset())
				if err != nil {
					return 0, err
				}
				if n != len(b) {
					return 0, io.ErrUnexpectedEOF
				}
				if _, err = f.WriteAt(b, int64(pos)); err != nil {
					return 0, err
				}
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
	if err = f.Sync(); err != nil {
		return 0, err
	}
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
	return copied, nil
}
