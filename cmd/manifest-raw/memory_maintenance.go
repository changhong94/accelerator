// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type memoryOwner struct {
	Store  string `json:"store"`
	State  string `json:"state"`
	Config string `json:"config"`
}

func maintenanceDir() string { return os.Getenv("KUASAR_MEMORY_MAINTENANCE") }

var metadataSyncUS atomic.Uint64
var metadataUpdates atomic.Uint64

func maintenanceStats() map[string]uint64 {
	return map[string]uint64{"sync_us": metadataSyncUS.Load(), "updates": metadataUpdates.Load()}
}
func tunedMemory() bool { return os.Getenv("KUASAR_MEMORY_TUNING") != "0" }
func atomicJSON(path string, v any) error {
	started := time.Now()
	defer func() { metadataSyncUS.Add(uint64(time.Since(started).Microseconds())); metadataUpdates.Add(1) }()
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func maintenanceLock(exclusive, nonblock bool) (*os.File, error) {
	dir := maintenanceDir()
	if dir == "" {
		return nil, nil
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(filepath.Join(dir, "transactions.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	flags := syscall.LOCK_SH
	if exclusive {
		flags = syscall.LOCK_EX
	}
	if nonblock {
		flags |= syscall.LOCK_NB
	}
	if e = syscall.Flock(int(f.Fd()), flags); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}
func closeLock(f *os.File) {
	if f != nil {
		f.Close()
	}
}
func ownMemory(key, kind string) error {
	if maintenanceDir() == "" {
		return nil
	}
	if _, e := manifest.ParseKeyRef(key); e != nil {
		return e
	}
	directory := "owned"
	if kind == "protected" {
		directory = "protected"
	}
	return atomicJSON(filepath.Join(maintenanceDir(), directory, key), map[string]string{"kind": kind})
}
func completedMemory(key string) (diskRef, bool, error) {
	var r diskRef
	if maintenanceDir() == "" {
		return r, false, nil
	}
	b, e := os.ReadFile(filepath.Join(maintenanceDir(), "complete", key))
	if os.IsNotExist(e) {
		return r, false, nil
	}
	if e != nil {
		return r, false, e
	}
	e = json.Unmarshal(b, &r)
	if e != nil {
		return r, false, e
	}
	_, e = manifest.ParseKeyRef(r.Manifest)
	if r.Kind != "raw" || r.LogicalBytes == 0 {
		return r, false, errors.New("invalid completed memory base")
	}
	return r, e == nil, e
}
func threshold(name string, def uint64) uint64 {
	n, e := strconv.ParseUint(os.Getenv(name), 10, 64)
	if e != nil || n == 0 {
		return def
	}
	return n
}
func shouldCompact(g diskGraph) bool {
	refs := map[string]bool{g.Base: true}
	var covered uint64
	for _, x := range g.Extents {
		covered += x.Length
		if x.Key != "" {
			refs[x.Key] = true
		}
	}
	return uint64(len(refs)) >= threshold("KUASAR_MEMORY_MAX_OBJECTS", 8) || uint64(len(g.Extents)) >= threshold("KUASAR_MEMORY_MAX_EXTENTS", 8192) || covered*100/g.Size >= threshold("KUASAR_MEMORY_MAX_PERCENT", 50)
}
func queueMemory(key string, g diskGraph) error {
	if maintenanceDir() == "" || !shouldCompact(g) {
		return nil
	}
	entries, _ := os.ReadDir(filepath.Join(maintenanceDir(), "jobs"))
	for _, entry := range entries {
		b, e := os.ReadFile(filepath.Join(maintenanceDir(), "jobs", entry.Name()))
		if e != nil {
			continue
		}
		var job struct {
			Base string `json:"base"`
		}
		if json.Unmarshal(b, &job) == nil && job.Base == g.Base {
			return nil
		}
	}
	return atomicJSON(filepath.Join(maintenanceDir(), "jobs", key), map[string]any{"manifest": key, "kind": "manifest-memory-graph", "logical_bytes": g.Size, "base": g.Base})
}
func kickMaintenance(config string) {
	if maintenanceDir() == "" {
		return
	}
	exe, e := os.Executable()
	if e != nil {
		return
	}
	log, e := os.OpenFile(filepath.Join(maintenanceDir(), "worker.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return
	}
	defer log.Close()
	cmd := exec.Command(exe, "--manifest-config", config, "maintain-memory", "opportunistic")
	cmd.Stdin = nil
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if cmd.Start() == nil {
		go cmd.Wait()
	} // Worker owns its process lock; duplicate starts exit immediately.
}

// The optional I/O scheduling gate is separate from the transaction/GC lock.
// Publishers share it; compaction checks it before each bounded source read.
// Release it before fetching: foreground must never wait for background RPC I/O.
func memoryIOGate() (*os.File, error) {
	if maintenanceDir() == "" || !tunedMemory() || os.Getenv("KUASAR_MEMORY_IO_GATE") == "0" {
		return nil, nil
	}
	return os.OpenFile(filepath.Join(maintenanceDir(), "io.lock"), os.O_CREATE|os.O_RDWR, 0600)
}
func foregroundMemoryIO() (*os.File, error) {
	f, e := memoryIOGate()
	if e != nil || f == nil {
		return f, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_SH); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}

type countedMemory struct {
	sparse.Source
	bytes  uint64
	readUS uint64
	gate   *os.File
	gateUS uint64
}

func (c *countedMemory) acquireIO() error {
	if c.gate == nil {
		return nil
	}
	start := time.Now()
	e := syscall.Flock(int(c.gate.Fd()), syscall.LOCK_EX)
	c.gateUS += uint64(time.Since(start).Microseconds())
	return e
}
func (c *countedMemory) releaseIO() {
	if c.gate != nil {
		syscall.Flock(int(c.gate.Fd()), syscall.LOCK_UN)
	}
}
func (c *countedMemory) readBounded(ctx context.Context, p []byte, off uint64, read func(context.Context, []byte, uint64) (int, error)) (int, error) {
	total := 0
	for total < len(p) {
		end := min(total+64*1024, len(p))
		if e := c.acquireIO(); e != nil {
			return total, e
		}
		// Only the admission check holds the gate. An already admitted read
		// may overlap a new publisher, but no further reads enter until it finishes.
		c.releaseIO()
		start := time.Now()
		n, e := read(ctx, p[total:end], off+uint64(total))
		c.readUS += uint64(time.Since(start).Microseconds())
		c.bytes += uint64(n)
		total += n
		if e != nil {
			return total, e
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}
func (c *countedMemory) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	return c.readBounded(ctx, p, off, c.Source.ReadAt)
}

type countedRun struct {
	sparse.Run
	owner *countedMemory
}

func (r countedRun) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	return r.owner.readBounded(ctx, p, off, r.Run.ReadAt)
}

func (c *countedMemory) RunAt(off, n uint64) (sparse.Run, error) {
	r, e := c.Source.RunAt(off, n)
	if e != nil {
		return nil, e
	}
	return countedRun{r, c}, nil
}

// Bound background concurrency independently of the foreground Store pool.
func memoryMergeWorkers() (int, error) {
	v := os.Getenv("KUASAR_MEMORY_MERGE_WORKERS")
	if v == "" {
		return 2, nil
	}
	n, e := strconv.Atoi(v)
	if e != nil || n < 1 || n > 4 {
		return 0, errors.New("KUASAR_MEMORY_MERGE_WORKERS must be between 1 and 4")
	}
	return n, nil
}
func compactMemory(cfg *manifest.Config, key string) error {
	start := time.Now()
	lock, e := maintenanceLock(false, false)
	if e != nil {
		return e
	}
	defer closeLock(lock)
	lockUS := time.Since(start).Microseconds()

	f, e := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
	if e != nil {
		return e
	}
	defer f.Close()
	s, e := newGraphStream(context.Background(), f, key)
	if e != nil {
		return e
	}
	defer s.Close()
	// Keep a bounded set of authenticated readers for a sequential compaction.
	// The foreground pager retains its existing small cache.
	if tunedMemory() {
		s.cacheLimit = 16
	}
	if ready, ok, e := completedMemory(key); e != nil {
		return e
	} else if ok {
		return atomicJSON(filepath.Join(maintenanceDir(), "catalog", s.graph.Base), memoryCatalog{Input: key, Output: ready})
	}
	gate, e := memoryIOGate()
	if e != nil {
		return e
	}
	defer closeLock(gate)
	src := &countedMemory{Source: s, gate: gate}
	// Bound background encryption and Store writes independently of foreground
	// publication. Backpressure bounds queued work; all durable acknowledgements
	// are still required before the completed root is published.
	background := *cfg
	if tunedMemory() {
		background.Store.Pool, e = memoryMergeWorkers()
		if e != nil {
			return e
		}
	}
	ing, e := background.NewIngester(background.IngestKeyFunc(), nil)
	if e != nil {
		return e
	}
	defer ing.Close()
	ingestStart := time.Now()
	result, e := ing.Ingest(context.Background(), src, manifest.IngestOption{})
	ingestUS := time.Since(ingestStart).Microseconds()
	commitStart := time.Now()
	if e != nil {
		return e
	}
	ref := diskRef{Manifest: manifest.HexKey(result.ManifestKey), LogicalBytes: s.Size(), Kind: "raw"}
	if e = ownMemory(ref.Manifest, "raw"); e != nil {
		return e
	}
	// Failure injection is opt-in to an isolated test directory. Never publish a
	// completion marker before the Store has acknowledged all immutable bytes.
	fail := os.Getenv("KUASAR_MEMORY_FAIL_ONCE")
	if fail != "" {
		if e = os.Remove(fail); e == nil {
			return errors.New("injected failure before compaction commit")
		}
	}
	if e = atomicJSON(filepath.Join(maintenanceDir(), "complete", key), ref); e != nil {
		return e
	}
	if e = atomicJSON(filepath.Join(maintenanceDir(), "catalog", s.graph.Base), memoryCatalog{Input: key, Output: ref}); e != nil {
		return e
	}
	fmt.Printf("MEMORY_COMPACT key=%s output=%s read_bytes=%d stored_bytes=%d elapsed_ms=%.3f metadata_opens=%d workers=%d lock_us=%d ingest_us=%d source_read_us=%d commit_us=%d io_wait_us=%d\n", key, ref.Manifest, src.bytes, result.StoredBytes, float64(time.Since(start).Microseconds())/1000, s.metadataOpens, background.Store.Pool, lockUS, ingestUS, src.readUS, time.Since(commitStart).Microseconds(), src.gateUS)
	return nil
}
func maintainMemory(cfg *manifest.Config, config string, forceGC bool) (retErr error) {
	dir := maintenanceDir()
	if dir == "" {
		return errors.New("memory maintenance is not configured")
	}
	worker, e := os.OpenFile(filepath.Join(dir, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	if e = syscall.Flock(int(worker.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		worker.Close()
		if errors.Is(e, syscall.EWOULDBLOCK) {
			return nil
		}
		return e
	}
	defer func() {
		worker.Close()
		if retErr == nil {
			jobs, _ := os.ReadDir(filepath.Join(dir, "jobs"))
			for _, j := range jobs {
				if len(j.Name()) == 64 {
					kickMaintenance(config)
					break
				}
			}
		}
	}()
	didWork := false
	// Keep scanning for jobs that arrived while the previous job was running.
	for {
		jobs, e := os.ReadDir(filepath.Join(dir, "jobs"))
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if len(jobs) == 0 {
			break
		}
		processed := 0
		for _, j := range jobs {
			if j.IsDir() || len(j.Name()) != 64 {
				continue
			}
			processed++
			didWork = true
			for attempt := 0; attempt < 3; attempt++ {
				e = compactMemory(cfg, j.Name())
				if e == nil {
					break
				}
				fmt.Printf("MEMORY_COMPACT_RETRY key=%s attempt=%d error=%v\n", j.Name(), attempt+1, e)
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
			}
			if e != nil {
				return e
			}
			if e = os.Remove(filepath.Join(dir, "jobs", j.Name())); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
		if processed == 0 {
			break
		}
	}
	if !didWork && !forceGC {
		return nil
	}
	for i := 0; i < 20; i++ {
		e := collectMemory(cfg, config)
		if !errors.Is(e, syscall.EWOULDBLOCK) {
			return e
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// The collector is deliberately opt-in and restricted to a dedicated FS
// checkpoint Store registered when empty. It never purges a shared generation.
func initMemoryOwner(path, config string) error {
	var o memoryOwner
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(b, &o); e != nil {
		return e
	}
	for _, p := range []string{o.Store, o.State} {
		if !filepath.IsAbs(p) {
			return errors.New("maintenance owner paths must be absolute")
		}
		r, e := filepath.EvalSymlinks(p)
		if e != nil || r != p {
			return errors.New("maintenance owner paths must be canonical directories")
		}
	}
	for _, partition := range []string{"manifest", "chunk", "blob"} {
		found := false
		_ = filepath.WalkDir(filepath.Join(o.Store, partition), func(_ string, d os.DirEntry, e error) error {
			if e == nil && !d.IsDir() {
				found = true
			}
			return nil
		})
		if found {
			return errors.New("initialize an empty dedicated Store before ingest")
		}
	}
	c, e := os.ReadFile(config)
	if e != nil {
		return e
	}
	o.Config = fmt.Sprintf("%x", sha256.Sum256(c))
	if maintenanceDir() == "" {
		return errors.New("missing maintenance directory")
	}
	owner := filepath.Join(maintenanceDir(), "owner.json")
	if _, e = os.Stat(owner); !os.IsNotExist(e) {
		return errors.New("maintenance owner already exists")
	}
	return atomicJSON(owner, o)
}
func collectMemory(cfg *manifest.Config, config string) error {
	dir := maintenanceDir()
	b, e := os.ReadFile(filepath.Join(dir, "owner.json"))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	var o memoryOwner
	if e = json.Unmarshal(b, &o); e != nil {
		return e
	}
	c, e := os.ReadFile(config)
	if e != nil {
		return e
	}
	if o.Config != fmt.Sprintf("%x", sha256.Sum256(c)) {
		return errors.New("maintenance config identity changed")
	}
	for _, p := range []string{o.Store, o.State} {
		canonical, err := filepath.EvalSymlinks(p)
		if err != nil || canonical != p {
			return errors.New("maintenance owner path changed")
		}
		st, err := os.Stat(p)
		if err != nil || !st.IsDir() {
			return errors.New("maintenance owner is not a directory")
		}
	}
	lock, e := maintenanceLock(true, true)
	if errors.Is(e, syscall.EWOULDBLOCK) {
		fmt.Println("MEMORY_GC deferred=active_transaction")
		return syscall.EWOULDBLOCK
	}
	if e != nil {
		return e
	}
	defer closeLock(lock)
	start := time.Now()
	f, e := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
	if e != nil {
		return e
	}
	defer f.Close()
	live := map[string]bool{}
	graphs := map[string]bool{}
	bases := map[string]bool{}
	var mark func(diskRef) error
	mark = func(r diskRef) error {
		if _, e := manifest.ParseKeyRef(r.Manifest); e != nil {
			return e
		}
		if live[r.Manifest] {
			return nil
		}
		live[r.Manifest] = true
		if r.Kind == "manifest-memory-graph" {
			graphs[r.Manifest] = true
			g, e := loadGraph(context.Background(), f, r.Manifest)
			if e != nil {
				return e
			}
			bases[g.Base] = true
			if c, ok, e := catalogMemory(g.Base); e != nil {
				return e
			} else if ok {
				if e = mark(diskRef{Manifest: c.Input, Kind: "manifest-memory-graph"}); e != nil {
					return e
				}
				if e = mark(c.Output); e != nil {
					return e
				}
			}
			if e = mark(diskRef{Manifest: g.Base, Kind: "raw"}); e != nil {
				return e
			}
			for _, x := range g.Extents {
				if x.Key != "" {
					if e = mark(diskRef{Manifest: x.Key, Kind: "raw"}); e != nil {
						return e
					}
				}
			}
			if ref, ok, e := completedMemory(r.Manifest); e != nil {
				return e
			} else if ok {
				if e = mark(ref); e != nil {
					return e
				}
			}
		} else if r.Kind != "raw" && r.Kind != "" {
			return errors.New("unknown memory root kind")
		}
		return nil
	}
	protected, e := os.ReadDir(filepath.Join(dir, "protected"))
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	for _, p := range protected {
		if strings.HasPrefix(p.Name(), ".pending-") {
			continue
		}
		if e = mark(diskRef{Manifest: p.Name(), Kind: "raw"}); e != nil {
			return e
		}
	}
	e = filepath.WalkDir(o.State, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			if d.Name() == "memory-roots.json" {
				return errors.New("symlink memory root is not allowed")
			}
			return nil
		}
		if d.Name() != "memory-roots.json" {
			return nil
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		var roots map[string]diskRef
		if e = json.Unmarshal(b, &roots); e != nil {
			return e
		}
		if len(roots) == 0 {
			return errors.New("empty memory root registry is invalid")
		}
		for _, r := range roots {
			if e = mark(r); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	// Explicit retained roots cover exported snapshots, independently of a VM.
	for _, sub := range []string{"retained", "jobs"} {
		entries, e := os.ReadDir(filepath.Join(dir, sub))
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		for _, p := range entries {
			if strings.HasPrefix(p.Name(), ".pending-") {
				continue
			}
			if p.IsDir() {
				return errors.New("unexpected root directory")
			}
			b, e := os.ReadFile(filepath.Join(dir, sub, p.Name()))
			if e != nil {
				return e
			}
			var r diskRef
			if e = json.Unmarshal(b, &r); e != nil {
				return e
			}
			if e = mark(r); e != nil {
				return e
			}
		}
	}
	owned := map[string]bool{}
	es, e := os.ReadDir(filepath.Join(dir, "owned"))
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	for _, p := range es {
		if strings.HasPrefix(p.Name(), ".pending-") {
			continue
		}
		if _, e = manifest.ParseKeyRef(p.Name()); e != nil {
			return e
		}
		owned[p.Name()] = true
	}
	chunks := map[string]bool{}
	seenManifests := map[string]bool{}
	var dead []string
	var reclaimed int64
	// Preserve every unowned manifest, including disk/config/state manifests.
	e = filepath.WalkDir(filepath.Join(o.Store, "manifest"), func(p string, d os.DirEntry, e error) error {
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errors.New("nonregular Store object")
		}
		key := d.Name()
		seenManifests[key] = true
		if _, e = manifest.ParseKeyRef(key); e != nil {
			return e
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		if fmt.Sprintf("%x", sha256.Sum256(b)) != key {
			return errors.New("Store manifest digest mismatch")
		}
		if owned[key] && !live[key] {
			dead = append(dead, p)
			return nil
		}
		m, _, e := manifest.Unmarshal(b)
		if e != nil {
			return e
		}
		for _, x := range m.Entries {
			if !x.IsZero {
				chunks[hex.EncodeToString(x.CiphertextHash[:])] = true
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	for key := range live {
		if !seenManifests[key] {
			return fmt.Errorf("missing live memory manifest %s", key)
		}
	}
	e = filepath.WalkDir(filepath.Join(o.Store, "chunk"), func(p string, d os.DirEntry, e error) error {
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errors.New("nonregular Store chunk")
		}
		if _, e = manifest.ParseKeyRef(d.Name()); e != nil {
			return e
		}
		if !chunks[d.Name()] {
			dead = append(dead, p)
		}
		return nil
	})
	if e != nil {
		return e
	}
	// All roots and manifests have validated before any removal. An interrupted
	// sweep is idempotent: it has only removed unreachable immutable objects.
	parents := map[string]bool{}
	for _, p := range dead {
		st, e := os.Stat(p)
		if e != nil {
			return e
		}
		if e = os.Remove(p); e != nil {
			return e
		}
		reclaimed += st.Size()
		parents[filepath.Dir(p)] = true
	}
	for p := range parents {
		d, e := os.Open(p)
		if e != nil {
			return e
		}
		e = d.Sync()
		d.Close()
		if e != nil {
			return e
		}
	}
	// Drop only cache entries whose input is no longer a registered live graph.
	es, e = os.ReadDir(filepath.Join(dir, "complete"))
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	for _, p := range es {
		if !graphs[p.Name()] {
			if e = os.Remove(filepath.Join(dir, "complete", p.Name())); e != nil {
				return e
			}
		}
	}
	es, e = os.ReadDir(filepath.Join(dir, "catalog"))
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	for _, p := range es {
		if !bases[p.Name()] {
			if e = os.Remove(filepath.Join(dir, "catalog", p.Name())); e != nil {
				return e
			}
		}
	}
	for key := range owned {
		if !live[key] {
			if e = os.Remove(filepath.Join(dir, "owned", key)); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
	}
	fmt.Printf("MEMORY_GC objects=%d reclaimed_bytes=%d elapsed_ms=%.3f\n", len(dead), reclaimed, float64(time.Since(start).Microseconds())/1000)
	return nil
}

// A completed ancestor can be adopted even if the VM has already saved newer
// deltas. Compare immutable extent maps, retaining only mappings that differ;
// no RAM payload is read and divergent siblings are handled by the same rule.
type memoryCatalog struct {
	Input  string  `json:"input"`
	Output diskRef `json:"output"`
}

func catalogMemory(base string) (memoryCatalog, bool, error) {
	var c memoryCatalog
	b, e := os.ReadFile(filepath.Join(maintenanceDir(), "catalog", base))
	if os.IsNotExist(e) {
		return c, false, nil
	}
	if e != nil {
		return c, false, e
	}
	e = json.Unmarshal(b, &c)
	return c, e == nil, e
}
func mappingAt(g diskGraph, off uint64) (string, uint64) {
	i := sort.Search(len(g.Extents), func(i int) bool { x := g.Extents[i]; return x.Offset+x.Length > off })
	if i < len(g.Extents) && off >= g.Extents[i].Offset {
		x := g.Extents[i]
		return x.Key, x.Source + off - x.Offset
	}
	return g.Base, off
}
func rebaseMemory(g, old diskGraph, base diskRef) (diskGraph, error) {
	if g.Size != old.Size || g.Base != old.Base || base.LogicalBytes != g.Size || base.Kind != "raw" {
		return g, errors.New("incompatible compaction baseline")
	}
	bounds := []uint64{0, g.Size}
	for _, m := range []diskGraph{g, old} {
		for _, x := range m.Extents {
			bounds = append(bounds, x.Offset, x.Offset+x.Length)
		}
	}
	sort.Slice(bounds, func(i, j int) bool { return bounds[i] < bounds[j] })
	result := diskGraph{Version: 1, Size: g.Size, Base: base.Manifest}
	for i := 0; i+1 < len(bounds); i++ {
		off, end := bounds[i], bounds[i+1]
		if off == end {
			continue
		}
		k, p := mappingAt(g, off)
		ok, op := mappingAt(old, off)
		if k == ok && (k == "" || p == op) {
			continue
		}
		if k == "" {
			p = 0
		}
		result.Extents = append(result.Extents, diskExtent{Offset: off, Length: end - off, Key: k, Source: p})
	}
	result.Extents = overlayExtents(nil, result.Extents)
	return result, validateGraph(result)
}
