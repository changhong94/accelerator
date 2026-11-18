//go:build !no_rocksdb

package rocks

// #include "rocksdb/c.h"
import "C"

import (
	"fmt"
	grocksdb "github.com/linxGnu/grocksdb"
	"runtime"
	"sync/atomic"
)

// Counters classify successful chunk payloads, not index/filter cache hits.
// Bytes are the encoded/encrypted object bytes returned to the cache client.
// Inline values with file IO cannot be attributed precisely and remain unknown.
// File IO means a RocksDB file read, not necessarily a physical device read.
type sourceCounters struct {
	memoryRequests  atomic.Uint64
	memoryBytes     atomic.Uint64
	fileRequests    atomic.Uint64
	fileBytes       atomic.Uint64
	unknownRequests atomic.Uint64
	unknownBytes    atomic.Uint64
}

func (s *impl) observedGet(ro *grocksdb.ReadOptions, cf *grocksdb.ColumnFamilyHandle, key []byte) (*grocksdb.Slice, error) {
	// RocksDB PerfContext is OS-thread-local. Pin only this one Get and its
	// counter capture; do not substitute process-wide deltas under concurrency.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	grocksdb.SetPerfLevel(grocksdb.KEnableCount)
	defer grocksdb.SetPerfLevel(grocksdb.KDisable)
	p := grocksdb.NewPerfContext()
	defer p.Destroy()
	p.Reset()
	value, err := s.db.GetCF(ro, cf, key)
	if err != nil || value.Size() == 0 {
		return value, err
	}
	bytes := uint64(value.Size())
	blobReads := p.Metric(int(C.rocksdb_blob_read_count))
	blobHits := p.Metric(int(C.rocksdb_blob_cache_hit_count))
	blockReads := p.Metric(int(C.rocksdb_block_read_count))
	switch {
	case blobReads > 0:
		s.sources.fileRequests.Add(1)
		s.sources.fileBytes.Add(bytes)
	case blobHits > 0 || blockReads == 0:
		s.sources.memoryRequests.Add(1)
		s.sources.memoryBytes.Add(bytes)
	default:
		s.sources.unknownRequests.Add(1)
		s.sources.unknownBytes.Add(bytes)
	}
	return value, nil
}

func (s *sourceCounters) report() string {
	return fmt.Sprintf("kuasar.chunk.memory.requests COUNT : %d\nkuasar.chunk.memory.bytes COUNT : %d\nkuasar.chunk.file.requests COUNT : %d\nkuasar.chunk.file.bytes COUNT : %d\nkuasar.chunk.unknown.requests COUNT : %d\nkuasar.chunk.unknown.bytes COUNT : %d\n",
		s.memoryRequests.Load(), s.memoryBytes.Load(), s.fileRequests.Load(), s.fileBytes.Load(), s.unknownRequests.Load(), s.unknownBytes.Load())
}
