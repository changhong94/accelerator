//go:build !no_rocksdb

package rocks

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	grocksdb "github.com/linxGnu/grocksdb"
)

func ticker(t *testing.T, s *impl, name string) uint64 {
	t.Helper()
	for _, line := range strings.Split(s.statistics.GetStatisticsString(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 4 && fields[0] == name && fields[1] == "COUNT" {
			n, err := strconv.ParseUint(fields[3], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatalf("counter %s is unavailable", name)
	return 0
}

func TestBlobCacheReportsPayloadHits(t *testing.T) {
	cfg := tempRocksConfigT(t)
	cfg.BlobCacheBytes = "8MiB"
	cfg.Statistics = true
	s, err := openImpl(cfg, runtime.FreqConfig{DisableEviction: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := bytes.Repeat([]byte("payload"), 40000)
	ctx := context.Background()
	if err := s.put(ctx, cfChunk, []byte("blob-stat-test"), data); err != nil {
		t.Fatal(err)
	}
	f := grocksdb.NewDefaultFlushOptions()
	defer f.Destroy()
	f.SetWait(true)
	if err := s.db.FlushCF(s.cfh[cfChunk], f); err != nil {
		t.Fatal(err)
	}
	read := func() {
		b, found, err := s.get(ctx, cfChunk, []byte("blob-stat-test"))
		if err != nil || !found {
			t.Fatalf("read: %v %v", found, err)
		}
		defer b.Release()
		if !bytes.Equal(b.Bytes(), data) {
			t.Fatal("payload mismatch")
		}
	}
	read()
	before := ticker(t, s, "rocksdb.blobdb.cache.bytes.read")
	fileBefore := ticker(t, s, "rocksdb.blobdb.blob.file.bytes.read")
	read()
	if delta := ticker(t, s, "rocksdb.blobdb.cache.bytes.read") - before; delta != uint64(len(data)) {
		t.Fatalf("cache payload bytes=%d want %d", delta, len(data))
	}
	if ticker(t, s, "rocksdb.blobdb.blob.file.bytes.read") != fileBefore {
		t.Fatal("warm payload read accessed blob file")
	}
	if s.sources.memoryRequests.Load() != 1 || s.sources.fileRequests.Load() != 1 || s.sources.unknownRequests.Load() != 0 {
		t.Fatalf("payload attribution: %s", s.sources.report())
	}
	if s.sources.memoryBytes.Load() != uint64(len(data)) || s.sources.fileBytes.Load() != uint64(len(data)) {
		t.Fatalf("payload byte attribution: %s", s.sources.report())
	}
	count := 0
	for _, cf := range s.AllStats() {
		if cf.DatabaseStatistics != "" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("database statistics emitted %d times", count)
	}
}
