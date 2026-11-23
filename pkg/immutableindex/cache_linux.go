// Package immutableindex shares already-validated, read-only metadata between
// trusted host processes. It never substitutes for reading/authenticating a source.
package immutableindex

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// Capture deployment configuration at process initialization, before any task
// environment is installed. An unset directory preserves the heap-only path.
var configured = newCache(os.Getenv("KUASAR_IMMUTABLE_INDEX_CACHE"), 8<<20, 64<<20)
var sequence atomic.Uint64

type cache struct {
	mu                     sync.Mutex
	dir                    string
	limit, diskLimit, used int
	mapped                 map[string][]byte
}

func newCache(dir string, limit, diskLimit int) *cache {
	return &cache{dir: dir, limit: limit, diskLimit: diskLimit}
}

// ShareReadOnly may return a PROT_READ mapping. Callers must never modify it.
// Each process retains at most 8 MiB of mappings for its lifetime; a full cache
// or any I/O/validation error falls back to the supplied authoritative bytes.
// Entries live only in private tmpfs, never as plaintext persistent metadata.
func ShareReadOnly(b []byte) []byte { return configured.share(b) }

func private(fd int, directory bool) bool {
	var s unix.Stat_t
	if unix.Fstat(fd, &s) != nil || s.Uid != uint32(os.Geteuid()) || s.Mode&0077 != 0 {
		return false
	}
	if directory {
		return s.Mode&unix.S_IFMT == unix.S_IFDIR
	}
	return s.Mode&unix.S_IFMT == unix.S_IFREG && s.Nlink == 1
}
func (c *cache) share(b []byte) []byte {
	if c.dir == "" || len(b) < 4096 || len(b) > c.limit {
		return b
	}
	key := fmt.Sprintf("%x.idx", sha256.Sum256(b))
	c.mu.Lock()
	defer c.mu.Unlock()
	if mapped := c.mapped[key]; mapped != nil {
		return mapped
	}
	charge := (len(b) + os.Getpagesize() - 1) / os.Getpagesize() * os.Getpagesize()
	if charge > c.limit-c.used {
		return b
	}
	if os.MkdirAll(c.dir, 0700) != nil {
		return b
	}
	dir, err := unix.Open(c.dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return b
	}
	defer unix.Close(dir)
	var fs unix.Statfs_t
	if !private(dir, true) || unix.Fstatfs(dir, &fs) != nil || fs.Type != unix.TMPFS_MAGIC {
		return b
	}
	mapped := mapEntry(dir, key, b)
	if mapped == nil {
		lock, err := unix.Openat(dir, ".lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if err != nil {
			return b
		}
		defer unix.Close(lock)
		if !private(lock, false) || unix.Flock(lock, unix.LOCK_EX) != nil {
			return b
		}
		defer unix.Flock(lock, unix.LOCK_UN)
		// Another process may have published the same validated bytes meanwhile.
		mapped = mapEntry(dir, key, b)
		if mapped == nil {
			if c.evict(dir, len(b)) != nil || publish(dir, key, b) != nil {
				return b
			}
			mapped = mapEntry(dir, key, b)
		}
	}
	if mapped == nil {
		return b
	}
	if c.mapped == nil {
		c.mapped = make(map[string][]byte)
	}
	c.mapped[key] = mapped
	c.used += charge
	return mapped
}

func mapEntry(dir int, key string, original []byte) []byte {
	fd, err := unix.Openat(dir, key, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if !private(fd, false) || unix.Fstat(fd, &st) != nil || st.Size != int64(len(original)) || st.Mode&0222 != 0 {
		return nil
	}
	b, err := unix.Mmap(fd, 0, len(original), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil
	}
	// A stale/corrupt cache can never change the source's metadata semantics.
	if !bytes.Equal(b, original) {
		unix.Munmap(b)
		return nil
	}
	return b
}

func publish(dir int, key string, b []byte) error {
	tmp := fmt.Sprintf(".tmp-%d-%d", os.Getpid(), sequence.Add(1))
	fd, err := unix.Openat(dir, tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	defer unix.Unlinkat(dir, tmp, 0)
	f := os.NewFile(uintptr(fd), "immutable-index")
	n, err := f.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Chmod(0400)
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// No persistence barrier is elided from the original data path: this cache
	// is disposable, and a partial/lost entry is rejected against source bytes.
	return unix.Renameat(dir, tmp, dir, key)
}

func (c *cache) evict(dir int, incoming int) error {
	if incoming > c.diskLimit {
		return fmt.Errorf("index cache budget")
	}
	dup, err := unix.FcntlInt(uintptr(dir), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(dup), "index-cache-dir")
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return err
	}
	type item struct {
		name     string
		size     int64
		modified unix.Timespec
	}
	var files []item
	var total int64
	for _, entry := range entries {
		name := entry.Name()
		if !(len(name) == 68 && strings.HasSuffix(name, ".idx")) && !strings.HasPrefix(name, ".tmp-") {
			continue
		}
		var st unix.Stat_t
		if unix.Fstatat(dir, name, &st, unix.AT_SYMLINK_NOFOLLOW) != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&unix.S_IFMT != unix.S_IFREG {
			continue
		}
		if strings.HasPrefix(name, ".tmp-") {
			unix.Unlinkat(dir, name, 0)
			continue
		}
		total += st.Size
		files = append(files, item{name, st.Size, st.Mtim})
	}
	sort.Slice(files, func(i, j int) bool {
		a, b := files[i].modified, files[j].modified
		return a.Sec < b.Sec || (a.Sec == b.Sec && a.Nsec < b.Nsec)
	})
	count := len(files)
	for _, v := range files {
		if total+int64(incoming) <= int64(c.diskLimit) && count < 128 {
			break
		}
		// Unlinking never truncates a file or invalidates existing mmap readers.
		if err := unix.Unlinkat(dir, v.name, 0); err != nil {
			return err
		}
		total -= v.size
		count--
	}
	return nil
}
