package immutableindex

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

func testCache(t *testing.T, dir string, limit, disk int) *cache {
	t.Helper()
	c := newCache(dir, limit, disk)
	t.Cleanup(func() {
		for _, b := range c.mapped {
			if err := unix.Munmap(b); err != nil {
				t.Error(err)
			}
		}
	})
	return c
}
func TestSharedImmutableCacheConcurrentOwners(t *testing.T) {
	dir := privateDir(t)
	body := bytes.Repeat([]byte{37}, 128<<10)
	owners := make([]*cache, 16)
	for i := range owners {
		owners[i] = testCache(t, dir, 1<<20, 2<<20)
	}
	var wg sync.WaitGroup
	for _, c := range owners {
		wg.Add(1)
		go func(c *cache) {
			defer wg.Done()
			b := c.share(body)
			if !bytes.Equal(b, body) || &b[0] == &body[0] {
				t.Error("not mapped")
			}
		}(c)
	}
	wg.Wait()
	files, _ := filepath.Glob(filepath.Join(dir, "*.idx"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	st, err := os.Stat(files[0])
	if err != nil || st.Mode().Perm() != 0400 {
		t.Fatal(st, err)
	}
	if got := owners[0].share(body); &got[0] != &owners[0].mapped[fmt.Sprintf("%x.idx", sha256.Sum256(body))][0] {
		t.Fatal("duplicate mapping")
	}
}
func TestSharedCacheRejectsCorruptionAndSymlink(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(fmt.Sprint(symlink), func(t *testing.T) {
			dir := privateDir(t)
			body := bytes.Repeat([]byte{9}, 8192)
			name := filepath.Join(dir, fmt.Sprintf("%x.idx", sha256.Sum256(body)))
			victim := filepath.Join(t.TempDir(), "victim")
			original := []byte("keep original")
			if err := os.WriteFile(victim, original, 0600); err != nil {
				t.Fatal(err)
			}
			if symlink {
				if err := os.Symlink(victim, name); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(name, bytes.Repeat([]byte{8}, len(body)), 0400); err != nil {
					t.Fatal(err)
				}
			}
			c := testCache(t, dir, 1<<20, 1<<20)
			got := c.share(body)
			if !bytes.Equal(got, body) || &got[0] == &body[0] {
				t.Fatal("bad cache changed source or failed repair")
			}
			preserved, _ := os.ReadFile(victim)
			if !bytes.Equal(preserved, original) {
				t.Fatal("modified symlink target")
			}
		})
	}
}
func TestEvictionKeepsLiveMappingsAndBudgets(t *testing.T) {
	dir := privateDir(t)
	c := testCache(t, dir, 16384, 8192)
	a := bytes.Repeat([]byte{1}, 8192)
	b := bytes.Repeat([]byte{2}, 8192)
	old := c.share(a)
	next := c.share(b)
	if !bytes.Equal(old, a) || !bytes.Equal(next, b) {
		t.Fatal("eviction invalidated live reader")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.idx"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	third := bytes.Repeat([]byte{3}, 8192)
	got := c.share(third)
	if &got[0] != &third[0] || c.used != 16384 {
		t.Fatal("unbounded process cache")
	}
}
func TestWritableDirectoryFallsBackWithoutPublication(t *testing.T) {
	dir := privateDir(t)
	if err := os.Chmod(dir, 0777); err != nil {
		t.Fatal(err)
	}
	c := testCache(t, dir, 1<<20, 1<<20)
	b := bytes.Repeat([]byte{3}, 8192)
	got := c.share(b)
	if &got[0] != &b[0] || len(c.mapped) != 0 {
		t.Fatal("used unsafe directory")
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("wrote unsafe directory")
	}
}

func privateDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/dev/shm", "kuasar-index-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	if err := os.Chmod(d, 0700); err != nil {
		t.Fatal(err)
	}
	return d
}
