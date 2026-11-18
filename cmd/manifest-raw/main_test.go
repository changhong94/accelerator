// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestMaterializeSparseAndNoOverwrite(t *testing.T) {
	data := make([]byte, 3<<20)
	copy(data, []byte("guest memory before checkpoint"))
	copy(data[2<<20:], []byte("persistent disk tail"))
	src, err := sparse.NewSource(bytes.NewReader(data), uint64(len(data)), []sparse.Extent{{Offset: 1 << 20, Size: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "memory-ranges")
	n, err := materialize(context.Background(), src, path)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2<<20 {
		t.Fatalf("copied %d bytes", n)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, got) {
		t.Fatal("restored content differs")
	}
	if _, err := materialize(context.Background(), src, path); err == nil {
		t.Fatal("overwrote existing immutable file")
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatal("temporary files leaked")
	}
}

func TestCanceledRestoreNeverPublishes(t *testing.T) {
	src := sparse.Dense(bytes.NewReader([]byte("abc")), 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "disk.raw")
	if _, err := materialize(ctx, src, path); err == nil {
		t.Fatal("ignored cancellation")
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 0 {
		t.Fatal("partial output published or leaked")
	}
}
