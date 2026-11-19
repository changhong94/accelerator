// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// servePages owns one authenticated immutable stream. No guest-supplied keys or
// paths are accepted. The VMM sends bounded offset/length requests over its
// private inherited socket; EOF releases the stream and its bounded chunk cache.
func servePages(cfg *manifest.Config, keyRef string, socket string) error {
	return servePagesMode(cfg, keyRef, socket, false)
}
func servePagesMode(cfg *manifest.Config, keyRef string, socket string, graph bool) error {
	key, err := manifest.ParseKeyRef(keyRef)
	if err != nil {
		return err
	}
	f, err := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
	if err != nil {
		return err
	}
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	var src interface {
		sparse.Source
		io.Closer
	}
	if graph {
		src, err = newGraphStream(ctx, f, keyRef)
	} else {
		src, err = f.OpenManifest(ctx, key)
	}
	cancel()
	if err != nil {
		return err
	}
	defer src.Close()
	if socket == "" {
		return pageProtocol(src, os.Stdin, os.Stdout)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	if _, err = os.Stdout.Write([]byte{1}); err != nil {
		return err
	}
	listener.SetDeadline(time.Now().Add(30 * time.Second))
	conn, err := listener.AcceptUnix()
	if err != nil {
		return err
	}
	defer conn.Close()
	return pageProtocol(src, conn, conn)
}

func pageProtocol(src sparse.Source, input io.Reader, output io.Writer) error {
	var requests, readBytes uint64
	defer func() { fmt.Fprintf(os.Stderr, "MANIFEST_READ requests=%d bytes=%d\n", requests, readBytes) }()
	if err := binary.Write(output, binary.LittleEndian, src.Size()); err != nil {
		return err
	}
	// Authenticated zero/hole extents let the VMM skip background fetching of
	// known-zero pages. Only complete pages inside these extents may be skipped.
	var zeros [][2]uint64
	for off := uint64(0); off < src.Size(); {
		run, err := src.RunAt(off, src.Size()-off)
		if err != nil {
			return err
		}
		if run == nil || run.Offset() > off || run.End() <= off || run.End() > src.Size() {
			return errors.New("invalid sparse extent")
		}
		if run.Kind() == sparse.Zero || run.Kind() == sparse.Hole {
			zeros = append(zeros, [2]uint64{off, run.End() - off})
		}
		off = run.End()
	}
	if len(zeros) > 1<<20 {
		return errors.New("too many sparse extents")
	}
	if err := binary.Write(output, binary.LittleEndian, uint64(len(zeros))); err != nil {
		return err
	}
	if err := binary.Write(output, binary.LittleEndian, zeros); err != nil {
		return err
	}
	var req [2]uint64
	var buf []byte
	for {
		err := binary.Read(input, binary.LittleEndian, &req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		offset, n := req[0], req[1]
		if n == 0 || n > 2<<20 || offset > src.Size() || n > src.Size()-offset {
			return errors.New("invalid page range")
		}
		if uint64(len(buf)) < n {
			buf = make([]byte, n)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		requests++
		readBytes += n
		got, err := src.ReadAt(ctx, buf[:n], offset)
		cancel()
		if err != nil {
			return err
		}
		if uint64(got) != n {
			return io.ErrUnexpectedEOF
		}
		if _, err = output.Write(buf[:n]); err != nil {
			return err
		}
	}
}
