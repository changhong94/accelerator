// SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// serveDisks is a host service, independent of any parent VM. Every connection
// binds one immutable Manifest. It accepts reads only; guest writes stay in the
// VMM's durably flushed private QCOW2 leaf. The socket directory must be private.
func serveDisks(cfg *manifest.Config, socket string) error {
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var mu sync.Mutex
	connections := make(map[net.Conn]bool)
	var workers sync.WaitGroup
	go func() {
		<-ctx.Done()
		listener.Close()
		mu.Lock()
		for c := range connections {
			c.Close()
		}
		mu.Unlock()
	}()
	defer workers.Wait()
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			stop()
			return err
		}
		mu.Lock()
		connections[conn] = true
		mu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer conn.Close()
			defer func() { mu.Lock(); delete(connections, conn); mu.Unlock() }()
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			var keyText [65]byte
			if _, err := io.ReadFull(conn, keyText[:]); err != nil {
				return
			}
			key, err := manifest.ParseKeyRef(string(keyText[:64]))
			if err != nil {
				return
			}
			f, err := cfg.NewFetcherWithOptions(fetch.Options{VerifyContent: true})
			if err != nil {
				return
			}
			defer f.Close()
			openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			var src interface {
				sparse.Source
				io.Closer
			}
			if keyText[64] == 0 {
				src, err = f.OpenManifest(openCtx, key)
			} else if keyText[64] == 1 {
				src, err = newGraphStream(openCtx, f, string(keyText[:64]))
			} else {
				cancel()
				return
			}
			cancel()
			if err != nil {
				return
			}
			defer src.Close()
			conn.SetReadDeadline(time.Time{})
			// No fallback to zeros or unauthenticated files on source failure.
			_ = pageProtocol(src, conn, conn)
		}()
	}
}
