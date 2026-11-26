package fetch

import (
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"log"
	"os"
	"time"
)

var remoteFaultTrace = os.Getenv("KUASAR_REMOTE_FAULT_TRACE") == "1"

func traceRemoteChunk(event string, stream *manifestStream, chunk uint64, entry codec.ChunkEntry, offset uint64, length int) {
	if remoteFaultTrace {
		log.Printf("REMOTE_CHUNK event=%s pid=%d stream=%p chunk=%d hash=%x chunk_offset=%d chunk_bytes=%d offset=%d bytes=%d", event, os.Getpid(), stream, chunk, entry.CiphertextHash[:8], entry.Offset, entry.Size, offset, length)
	}
}

// Diagnostics only: measure retrieval separately from authentication and decode.
// No key/plaintext is logged and no timer is read when diagnostics are off.
func traceReadStages(s *manifestStream, chunk uint64, e codec.ChunkEntry, mode string, begin, got, verified time.Time) {
	if got.IsZero() || verified.IsZero() {
		return
	}
	end := time.Now()
	log.Printf("REMOTE_READ stream=%p chunk=%d hash=%x stream_bytes=%d chunk_bytes=%d mode=%s get_ns=%d verify_ns=%d decode_copy_ns=%d total_ns=%d", s, chunk, e.CiphertextHash[:8], s.Size(), e.Size, mode, got.Sub(begin).Nanoseconds(), verified.Sub(got).Nanoseconds(), end.Sub(verified).Nanoseconds(), end.Sub(begin).Nanoseconds())
}
