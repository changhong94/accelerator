package tarstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestSparseIdentityRoundTripAndTampering(t *testing.T) {
	src, body := presenceFixture(t)
	var output bytes.Buffer
	_, digest, err := WriteTo(context.Background(), &output, "snapshot", src, WithSparseIdentity())
	if err != nil {
		t.Fatal(err)
	}
	for _, sequential := range []bool{false, true} {
		var opened sparse.Source
		if sequential {
			opened, _, err = SourceFrom(readerOnly{bytes.NewReader(output.Bytes())}, "", WithExpectedDigest(DigestScheme, digest))
		} else {
			opened, _, err = SourceAt(bytes.NewReader(output.Bytes()), int64(output.Len()), "", WithExpectedDigest(DigestScheme, digest))
		}
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(body))
		_, err = opened.ReadAt(context.Background(), got, 0)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("round trip sequential=%v: %v", sequential, err)
		}
		if closer, ok := opened.(io.Closer); ok {
			closer.Close()
		}
	}
	verify := func(b []byte) error {
		s, _, e := SourceFrom(readerOnly{bytes.NewReader(b)}, "", WithExpectedDigest(DigestScheme, digest))
		if e != nil {
			return e
		}
		_, e = s.ReadAt(context.Background(), make([]byte, len(body)), 0)
		return e
	}
	// Change present Zero into Hole, repairing the non-cryptographic CRC. The
	// cryptographic identity must still reject the changed overlay semantics.
	changed := append([]byte(nil), output.Bytes()...)
	p := []byte{0x0a}
	header := makePresenceHeader(p, src.payload)
	copy(changed[512:], header[:])
	copy(changed[512+presenceHeaderSize:], p)
	if e := verify(changed); !errors.Is(e, ErrDigestMismatch) {
		t.Fatalf("presence tamper: %v", e)
	}
	cursor := bytes.NewReader(output.Bytes())
	m, e := locate(cursor, "snapshot", seekSkip(cursor))
	if e != nil {
		t.Fatal(e)
	}
	start, _ := cursor.Seek(0, io.SeekCurrent)
	for _, off := range []int64{start, start + m.stored - m.mapLen - 1} {
		changed = append([]byte(nil), output.Bytes()...)
		changed[off] ^= 1
		if e := verify(changed); !errors.Is(e, ErrDigestMismatch) {
			t.Fatalf("byte tamper at %d: %v", off, e)
		}
	}
}

func TestSparseIdentityBindsLayoutWithoutHashingOmittedZeros(t *testing.T) {
	// A large all-Zero logical payload hashes only a small descriptor. No large
	// allocation or zero-byte expansion is needed, including an empty tail.
	const size = uint64(2 << 30)
	p := bytes.Repeat([]byte{255}, int((size/4096+7)/8))
	a := newIdentityAccumulator(size, nil, 0, 0, p, nil, true)
	if a.presence != nil || a.payloadRemaining != 0 {
		t.Fatal("still expands omitted zero bytes")
	}
	x := a.finish("snapshot", size, size)
	p[0] ^= 1
	b := newIdentityAccumulator(size, nil, 0, 0, p, nil, true)
	if x.digest == b.finish("snapshot", size, size).digest {
		t.Fatal("Hole and Zero collide")
	}
	// Equal stored bytes at different offsets must have different identities.
	p[0] = 255
	e1 := []extent{{Offset: 0, Size: 4096}}
	e2 := []extent{{Offset: 4096, Size: 4096}}
	a = newIdentityAccumulator(size, nil, 4096, 0, p, e1, true)
	b = newIdentityAccumulator(size, nil, 4096, 0, p, e2, true)
	data := make([]byte, 4096)
	binary.LittleEndian.PutUint32(data, 123)
	a.Write(data)
	b.Write(data)
	if a.finish("snapshot", size, size).digest == b.finish("snapshot", size, size).digest {
		t.Fatal("extent positions not authenticated")
	}
}

func TestSparseIdentityEncryptedRoundTrip(t *testing.T) {
	src, body := presenceFixture(t)
	codec := &optionTestCodec{}
	var out bytes.Buffer
	scheme, digest, err := WriteTo(context.Background(), &out, "snapshot", src, WithSparseIdentity(), WithCodec(codec, true))
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range []bool{false, true} {
		var s sparse.Source
		opts := []ReadOption{WithCodec(codec, true), WithExpectedDigest(scheme, digest)}
		if seq {
			s, _, err = SourceFrom(readerOnly{bytes.NewReader(out.Bytes())}, "", opts...)
		} else {
			s, _, err = SourceAt(bytes.NewReader(out.Bytes()), int64(out.Len()), "", opts...)
		}
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(body))
		_, err = s.ReadAt(context.Background(), got, 0)
		if err != nil || !bytes.Equal(body, got) {
			t.Fatal("encrypted", seq, err)
		}
		if c, ok := s.(io.Closer); ok {
			c.Close()
		}
	}
}

func TestSparseIdentityDefaultRepublishRecomputes(t *testing.T) {
	src, _ := presenceFixture(t)
	var out bytes.Buffer
	_, _, err := WriteTo(context.Background(), &out, "snapshot", src, WithSparseIdentity())
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := SourceAt(bytes.NewReader(out.Bytes()), int64(out.Len()), "")
	if err != nil {
		t.Fatal(err)
	}
	provider := s.(IdentityProvider)
	if _, known := provider.TarStreamDigest("snapshot"); known {
		t.Fatal("v2 identity incorrectly reused by legacy writer")
	}
	if _, _, known := provider.PayloadCommitment(); known {
		t.Fatal("v2 payload incorrectly reused for legacy tail replacement")
	}
	var legacy bytes.Buffer
	_, got, err := WriteTo(context.Background(), &legacy, "snapshot", s)
	if err != nil {
		t.Fatal(err)
	}
	var expected bytes.Buffer
	_, want, err := WriteTo(context.Background(), &expected, "snapshot", src)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || !bytes.Equal(legacy.Bytes(), expected.Bytes()) {
		t.Fatal("legacy republish mismatch")
	}
}
