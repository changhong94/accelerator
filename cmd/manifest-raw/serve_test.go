// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"encoding/binary"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"testing"
)

func TestPageProtocol(t *testing.T) {
	data := []byte("0123456789")
	var request, reply bytes.Buffer
	binary.Write(&request, binary.LittleEndian, [2]uint64{3, 4})
	if err := pageProtocol(sparse.Dense(bytes.NewReader(data), uint64(len(data))), &request, &reply); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(reply.Bytes()[:8]) != 10 || string(reply.Bytes()[16:]) != "3456" {
		t.Fatal("incorrect page contents")
	}
	for _, r := range [][2]uint64{{9, 2}, {0, 0}, {0, 2<<20 + 1}, {^uint64(0), 1}} {
		request.Reset()
		reply.Reset()
		binary.Write(&request, binary.LittleEndian, r)
		if pageProtocol(sparse.Dense(bytes.NewReader(data), 10), &request, &reply) == nil {
			t.Fatalf("accepted invalid range %v", r)
		}
		if reply.Len() != 16 {
			t.Fatal("invalid request exposed data")
		}
	}
}

func TestPageProtocolReportsSparseMetadataWithoutReadingPayload(t *testing.T) {
	src, err := sparse.NewSource(bytes.NewReader(make([]byte, 12288)), 12288, []sparse.Extent{{Offset: 4096, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	var reply bytes.Buffer
	if err = pageProtocol(src, bytes.NewReader(nil), &reply); err != nil {
		t.Fatal(err)
	}
	var header [4]uint64
	if err = binary.Read(&reply, binary.LittleEndian, &header); err != nil {
		t.Fatal(err)
	}
	if header != [4]uint64{12288, 1, 4096, 4096} || reply.Len() != 0 {
		t.Fatalf("bad sparse header %v", header)
	}
}
