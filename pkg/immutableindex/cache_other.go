//go:build !linux

package immutableindex

// ShareReadOnly retains the ordinary heap representation on non-Linux hosts.
func ShareReadOnly(b []byte) []byte { return b }
