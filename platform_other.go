//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package httpcache

import "os"

// Without mmap the memory tier falls back to the Go heap. The limit on the
// bytes in use still holds, but freed memory is only returned by the garbage
// collector.
func mapSegment(size int) ([]byte, error) {
	return make([]byte, size), nil
}

func unmapSegment([]byte) {}

func releasePages([]byte) {}

func lockDir(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
}
