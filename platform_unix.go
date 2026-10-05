//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package httpcache

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// mapSegment reserves anonymous memory outside the Go heap.
func mapSegment(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
}

func unmapSegment(b []byte) {
	_ = unix.Munmap(b)
}

// releasePages gives the memory backing b back to the system while keeping
// the address range usable.
func releasePages(b []byte) {
	_ = unix.Madvise(b, madvRelease)
}

// lockDir takes an exclusive lock on the cache directory so two processes
// cannot manage the same files with separate accounting.
func lockDir(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			err = errors.New("the cache directory is in use by another process")
		}

		return nil, err
	}

	return f, nil
}
