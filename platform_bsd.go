//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package httpcache

import "golang.org/x/sys/unix"

// madvRelease lets the system take the pages back when it needs memory.
const madvRelease = unix.MADV_FREE
