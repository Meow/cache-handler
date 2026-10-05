package httpcache

import "golang.org/x/sys/unix"

// madvRelease frees the pages at once.
const madvRelease = unix.MADV_DONTNEED
