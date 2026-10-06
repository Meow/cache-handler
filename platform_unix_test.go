//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package httpcache

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLockDir(t *testing.T) {
	dir := t.TempDir()

	if f, err := lockDir(filepath.Join(dir, "missing", lockName)); err == nil {
		_ = f.Close()
		t.Error("locked a directory that does not exist")
	}

	first, err := lockDir(filepath.Join(dir, lockName))
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lockDir(filepath.Join(dir, lockName)); err == nil {
		_ = second.Close()
		t.Error("locked a directory twice")
	}

	// Closing the holder releases the lock.
	_ = first.Close()
	second, err := lockDir(filepath.Join(dir, lockName))
	if err != nil {
		t.Fatalf("locking a directory that was released: %v", err)
	}
	_ = second.Close()
}

// TestStorePurgedAsItsLoaderEnds covers a purge while the loader reads its
// last file: the loader must not report the cache as it found it.
func TestStorePurgedAsItsLoaderEnds(t *testing.T) {
	dir := t.TempDir()

	// The loader's last file is a pipe, whose open blocks until a writer
	// opens it: that signals when the loader gets there.
	var id ID
	for i := range id {
		id[i] = 0xff
	}
	pipe := filepath.Join(dir, "ff", id.String())
	if err := os.Mkdir(filepath.Dir(pipe), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(pipe, 0o600); err != nil {
		t.Skipf("no named pipes here: %v", err)
	}
	meet := func() bool {
		f, err := os.OpenFile(pipe, os.O_WRONLY|unix.O_NONBLOCK, 0)
		if err == nil {
			_ = f.Close()
		}

		return err == nil
	}

	s := openTestStore(t, dir, Limits{})
	// The loader must never be left blocked on the pipe, or the store could
	// not be closed.
	t.Cleanup(func() { meet() })

	// The pipe is not a cache file. Removing it takes its name's stripe lock,
	// which is where the loader is held next.
	stripe := &s.stripes[id[0]]
	stripe.Lock()
	held := true
	defer func() {
		if held {
			stripe.Unlock()
		}
	}()
	waitFor(t, "the loader to get to the last file", meet)

	purged := make(chan int)
	go func() { purged <- s.PurgeAll() }()
	waitFor(t, "the cache to be emptied", func() bool { return !s.Stats().Loading })

	stripe.Unlock()
	held = false
	if n := <-purged; n != 0 {
		t.Errorf("purged %d entries from an empty cache", n)
	}
	if err := storePut(t, s, "after", nil, nil, []byte("body")); err != nil {
		t.Fatal(err)
	}
	// Closing waits for the loader.
	_ = s.Close()

	if _, err := os.Lstat(pipe); !os.IsNotExist(err) {
		t.Error("a file that is not a cache file was kept")
	}
	if len(s.index) != 1 || s.index[makeID("after")] == nil || cacheFiles(dir) != 1 {
		t.Errorf("%d entries and %d files, want the response stored after the purge", len(s.index), cacheFiles(dir))
	}
}
