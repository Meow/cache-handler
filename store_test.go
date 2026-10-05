package httpcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func openTestStore(t *testing.T, dir string, limits Limits) *Store {
	t.Helper()

	if limits.MaxSize == 0 {
		limits.MaxSize = 64 << 20
	}
	s, err := OpenStore(dir, limits, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// waitLoaded blocks until the store indexed the files it found on disk.
func waitLoaded(t *testing.T, s *Store) {
	t.Helper()

	waitFor(t, "the store to load", func() bool { return !s.Stats().Loading })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// bodyFor returns a body that can be told apart from the body of any other
// key, so that a response served for the wrong key is noticed.
func bodyFor(key string, size int) []byte {
	sum := sha256.Sum256([]byte(key))
	body := make([]byte, size)
	for i := range body {
		body[i] = sum[i%len(sum)]
	}

	return body
}

func storePut(t testing.TB, s *Store, key string, vary []string, reqHeader http.Header, body []byte) error {
	t.Helper()

	return storePutUses(t, s, key, vary, reqHeader, body, 1)
}

// storePutUses stores a response that is to be written to disk once it was
// requested minUses times.
func storePutUses(t testing.TB, s *Store, key string, vary []string, reqHeader http.Header, body []byte, minUses int) error {
	t.Helper()

	now := time.Now()
	rec := &record{
		stored: now.UnixMilli(),
		fresh:  now.Add(time.Hour).UnixMilli(),
		status: http.StatusOK,
		header: http.Header{"Content-Type": {"application/octet-stream"}, "X-Key": {key}},
	}

	w, err := s.Create(key, vary, reqHeader, rec, 0, -1, minUses)
	if err != nil {
		return err
	}

	// The request a response is fetched for reads it as it arrives, which
	// counts as its first use.
	var tail *Tail
	if minUses > 1 {
		if tail, err = w.Tail(context.Background()); err != nil {
			w.Abort()
			return err
		}
		defer tail.Close()
	}

	if _, err := w.Write(body); err != nil {
		w.Abort()
		return err
	}
	if err := w.Commit(); err != nil {
		return err
	}

	if tail != nil {
		if got, err := io.ReadAll(tail); err != nil || !bytes.Equal(got, body) {
			return fmt.Errorf("read %d bytes (%v) from the response as it was stored, want the %d of its body", len(got), err, len(body))
		}
	}

	return nil
}

// cacheFiles counts the responses and markers that have a file under dir.
func cacheFiles(dir string) int {
	files := 0
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && len(info.Name()) == 32 {
			files++
		}
		return nil
	})

	return files
}

// waitPersisted blocks until the store has nothing left to write to disk of
// what it was asked to.
func waitPersisted(t *testing.T, s *Store) {
	t.Helper()

	waitFor(t, "the responses requested again to be written to disk", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		if len(s.persistCh) > 0 {
			return false
		}
		for _, e := range s.index {
			if e.persisting {
				return false
			}
		}

		return true
	})
}

func storeGet(t testing.TB, s *Store, key string, reqHeader http.Header) ([]byte, *Hit) {
	t.Helper()

	_, hit := s.Lookup(key, reqHeader)
	if hit == nil {
		return nil, nil
	}
	defer hit.Close()

	var buf bytes.Buffer
	if err := hit.WriteBody(&buf); err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}

	return buf.Bytes(), hit
}

func TestStoreRoundTrip(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})

	if _, hit := s.Lookup("missing", nil); hit != nil {
		t.Fatal("found a response that was never stored")
	}

	for _, size := range []int{0, 1, 4095, 4096, 4097, 300_000} {
		key := fmt.Sprintf("key-%d", size)
		body := bodyFor(key, size)
		if err := storePut(t, s, key, nil, nil, body); err != nil {
			t.Fatalf("storing %s: %v", key, err)
		}

		got, hit := storeGet(t, s, key, nil)
		if hit == nil {
			t.Fatalf("%s not found", key)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("%s: body differs", key)
		}
		if hit.rec.status != http.StatusOK || hit.rec.header.Get("X-Key") != key {
			t.Errorf("%s: unexpected record %+v", key, hit.rec)
		}

		// Random access, as used for range requests.
		if size > 100 {
			_, hit := s.Lookup(key, nil)
			rs := hit.Body()
			if _, err := rs.Seek(-50, io.SeekEnd); err != nil {
				t.Fatal(err)
			}
			tail, _ := io.ReadAll(rs)
			if !bytes.Equal(tail, body[size-50:]) {
				t.Errorf("%s: tail differs", key)
			}
			hit.Close()
		}
	}
}

func TestStoreReplace(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})

	for _, body := range []string{"first", "second, longer"} {
		if err := storePut(t, s, "key", nil, nil, []byte(body)); err != nil {
			t.Fatal(err)
		}
		if got, _ := storeGet(t, s, "key", nil); string(got) != body {
			t.Errorf("got %q, want %q", got, body)
		}
	}
	if n := s.Stats().Entries; n != 1 {
		t.Errorf("%d entries, want 1", n)
	}
}

func TestStoreSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	s := openTestStore(t, dir, Limits{})
	for i := range 50 {
		key := fmt.Sprintf("key-%d", i)
		if err := storePut(t, s, key, nil, nil, bodyFor(key, 1000+i)); err != nil {
			t.Fatal(err)
		}
	}
	before := s.Stats()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A download the previous process did not finish.
	if err := os.WriteFile(filepath.Join(dir, tmpDirName, "w-123"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	s = openTestStore(t, dir, Limits{})
	// Before the loader is done, the files are found on demand.
	if got, _ := storeGet(t, s, "key-7", nil); !bytes.Equal(got, bodyFor("key-7", 1007)) {
		t.Error("key-7 was not found right after reopening")
	}
	waitLoaded(t, s)

	after := s.Stats()
	if after.Entries != before.Entries || after.DiskBytes != before.DiskBytes {
		t.Errorf("reopened with %d entries and %d bytes, had %d and %d", after.Entries, after.DiskBytes, before.Entries, before.DiskBytes)
	}
	for i := range 50 {
		key := fmt.Sprintf("key-%d", i)
		if got, _ := storeGet(t, s, key, nil); !bytes.Equal(got, bodyFor(key, 1000+i)) {
			t.Errorf("%s lost or damaged by the restart", key)
		}
	}
	if left, _ := os.ReadDir(filepath.Join(dir, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}
}

func TestStoreDirectoryIsLocked(t *testing.T) {
	dir := t.TempDir()
	openTestStore(t, dir, Limits{})

	if s, err := OpenStore(dir, Limits{MaxSize: 1 << 20}, nil); err == nil {
		_ = s.Close()
		t.Skip("directory locking is not available on this platform")
	}
}

func TestStoreDropsDamagedFiles(t *testing.T) {
	dir := t.TempDir()

	s := openTestStore(t, dir, Limits{})
	keys := []string{"truncated", "garbled", "intact"}
	for _, key := range keys {
		if err := storePut(t, s, key, nil, nil, bodyFor(key, 10_000)); err != nil {
			t.Fatal(err)
		}
	}
	paths := map[string]string{}
	for _, key := range keys {
		paths[key] = s.path(makeID(key))
	}
	_ = s.Close()

	if err := os.Truncate(paths["truncated"], 5000); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(paths["garbled"])
	data[70] ^= 0xff
	if err := os.WriteFile(paths["garbled"], data, 0o600); err != nil {
		t.Fatal(err)
	}
	// A file that is not ours must be left alone.
	foreign := filepath.Join(filepath.Dir(paths["intact"]), "notes.txt")
	if err := os.WriteFile(foreign, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}

	s = openTestStore(t, dir, Limits{})
	waitLoaded(t, s)

	for _, key := range []string{"truncated", "garbled"} {
		if _, hit := storeGet(t, s, key, nil); hit != nil {
			t.Errorf("the %s file is still served", key)
		}
		if _, err := os.Stat(paths[key]); !os.IsNotExist(err) {
			t.Errorf("the %s file was not removed", key)
		}
	}
	if got, _ := storeGet(t, s, "intact", nil); !bytes.Equal(got, bodyFor("intact", 10_000)) {
		t.Error("the intact file was lost")
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Errorf("a foreign file was removed: %v", err)
	}
}

func TestStoreServesWhatWasDeletedBehindItsBack(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})
	if err := storePut(t, s, "key", nil, nil, []byte("body")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(s.path(makeID("key"))); err != nil {
		t.Fatal(err)
	}

	if _, hit := storeGet(t, s, "key", nil); hit != nil {
		t.Fatal("served a response whose file is gone")
	}
	if st := s.Stats(); st.Entries != 0 || st.DiskBytes != 0 {
		t.Errorf("the missing file is still accounted for: %+v", st)
	}
}

// TestStoreKeepsWhatItCannotOpen checks that an entry is only forgotten when
// its file is gone or damaged, not when opening it fails for another reason,
// as it does when the process is out of file descriptors.
func TestStoreKeepsWhatItCannotOpen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("file permissions do not apply to root")
	}

	s := openTestStore(t, t.TempDir(), Limits{})
	if err := storePut(t, s, "key", nil, nil, []byte("body")); err != nil {
		t.Fatal(err)
	}
	path := s.path(makeID("key"))

	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, hit := storeGet(t, s, "key", nil); hit != nil {
		t.Fatal("read a file that cannot be opened")
	}
	if n := s.Stats().Entries; n != 1 {
		t.Fatalf("the entry was forgotten: %d entries", n)
	}

	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, _ := storeGet(t, s, "key", nil); string(got) != "body" {
		t.Error("the response is gone")
	}
}

// TestStoreRecoversItsDirectories checks that the cache keeps working when
// its directory is emptied by hand.
func TestStoreRecoversItsDirectories(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{})
	if err := storePut(t, s, "before", nil, nil, []byte("body")); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != lockName {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}

	if _, hit := storeGet(t, s, "before", nil); hit != nil {
		t.Error("served a response whose file was removed")
	}
	if err := storePut(t, s, "after", nil, nil, []byte("body")); err != nil {
		t.Fatalf("storing after the directory was emptied: %v", err)
	}
	if got, _ := storeGet(t, s, "after", nil); string(got) != "body" {
		t.Error("the response stored after the directory was emptied is not found")
	}
}

func TestStoreMaxSize(t *testing.T) {
	const maxSize = 1 << 20

	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxSize: maxSize})

	for i := range 100 {
		key := fmt.Sprintf("key-%d", i)
		if err := storePut(t, s, key, nil, nil, bodyFor(key, 50_000)); err != nil {
			t.Fatal(err)
		}
		// Keep the first response in use: it must outlive the others.
		if _, hit := storeGet(t, s, "key-0", nil); hit == nil {
			t.Fatalf("the response in use was evicted after %d stores", i)
		}
		if used := s.Stats().DiskBytes; used > maxSize {
			t.Fatalf("%d bytes accounted, over the limit of %d", used, maxSize)
		}
	}

	if _, hit := storeGet(t, s, "key-1", nil); hit != nil {
		t.Error("the least recently used response was not evicted")
	}
	if _, hit := storeGet(t, s, "key-99", nil); hit == nil {
		t.Error("the most recent response was evicted")
	}

	// What the index says must be what the disk holds.
	var onDisk int64
	files := 0
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && len(info.Name()) == 32 {
			onDisk += info.Size()
			files++
		}
		return nil
	})
	st := s.Stats()
	if files != st.Entries || onDisk > maxSize {
		t.Errorf("%d files of %d bytes on disk for %d entries", files, onDisk, st.Entries)
	}
	if st.Evicted == 0 {
		t.Error("no eviction was counted")
	}

	// An object that could never fit is refused rather than emptying the cache.
	if err := storePut(t, s, "huge", nil, nil, make([]byte, maxSize)); err == nil {
		t.Error("stored an object larger than the cache")
	}
	if left, _ := os.ReadDir(filepath.Join(dir, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}

	// Shrinking the limit evicts down to it.
	s.SetLimits(Limits{MaxSize: maxSize / 4})
	if used := s.Stats().DiskBytes; used > maxSize/4 {
		t.Errorf("%d bytes accounted after shrinking the limit to %d", used, maxSize/4)
	}
}

func TestStoreMaxFiles(t *testing.T) {
	const maxFiles = 10

	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxFiles: maxFiles})

	countFiles := func() int {
		files := 0
		_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && path != filepath.Join(dir, lockName) && path != filepath.Join(dir, cacheDirTag) {
				files++
			}
			return nil
		})
		return files
	}

	for i := range 40 {
		key := fmt.Sprintf("key-%d", i)
		if err := storePut(t, s, key, nil, nil, bodyFor(key, 100)); err != nil {
			t.Fatal(err)
		}
		// Keep the first response in use: it must outlive the others.
		if _, hit := storeGet(t, s, "key-0", nil); hit == nil {
			t.Fatalf("the response in use was evicted after %d stores", i)
		}
		if n := s.Stats().Entries; n > maxFiles {
			t.Fatalf("%d entries, over the limit of %d", n, maxFiles)
		}
		if n := countFiles(); n > maxFiles {
			t.Fatalf("%d files on disk, over the limit of %d", n, maxFiles)
		}
	}
	if n := s.Stats().Entries; n != maxFiles {
		t.Errorf("%d entries, want the cache full at %d", n, maxFiles)
	}
	if _, hit := storeGet(t, s, "key-1", nil); hit != nil {
		t.Error("the least recently used response was not evicted")
	}
	if _, hit := storeGet(t, s, "key-39", nil); hit == nil {
		t.Error("the most recent response was evicted")
	}

	// The file that says what a response varies on is a file too.
	s.PurgeAll()
	for i := range 20 {
		key := fmt.Sprintf("varied-%d", i)
		if err := storePut(t, s, key, []string{"accept-language"}, http.Header{"Accept-Language": {"fr"}}, []byte("bonjour")); err != nil {
			t.Fatal(err)
		}
		if n := countFiles(); n > maxFiles {
			t.Fatalf("%d files on disk with varied responses, over the limit of %d", n, maxFiles)
		}
	}
	if got, _ := storeGet(t, s, "varied-19", http.Header{"Accept-Language": {"fr"}}); string(got) != "bonjour" {
		t.Error("the most recent varied response is gone")
	}

	// Downloads in progress count: when they take every slot there is no
	// room for another, and room again once one ends.
	s.PurgeAll()
	create := func(i int) (*Writer, error) {
		now := time.Now()
		rec := &record{stored: now.UnixMilli(), fresh: now.Add(time.Hour).UnixMilli(), status: http.StatusOK}
		return s.Create(fmt.Sprintf("download-%d", i), nil, nil, rec, 0, -1, 1)
	}
	var writers []*Writer
	for i := range maxFiles {
		w, err := create(i)
		if err != nil {
			t.Fatalf("download %d refused: %v", i, err)
		}
		writers = append(writers, w)
	}
	if w, err := create(maxFiles); err == nil {
		w.Abort()
		t.Error("a download was accepted over the limit")
	}
	if n := countFiles(); n > maxFiles {
		t.Errorf("%d files on disk during downloads, over the limit of %d", n, maxFiles)
	}
	writers[0].Abort()
	if err := writers[1].Commit(); err != nil {
		t.Fatal(err)
	}
	w, err := create(maxFiles)
	if err != nil {
		t.Fatalf("no room after a download ended: %v", err)
	}
	w.Abort()
	for _, w := range writers[2:] {
		w.Abort()
	}
	if n := s.tempFiles.Load(); n != 0 {
		t.Errorf("%d downloads still counted", n)
	}

	// Lowering the limit evicts down to it; lifting it stops limiting.
	for i := range maxFiles {
		_ = storePut(t, s, fmt.Sprintf("key-%d", i), nil, nil, []byte("x"))
	}
	s.SetLimits(Limits{MaxSize: 64 << 20, MaxFiles: 3})
	if n := s.Stats().Entries; n != 3 || countFiles() != 3 {
		t.Errorf("%d entries and %d files after lowering the limit to 3", n, countFiles())
	}
	s.SetLimits(Limits{MaxSize: 64 << 20})
	for i := range 30 {
		_ = storePut(t, s, fmt.Sprintf("key-%d", i), nil, nil, []byte("x"))
	}
	if n := s.Stats().Entries; n != 30 {
		t.Errorf("%d entries without a limit, want 30", n)
	}
}

// TestStoreKeepsWhatFits checks that storing a response evicts no more than
// the room it needs.
func TestStoreKeepsWhatFits(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{MaxSize: 1 << 20})

	for i := range 10 {
		key := fmt.Sprintf("small-%d", i)
		if err := storePut(t, s, key, nil, nil, bodyFor(key, 50_000)); err != nil {
			t.Fatal(err)
		}
	}
	// 10 × 50kB and one of 400kB fit together in 1MiB.
	if err := storePut(t, s, "large", nil, nil, bodyFor("large", 400_000)); err != nil {
		t.Fatal(err)
	}

	if st := s.Stats(); st.Evicted != 0 || st.Entries != 11 {
		t.Errorf("%d evictions and %d entries, want everything kept", st.Evicted, st.Entries)
	}
}

func TestStoreMemoryTier(t *testing.T) {
	const maxMemory = 2 << 20

	s := openTestStore(t, t.TempDir(), Limits{MaxMemory: maxMemory})

	const objects = 40
	const size = 200_000
	for i := range objects {
		key := fmt.Sprintf("key-%d", i)
		if err := storePut(t, s, key, nil, nil, bodyFor(key, size)); err != nil {
			t.Fatal(err)
		}
	}

	// A response requested repeatedly moves to memory.
	waitFor(t, "a response to be served from memory", func() bool {
		_, hit := storeGet(t, s, "key-0", nil)
		return hit != nil && hit.InMemory()
	})
	if got, _ := storeGet(t, s, "key-0", nil); !bytes.Equal(got, bodyFor("key-0", size)) {
		t.Fatal("the response served from memory differs")
	}

	// Requesting everything cannot take more memory than allowed: 40 objects
	// of 200kB do not fit in 2MiB.
	for range 5 {
		for i := range objects {
			key := fmt.Sprintf("key-%d", i)
			got, hit := storeGet(t, s, key, nil)
			if hit == nil || !bytes.Equal(got, bodyFor(key, size)) {
				t.Fatalf("%s lost or damaged", key)
			}
			if st := s.Stats(); st.MemoryBytes > maxMemory {
				t.Fatalf("%d bytes of memory in use, over the limit of %d", st.MemoryBytes, maxMemory)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	st := s.Stats()
	if st.HotEntries == 0 || st.Promoted == 0 || st.HotHits == 0 {
		t.Errorf("the memory tier was not used: %+v", st)
	}
	if st.HotEntries >= objects {
		t.Errorf("%d responses in memory, more than can fit", st.HotEntries)
	}

	// Lowering the limit frees the memory; turning it off frees it all.
	s.SetLimits(Limits{MaxSize: 64 << 20, MaxMemory: 1 << 20})
	if st := s.Stats(); st.HotBytes+st.IndexBytes > 1<<20 {
		t.Errorf("%d bytes of memory in use after lowering the limit to 1MiB", st.HotBytes+st.IndexBytes)
	}
	// What is no longer used goes back to the system shortly after.
	waitFor(t, "the memory to be given back", func() bool { return s.Stats().MemoryBytes <= 1<<20 })

	s.SetLimits(Limits{MaxSize: 64 << 20})
	if st := s.Stats(); st.HotEntries != 0 || st.HotBytes != 0 {
		t.Errorf("memory still in use with the memory tier off: %+v", st)
	}
	waitFor(t, "all the memory to be given back", func() bool { return s.arena.residentBytes() == 0 })
	if got, _ := storeGet(t, s, "key-0", nil); !bytes.Equal(got, bodyFor("key-0", size)) {
		t.Error("the response is gone with its in-memory copy")
	}
}

func TestStoreIndexCountsAgainstMemory(t *testing.T) {
	const maxMemory = 1 << 20

	s := openTestStore(t, t.TempDir(), Limits{MaxSize: 1 << 30, MaxMemory: maxMemory})

	// Many tiny files: the index alone would outgrow the budget.
	for i := range 6000 {
		if err := storePut(t, s, fmt.Sprintf("key-%d", i), nil, nil, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	st := s.Stats()
	if st.MemoryBytes > maxMemory {
		t.Errorf("%d bytes of memory in use, over the limit of %d", st.MemoryBytes, maxMemory)
	}
	if st.Entries >= 6000 || st.Entries == 0 {
		t.Errorf("%d entries kept", st.Entries)
	}
}

func TestStoreVary(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})

	french := http.Header{"Accept-Language": {"fr"}}
	english := http.Header{"Accept-Language": {"en"}}
	vary := []string{"accept-language"}

	if err := storePut(t, s, "key", vary, french, []byte("bonjour")); err != nil {
		t.Fatal(err)
	}
	if err := storePut(t, s, "key", vary, english, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	if got, _ := storeGet(t, s, "key", french); string(got) != "bonjour" {
		t.Errorf("fr: got %q", got)
	}
	if got, _ := storeGet(t, s, "key", english); string(got) != "hello" {
		t.Errorf("en: got %q", got)
	}
	if _, hit := storeGet(t, s, "key", http.Header{"Accept-Language": {"de"}}); hit != nil {
		t.Error("de: served a response for another language")
	}

	// The variants survive a restart.
	dir := s.dir
	_ = s.Close()
	s = openTestStore(t, dir, Limits{})
	if got, _ := storeGet(t, s, "key", french); string(got) != "bonjour" {
		t.Errorf("fr after restart: got %q", got)
	}

	// Purging the key invalidates all of its variants.
	if !s.Purge("key") {
		t.Error("nothing purged")
	}
	for _, h := range []http.Header{french, english} {
		if _, hit := storeGet(t, s, "key", h); hit != nil {
			t.Error("a variant survived the purge of its key")
		}
	}
	// Even once the key varies again.
	if err := storePut(t, s, "key", vary, english, []byte("hello again")); err != nil {
		t.Fatal(err)
	}
	if _, hit := storeGet(t, s, "key", french); hit != nil {
		t.Error("a purged variant came back")
	}

	// A response that no longer varies replaces the variants.
	if err := storePut(t, s, "key", nil, nil, []byte("same for all")); err != nil {
		t.Fatal(err)
	}
	if got, _ := storeGet(t, s, "key", french); string(got) != "same for all" {
		t.Errorf("got %q once the response stopped varying", got)
	}
}

// TestStoreVaryNamesAreKept checks that a response listing fewer names than
// the ones stored before it joins them instead of replacing them all.
func TestStoreVaryNamesAreKept(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})

	gzip := http.Header{"Accept-Encoding": {"gzip"}, "Accept-Language": {"fr"}}
	plain := http.Header{"Accept-Language": {"fr"}}

	if err := storePut(t, s, "key", []string{"accept-encoding", "accept-language"}, gzip, []byte("compressed")); err != nil {
		t.Fatal(err)
	}
	if err := storePut(t, s, "key", []string{"accept-language"}, plain, []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if got, _ := storeGet(t, s, "key", gzip); string(got) != "compressed" {
		t.Errorf("got %q for the first response once the second was stored", got)
	}
	if got, _ := storeGet(t, s, "key", plain); string(got) != "plain" {
		t.Errorf("got %q for the second response", got)
	}

	// A name that is not listed yet is another matter: what was stored
	// without it cannot be told apart by it.
	if err := storePut(t, s, "key", []string{"accept-language", "origin"}, plain, []byte("new")); err != nil {
		t.Fatal(err)
	}
	for _, h := range []http.Header{gzip, plain} {
		if got, _ := storeGet(t, s, "key", h); string(got) != "new" {
			t.Errorf("got %q once the names were replaced", got)
		}
	}
}

func TestStorePurge(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{})

	keys := []string{"GET-a/1", "GET-a/2", "GET-b/1", "GET-b/2", "GET-c/1"}
	for _, key := range keys {
		if err := storePut(t, s, key, nil, nil, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	if !s.Purge("GET-c/1") || s.Purge("GET-c/1") {
		t.Error("purging a key should succeed once")
	}
	if n := s.PurgeMatch(func(key string) bool { return strings.HasPrefix(key, "GET-a/") }); n != 2 {
		t.Errorf("purged %d responses by prefix, want 2", n)
	}
	if _, hit := storeGet(t, s, "GET-a/1", nil); hit != nil {
		t.Error("a purged response is still served")
	}
	if got, _ := storeGet(t, s, "GET-b/1", nil); string(got) != "GET-b/1" {
		t.Error("a response that was not purged is gone")
	}

	if n := s.PurgeAll(); n != 2 {
		t.Errorf("purged %d responses, want 2", n)
	}
	if st := s.Stats(); st.Entries != 0 || st.DiskBytes != 0 {
		t.Errorf("the cache is not empty: %+v", st)
	}
	files := 0
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && len(info.Name()) == 32 {
			files++
		}
		return nil
	})
	if files != 0 {
		t.Errorf("%d files left after purging everything", files)
	}
}

func TestStoreInactive(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{Inactive: 2 * time.Second})

	for _, key := range []string{"idle", "busy"} {
		if err := storePut(t, s, key, nil, nil, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// The last use of an entry is known to the second.
	time.Sleep(1500 * time.Millisecond)
	if _, hit := storeGet(t, s, "busy", nil); hit == nil {
		t.Fatal("busy is gone")
	}
	time.Sleep(1500 * time.Millisecond)
	s.expireInactive()

	if _, hit := storeGet(t, s, "idle", nil); hit != nil {
		t.Error("the idle response was kept")
	}
	if _, hit := storeGet(t, s, "busy", nil); hit == nil {
		t.Error("the response in use was removed")
	}
}

// TestStoreTail reads responses while they are being written.
func TestStoreTail(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})
	body := bodyFor("tail", 300_000)

	create := func(key string, declared int64) *Writer {
		now := time.Now()
		rec := &record{stored: now.UnixMilli(), fresh: now.Add(time.Hour).UnixMilli(), status: http.StatusOK}
		w, err := s.Create(key, nil, nil, rec, 0, declared, 1)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}

	// Readers that start at different moments all get the whole body, and
	// get its beginning before its end is written.
	w := create("complete", int64(len(body)))
	var wg sync.WaitGroup
	firstRead := make(chan struct{}, 8)
	read := func() {
		defer wg.Done()
		tail, err := w.Tail(context.Background())
		if err != nil {
			t.Errorf("opening the tail: %v", err)
			return
		}
		defer tail.Close()

		head := make([]byte, 1000)
		if _, err := io.ReadFull(tail, head); err != nil {
			t.Errorf("reading the beginning: %v", err)
		}
		firstRead <- struct{}{}
		rest, err := io.ReadAll(tail)
		if err != nil || !bytes.Equal(append(head, rest...), body) {
			t.Errorf("read %d bytes (%v), want the %d of the body", len(head)+len(rest), err, len(body))
		}
	}

	wg.Add(1)
	go read()
	if _, err := w.Write(body[:100_000]); err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go read()
	for range 2 {
		select {
		case <-firstRead:
		case <-time.After(5 * time.Second):
			t.Fatal("a reader got nothing while the response was being written")
		}
	}

	// A range of what is announced can be read as soon as it is there.
	tail, err := w.Tail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if size, err := tail.Seek(0, io.SeekEnd); err != nil || size != int64(len(body)) {
		t.Errorf("size %d, %v", size, err)
	}
	if _, err := tail.Seek(50_000, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 10_000)
	if _, err := io.ReadFull(tail, part); err != nil || !bytes.Equal(part, body[50_000:60_000]) {
		t.Errorf("reading a range: %v", err)
	}
	tail.Close()

	for off := 100_000; off < len(body); off += 50_000 {
		if _, err := w.Write(body[off:min(off+50_000, len(body))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// Once committed the response is found in the cache, not tailed.
	if _, err := w.Tail(context.Background()); err == nil {
		t.Error("tailed a committed response")
	}
	if got, _ := storeGet(t, s, "complete", nil); !bytes.Equal(got, body) {
		t.Error("the committed response differs")
	}

	// A response that is given up fails its readers instead of leaving them
	// with a truncated body.
	w = create("aborted", -1)
	if _, err := w.Write(body[:1000]); err != nil {
		t.Fatal(err)
	}
	tail, err = w.Tail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()
	if _, err := io.ReadFull(tail, make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	failed := make(chan error)
	go func() {
		_, err := tail.Read(make([]byte, 10))
		failed <- err
	}()
	time.Sleep(20 * time.Millisecond)
	w.Abort()
	select {
	case err := <-failed:
		if err == nil || err == io.EOF {
			t.Errorf("reading an aborted response returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reader of an aborted response was left waiting")
	}

	// A reader told where to stop gets what was written up to there even
	// though the response is given up.
	w = create("drained", -1)
	if _, err := w.Write(body[:5000]); err != nil {
		t.Fatal(err)
	}
	drained, err := w.Tail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer drained.Close()
	drained.finishAt(5000)
	w.Abort()
	if got, err := io.ReadAll(drained); err != nil || !bytes.Equal(got, body[:5000]) {
		t.Errorf("read %d bytes (%v) of a response dropped after 5000", len(got), err)
	}

	// A reader whose client left stops waiting.
	w = create("canceled", -1)
	defer w.Abort()
	ctx, cancel := context.WithCancel(context.Background())
	tail2, err := w.Tail(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tail2.Close()
	go func() {
		_, err := tail2.Read(make([]byte, 10))
		failed <- err
	}()
	cancel()
	select {
	case err := <-failed:
		if err != context.Canceled {
			t.Errorf("reading with a canceled context returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled reader was left waiting")
	}
}

// transientUsage returns the memory the transient responses are accounted
// for.
func transientUsage(s *Store) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.transMeta + int64(s.transBlocks)*int64(s.arena.blockSize)
}

// TestStoreMinUses checks that a response is only written to disk once it
// was requested as many times as asked, and is served from memory meanwhile.
func TestStoreMinUses(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxMemory: 4 << 20})

	sizes := map[string]int{"twice": 20_000, "once": 9000, "empty": 0}
	for key, size := range sizes {
		if err := storePutUses(t, s, key, nil, nil, bodyFor(key, size), 2); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if n := cacheFiles(dir); n != 0 || st.Entries != 3 || st.TransientEntries != 3 || st.DiskBytes != 0 || st.Stored != 3 {
		t.Fatalf("%d files after the first request: %+v", n, st)
	}
	if left, _ := os.ReadDir(filepath.Join(dir, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files for responses held in memory", len(left))
	}

	// The second request is served from memory, and has the response
	// written.
	for _, key := range []string{"twice", "empty"} {
		got, hit := storeGet(t, s, key, nil)
		if hit == nil || !hit.InMemory() || !bytes.Equal(got, bodyFor(key, sizes[key])) {
			t.Fatalf("%s: second request not served from memory", key)
		}
	}
	waitFor(t, "the responses requested twice to be written", func() bool { return cacheFiles(dir) == 2 })
	waitPersisted(t, s)
	st = s.Stats()
	if st.TransientEntries != 1 || st.Persisted != 2 || st.DiskBytes == 0 || st.HotEntries != 2 || st.Stored != 3 {
		t.Errorf("after the second request: %+v", st)
	}
	if u := transientUsage(s); u == 0 || u > 4<<20/transientShare {
		t.Errorf("%d bytes accounted for the one response left in memory only", u)
	}

	// It stays in memory, as the copy of a file now.
	got, hit := storeGet(t, s, "twice", nil)
	if hit == nil || !hit.InMemory() || !bytes.Equal(got, bodyFor("twice", sizes["twice"])) {
		t.Error("the response left memory when it was written")
	}

	// Only what was written survives a restart.
	_ = s.Close()
	if n := s.arena.inUse.Load(); n != 0 {
		t.Errorf("%d memory blocks still in use after closing", n)
	}
	s = openTestStore(t, dir, Limits{MaxMemory: 4 << 20})
	if got, hit := storeGet(t, s, "twice", nil); hit == nil || !bytes.Equal(got, bodyFor("twice", sizes["twice"])) {
		t.Error("the response requested twice did not survive the restart")
	}
	if _, hit := storeGet(t, s, "empty", nil); hit == nil {
		t.Error("the empty response requested twice did not survive the restart")
	}
	if _, hit := storeGet(t, s, "once", nil); hit != nil {
		t.Error("the response requested once survived the restart")
	}

	// A response that memory cannot hold is written at once.
	large := bodyFor("large", 4<<20/transientBodyShare+1)
	if err := storePutUses(t, s, "large", nil, nil, large, 2); err != nil {
		t.Fatal(err)
	}
	if got, hit := storeGet(t, s, "large", nil); hit == nil || hit.InMemory() || !bytes.Equal(got, large) {
		t.Error("a response too large for memory was not stored on disk")
	}
	// As all are without memory.
	s.SetLimits(Limits{MaxSize: 64 << 20})
	if err := storePutUses(t, s, "no-memory", nil, nil, []byte("body"), 2); err != nil {
		t.Fatal(err)
	}
	if got, hit := storeGet(t, s, "no-memory", nil); hit == nil || hit.InMemory() || string(got) != "body" {
		t.Error("a response was not stored on disk with the memory off")
	}
	if st := s.Stats(); st.TransientEntries != 0 || st.MemoryBytes != st.IndexBytes {
		t.Errorf("with the memory off: %+v", st)
	}
}

// TestStoreMinUsesCounts follows the count of the requests a response is
// still to get, through what can happen to the response meanwhile.
func TestStoreMinUsesCounts(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxMemory: 4 << 20})

	left := func(key string) int {
		s.mu.Lock()
		defer s.mu.Unlock()

		e := s.index[makeID(key)]
		if e == nil || !e.transient {
			return -1
		}
		return int(e.left)
	}
	drop := func(key string) {
		s.mu.Lock()
		s.dropLocked(s.index[makeID(key)])
		s.mu.Unlock()
	}
	body := bodyFor("key", 10_000)

	// Three requests, the one that stores it included.
	if err := storePutUses(t, s, "key", nil, nil, body, 3); err != nil {
		t.Fatal(err)
	}
	if n := left("key"); n != 2 {
		t.Fatalf("%d requests left after the first, want 2", n)
	}
	storeGet(t, s, "key", nil)
	waitPersisted(t, s)
	if n := left("key"); n != 1 || cacheFiles(dir) != 0 {
		t.Fatalf("%d requests left after the second, want 1 and no file", n)
	}
	storeGet(t, s, "key", nil)
	waitFor(t, "the response to be written at its third request", func() bool { return cacheFiles(dir) == 1 })
	waitPersisted(t, s)

	// A response dropped from memory is not made to start over when it
	// comes back…
	if err := storePutUses(t, s, "dropped", nil, nil, body, 3); err != nil {
		t.Fatal(err)
	}
	drop("dropped")
	if _, hit := storeGet(t, s, "dropped", nil); hit != nil {
		t.Fatal("a dropped response was served")
	}
	if err := storePutUses(t, s, "dropped", nil, nil, body, 3); err != nil {
		t.Fatal(err)
	}
	if n := left("dropped"); n != 1 {
		t.Fatalf("%d requests left for a response stored a second time, want 1", n)
	}
	// …and is written at once the time it has been requested enough.
	drop("dropped")
	if err := storePutUses(t, s, "dropped", nil, nil, body, 3); err != nil {
		t.Fatal(err)
	}
	if n := cacheFiles(dir); n != 2 || left("dropped") != -1 {
		t.Fatalf("%d files, want the response stored a third time on disk", n)
	}
	if st := s.Stats(); st.Dropped != 2 || st.Persisted != 1 {
		t.Errorf("unexpected counters: %+v", st)
	}

	// The requests that join a download count like those that come after.
	now := time.Now()
	rec := &record{stored: now.UnixMilli(), fresh: now.Add(time.Hour).UnixMilli(), status: http.StatusOK}
	w, err := s.Create("joined", nil, nil, rec, 0, int64(len(body)), 2)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		tail, err := w.Tail(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tail.Close()
	}
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a response two requests were served from to be written", func() bool { return cacheFiles(dir) == 3 })
	waitPersisted(t, s)

	// A response found stale at its second request is replaced, not
	// written: its new version is, and only that.
	persisted := s.Stats().Persisted
	if err := storePutUses(t, s, "stale", nil, nil, []byte("old"), 2); err != nil {
		t.Fatal(err)
	}
	_, hit := s.Lookup("stale", nil)
	if hit == nil {
		t.Fatal("not found")
	}
	if err := storePutUses(t, s, "stale", nil, nil, []byte("new"), 2); err != nil {
		t.Fatal(err)
	}
	hit.Close()
	waitPersisted(t, s)
	if got, hit := storeGet(t, s, "stale", nil); string(got) != "new" || hit.InMemory() {
		t.Errorf("got %q, want the new version, from disk", got)
	}
	if n := s.Stats().Persisted; n != persisted || cacheFiles(dir) != 4 {
		t.Errorf("%d responses written from memory and %d files, want the new version stored directly", n-persisted, cacheFiles(dir))
	}

	// The same goes for one the upstream confirmed.
	if err := storePutUses(t, s, "confirmed", nil, nil, []byte("same"), 2); err != nil {
		t.Fatal(err)
	}
	_, hit = s.Lookup("confirmed", nil)
	if hit == nil {
		t.Fatal("not found")
	}
	fresh := *hit.rec
	fresh.header = http.Header{"X-Version": {"2"}}
	if err := s.Rewrite(hit, &fresh, 2); err != nil {
		t.Fatal(err)
	}
	hit.Close()
	waitPersisted(t, s)
	got, hit := storeGet(t, s, "confirmed", nil)
	if string(got) != "same" || hit.InMemory() || hit.rec.header.Get("X-Version") != "2" {
		t.Errorf("got %q, want the confirmed response, from disk", got)
	}
	if n := s.Stats().Persisted; n != persisted || cacheFiles(dir) != 5 {
		t.Errorf("%d responses written from memory and %d files, want the confirmed response stored directly", n-persisted, cacheFiles(dir))
	}

	// A response in memory only that replaces one on disk takes its file
	// away: it would come back at the next start.
	now = time.Now()
	rec = &record{stored: now.UnixMilli(), fresh: now.Add(time.Hour).UnixMilli(), status: http.StatusOK}
	if w, err = s.Create("replaced", nil, nil, rec, 0, -1, 2); err != nil {
		t.Fatal(err)
	}
	if err := storePut(t, s, "replaced", nil, nil, []byte("on disk")); err != nil {
		t.Fatal(err)
	}
	files := cacheFiles(dir)
	if _, err := w.Write([]byte("in memory")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, hit := storeGet(t, s, "replaced", nil); string(got) != "in memory" || !hit.InMemory() {
		t.Errorf("got %q, want the response in memory", got)
	}
	if n := cacheFiles(dir); n != files-1 {
		t.Errorf("%d files, want the one of the replaced response gone from %d", n, files)
	}
}

// TestStoreMinUsesCountsLateReaders checks that a request that comes while a
// response received in memory is being committed counts once among those the
// response waits for, whether it still gets to read it from the writer or
// has to find it in the cache: a request that was not counted would leave the
// response in memory for one more than min_uses asks.
func TestStoreMinUsesCountsLateReaders(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{MaxMemory: 4 << 20})
	body := bodyFor("key", 1000)

	rounds := 3000
	if testing.Short() {
		rounds = 300
	}
	for i := range rounds {
		key := fmt.Sprintf("key-%d", i)
		now := time.Now()
		rec := &record{stored: now.UnixMilli(), fresh: now.Add(time.Hour).UnixMilli(), status: http.StatusOK}
		w, err := s.Create(key, nil, nil, rec, 0, int64(len(body)), 3)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}

		committed := make(chan error, 1)
		go func() { committed <- w.Commit() }()
		// The request comes at about any point of the commit.
		for range i % 50 {
			runtime.Gosched()
		}
		tail, _ := w.Tail(context.Background())
		if err := <-committed; err != nil {
			t.Fatal(err)
		}
		if tail != nil {
			tail.Close()
		} else if _, hit := storeGet(t, s, key, nil); hit == nil {
			t.Fatal("a committed response was not found")
		}

		s.mu.Lock()
		left := s.index[makeID(key)].left
		s.mu.Unlock()
		if left != 2 {
			t.Fatalf("%d requests left after the first (read from the writer: %t), want 2", left, tail != nil)
		}
		s.Purge(key)
	}
}

// TestStoreMinUsesLimits checks that the responses held in memory only stay
// within their share of it, the oldest making way.
func TestStoreMinUsesLimits(t *testing.T) {
	const maxMemory = 1 << 20

	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxMemory: maxMemory})

	const objects = 100
	const size = 40_000
	for i := range objects {
		key := fmt.Sprintf("key-%d", i)
		if err := storePutUses(t, s, key, nil, nil, bodyFor(key, size), 2); err != nil {
			t.Fatal(err)
		}
		if u := transientUsage(s); u > maxMemory/transientShare {
			t.Fatalf("%d bytes in memory only after %d responses, over their share", u, i+1)
		}
		if st := s.Stats(); st.MemoryBytes > maxMemory {
			t.Fatalf("%d bytes of memory in use, over the limit of %d", st.MemoryBytes, maxMemory)
		}
	}

	st := s.Stats()
	if st.Dropped == 0 || st.TransientEntries == 0 || st.TransientEntries >= objects || st.Entries != st.TransientEntries {
		t.Errorf("unexpected state: %+v", st)
	}
	if n := cacheFiles(dir); n != 0 {
		t.Errorf("%d files for responses requested once", n)
	}
	if _, hit := storeGet(t, s, "key-0", nil); hit != nil {
		t.Error("the oldest response is still in memory")
	}

	// The latest is still there, and its second request has it written.
	last := fmt.Sprintf("key-%d", objects-1)
	if got, hit := storeGet(t, s, last, nil); hit == nil || !bytes.Equal(got, bodyFor(last, size)) {
		t.Fatal("the latest response is gone")
	}
	waitFor(t, "the response requested twice to be written", func() bool { return cacheFiles(dir) == 1 })

	// A response that was dropped is remembered: its second request, though
	// it finds nothing, has it written.
	if err := storePutUses(t, s, "key-0", nil, nil, bodyFor("key-0", size), 2); err != nil {
		t.Fatal(err)
	}
	if n := cacheFiles(dir); n != 2 {
		t.Errorf("%d files, want the response stored a second time on disk", n)
	}

	// Responses without a body take no blocks, yet do not pile up.
	for i := range 3000 {
		if err := storePutUses(t, s, fmt.Sprintf("empty-%d", i), nil, nil, nil, 2); err != nil {
			t.Fatal(err)
		}
	}
	if u := transientUsage(s); u > maxMemory/transientShare {
		t.Errorf("%d bytes in memory only, over their share", u)
	}
	if st := s.Stats(); st.MemoryBytes > maxMemory || st.TransientEntries >= 3000 {
		t.Errorf("unexpected state: %+v", st)
	}

	// The responses not requested for too long leave memory too.
	s.SetLimits(Limits{MaxSize: 64 << 20, MaxMemory: maxMemory, Inactive: time.Nanosecond})
	time.Sleep(1100 * time.Millisecond)
	s.expireInactive()
	if st := s.Stats(); st.Entries != 0 || st.HotBytes != 0 {
		t.Errorf("after the inactive period: %+v", st)
	}

	// Lowering the memory drops what no longer fits, turning it off all.
	s.SetLimits(Limits{MaxSize: 64 << 20, MaxMemory: 4 * maxMemory})
	for i := range 20 {
		key := fmt.Sprintf("key-%d", i)
		if err := storePutUses(t, s, "again-"+key, nil, nil, bodyFor(key, size), 2); err != nil {
			t.Fatal(err)
		}
	}
	if st := s.Stats(); st.TransientEntries != 20 {
		t.Fatalf("%d responses in memory only, want 20", st.TransientEntries)
	}
	s.SetLimits(Limits{MaxSize: 64 << 20, MaxMemory: maxMemory})
	if u := transientUsage(s); u > maxMemory/transientShare || u == 0 {
		t.Errorf("%d bytes in memory only after lowering the limit", u)
	}
	s.SetLimits(Limits{MaxSize: 64 << 20})
	if st := s.Stats(); st.Entries != 0 || st.HotBytes != 0 || transientUsage(s) != 0 {
		t.Errorf("with the memory off: %+v", st)
	}
	waitFor(t, "all the memory to be given back", func() bool { return s.arena.residentBytes() == 0 })
}

// TestStoreMinUsesVary checks that what tells the variants of a response
// apart is written to disk with the first of them that is.
func TestStoreMinUsesVary(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxMemory: 4 << 20})

	french := http.Header{"Accept-Language": {"fr"}}
	english := http.Header{"Accept-Language": {"en"}}
	german := http.Header{"Accept-Language": {"de"}}
	vary := []string{"accept-language"}

	if err := storePutUses(t, s, "key", vary, french, []byte("bonjour"), 2); err != nil {
		t.Fatal(err)
	}
	if err := storePutUses(t, s, "key", vary, english, []byte("hello"), 2); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); cacheFiles(dir) != 0 || st.Entries != 3 || st.TransientEntries != 3 {
		t.Fatalf("%d files after the first requests: %+v", cacheFiles(dir), st)
	}

	if got, _ := storeGet(t, s, "key", french); string(got) != "bonjour" {
		t.Errorf("fr: got %q", got)
	}
	waitFor(t, "the variant requested twice to be written", func() bool { return cacheFiles(dir) == 2 })
	waitPersisted(t, s)
	if st := s.Stats(); st.TransientEntries != 1 {
		t.Errorf("%d entries in memory only, want the other variant", st.TransientEntries)
	}

	// A variant written at once does the same.
	other := t.TempDir()
	s2 := openTestStore(t, other, Limits{MaxMemory: 4 << 20})
	if err := storePutUses(t, s2, "key", vary, french, []byte("bonjour"), 2); err != nil {
		t.Fatal(err)
	}
	if err := storePut(t, s2, "key", vary, german, []byte("hallo")); err != nil {
		t.Fatal(err)
	}
	if n := cacheFiles(other); n != 2 {
		t.Errorf("%d files, want the variant and what leads to it", n)
	}
	_ = s2.Close()
	s2 = openTestStore(t, other, Limits{})
	if got, _ := storeGet(t, s2, "key", german); string(got) != "hallo" {
		t.Errorf("de after restart: got %q", got)
	}

	// The variants on disk are found after a restart, the others are gone.
	_ = s.Close()
	s = openTestStore(t, dir, Limits{MaxMemory: 4 << 20})
	if got, _ := storeGet(t, s, "key", french); string(got) != "bonjour" {
		t.Errorf("fr after restart: got %q", got)
	}
	if _, hit := storeGet(t, s, "key", english); hit != nil {
		t.Error("en: a variant requested once survived the restart")
	}

	// A variant nothing leads to anymore is not worth a file.
	if err := storePutUses(t, s, "other", vary, french, []byte("bonjour"), 2); err != nil {
		t.Fatal(err)
	}
	_, hit := s.Lookup("other", french)
	if hit == nil {
		t.Fatal("not found")
	}
	if err := storePutUses(t, s, "other", nil, nil, []byte("same for all"), 2); err != nil {
		t.Fatal(err)
	}
	files, persisted := cacheFiles(dir), s.Stats().Persisted
	hit.Close()
	waitPersisted(t, s)
	if n := cacheFiles(dir); n != files || s.Stats().Persisted != persisted {
		t.Errorf("%d files, want %d: a variant nothing leads to was written", n, files)
	}
	if got, _ := storeGet(t, s, "other", french); string(got) != "same for all" {
		t.Errorf("got %q once the response stopped varying", got)
	}
}

// TestStoreMinUsesTail reads responses received in memory while they are,
// one of which outgrows memory on its way.
func TestStoreMinUsesTail(t *testing.T) {
	const maxMemory = 1 << 20

	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxMemory: maxMemory})
	body := bodyFor("tail", 300_000)

	create := func(key string, declared int64) *Writer {
		now := time.Now()
		rec := &record{stored: now.UnixMilli(), fresh: now.Add(time.Hour).UnixMilli(), status: http.StatusOK}
		// More requests than the test makes, so that nothing is written
		// to disk that does not have to be.
		w, err := s.Create(key, nil, nil, rec, 0, declared, 10)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	tmpFiles := func() int {
		left, _ := os.ReadDir(filepath.Join(dir, tmpDirName))
		return len(left)
	}
	var wg sync.WaitGroup
	read := func(w *Writer, want []byte, started chan<- struct{}) {
		defer wg.Done()
		tail, err := w.Tail(context.Background())
		if err != nil {
			t.Errorf("opening the tail: %v", err)
			close(started)
			return
		}
		defer tail.Close()

		head := make([]byte, 1000)
		if _, err := io.ReadFull(tail, head); err != nil {
			t.Errorf("reading the beginning: %v", err)
		}
		close(started)
		rest, err := io.ReadAll(tail)
		if err != nil || !bytes.Equal(append(head, rest...), want) {
			t.Errorf("read %d bytes (%v), want the %d of the body", len(head)+len(rest), err, len(want))
		}
	}

	// A response that fits is read from memory as it arrives.
	small := body[:100_000]
	w := create("small", int64(len(small)))
	if w.mem == nil || tmpFiles() != 0 {
		t.Fatal("a response that fits in memory is received in a file")
	}
	if _, err := w.Write(small[:30_000]); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	wg.Add(1)
	go read(w, small, started)
	<-started
	// A range of what is announced can be read as soon as it is there.
	tail, err := w.Tail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tail.Seek(20_000, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 5000)
	if _, err := io.ReadFull(tail, part); err != nil || !bytes.Equal(part, small[20_000:25_000]) {
		t.Errorf("reading a range: %v", err)
	}
	tail.Close()
	for off := 30_000; off < len(small); off += 7000 {
		if _, err := w.Write(small[off:min(off+7000, len(small))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, err := w.Tail(context.Background()); err == nil {
		t.Error("tailed a committed response")
	}
	if got, hit := storeGet(t, s, "small", nil); !bytes.Equal(got, small) || !hit.InMemory() || cacheFiles(dir) != 0 {
		t.Error("the committed response differs, or is not in memory only")
	}

	// One of unknown length that outgrows what memory may hold of it moves
	// to a file, which its readers do not notice.
	w = create("grown", -1)
	if w.mem == nil {
		t.Fatal("a response of unknown length is not received in memory")
	}
	if _, err := w.Write(body[:100_000]); err != nil {
		t.Fatal(err)
	}
	started = make(chan struct{})
	wg.Add(1)
	go read(w, body, started)
	<-started
	if w.mem == nil || tmpFiles() != 0 {
		t.Fatal("the response left memory before it had to")
	}
	for off := 100_000; off < len(body); off += 50_000 {
		if _, err := w.Write(body[off:min(off+50_000, len(body))]); err != nil {
			t.Fatal(err)
		}
	}
	if w.mem != nil || tmpFiles() != 1 {
		t.Fatal("a response larger than memory may hold is still received there")
	}
	// A reader that comes after reads the file.
	started = make(chan struct{})
	wg.Add(1)
	go read(w, body, started)
	<-started
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if got, hit := storeGet(t, s, "grown", nil); !bytes.Equal(got, body) || hit.InMemory() {
		t.Error("the response that outgrew memory differs, or is not on disk")
	}
	if cacheFiles(dir) != 1 || tmpFiles() != 0 {
		t.Errorf("%d files and %d temporary ones, want the response on disk", cacheFiles(dir), tmpFiles())
	}
	if u := transientUsage(s); u > maxMemory/transientShare {
		t.Errorf("%d bytes accounted in memory only", u)
	}

	// A response that is given up fails its readers, except the one told
	// where to stop.
	w = create("aborted", -1)
	if _, err := w.Write(body[:5000]); err != nil {
		t.Fatal(err)
	}
	failing, err := w.Tail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer failing.Close()
	drained, err := w.Tail(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer drained.Close()
	drained.finishAt(5000)
	w.Abort()
	if got, err := io.ReadAll(failing); err == nil {
		t.Errorf("read %d bytes of an aborted response without error", len(got))
	}
	if got, err := io.ReadAll(drained); err != nil || !bytes.Equal(got, body[:5000]) {
		t.Errorf("read %d bytes (%v) of a response dropped after 5000", len(got), err)
	}
	if _, hit := storeGet(t, s, "aborted", nil); hit != nil {
		t.Error("an aborted response was stored")
	}

	// The memory of what is not stored is given back once nobody reads it.
	failing.Close()
	drained.Close()
	s.Purge("small")
	if n := s.arena.inUse.Load(); n != 0 || transientUsage(s) != 0 {
		t.Errorf("%d memory blocks in use, %d bytes accounted, with nothing in memory", n, transientUsage(s))
	}
}

func TestStoreFlights(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{})
	id := makeID("key")

	leaderFlight, leader := s.BeginFlight(id)
	if !leader {
		t.Fatal("the first request should lead")
	}
	follower, leader := s.BeginFlight(id)
	if leader || follower != leaderFlight {
		t.Fatal("the second request should wait for the first")
	}

	s.EndFlight(id, leaderFlight, true)
	select {
	case <-follower.done:
		if !follower.stored {
			t.Error("the waiter was not told the response is stored")
		}
	default:
		t.Fatal("the waiter was not released")
	}
	// Ending twice is harmless.
	s.EndFlight(id, leaderFlight, false)

	if _, leader := s.BeginFlight(id); !leader {
		t.Error("the next request should lead again")
	}

	// A response that is stored under another ID than the one it was
	// fetched under, because it varies, is waited for under both.
	fl, _ := s.BeginFlight(id)
	w, err := s.Create("key", []string{"accept-language"}, http.Header{"Accept-Language": {"fr"}}, &record{status: http.StatusOK}, 0, 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if w.id == id {
		t.Fatal("a variant is stored under the ID of its key")
	}
	s.ShareFlight(id, fl, w)
	if joined, leader := s.BeginFlight(w.id); leader || joined != fl {
		t.Error("a request for the variant being fetched began a fetch of its own")
	}
	if s.SharedFlight(w.id) != w {
		t.Error("the variant being fetched is not found under its own ID")
	}
	s.EndFlight(id, fl, false)
	if _, leader := s.BeginFlight(w.id); !leader {
		t.Error("the flight outlived its end under the ID of the variant")
	}

	if s.Uncacheable(id) {
		t.Error("uncacheable before being told")
	}
	s.SetUncacheable(id, true)
	if !s.Uncacheable(id) {
		t.Error("not remembered as uncacheable")
	}
	s.SetUncacheable(id, false)
	if s.Uncacheable(id) {
		t.Error("still uncacheable after being cleared")
	}
}

// TestStoreStress hammers a store that is too small for its load from many
// goroutines. Every body read must be the one of its key, whole: a response
// served from memory that was freed, or from a file that was replaced, would
// show here. Run with -race.
func TestStoreStress(t *testing.T) {
	// With min_uses the responses start in memory only, and reach the disk
	// from there.
	for _, minUses := range []int{1, 2} {
		t.Run(fmt.Sprintf("min_uses=%d", minUses), func(t *testing.T) {
			stressStore(t, minUses)
		})
	}
}

func stressStore(t *testing.T, minUses int) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Limits{MaxSize: 3 << 20, MaxMemory: 1 << 20})

	const (
		workers = 16
		keys    = 64
	)
	duration := 2 * time.Second
	if testing.Short() {
		duration = 300 * time.Millisecond
	}
	sizeOf := func(k int) int { return 500 + k*1500 }

	stop := time.Now().Add(duration)
	var wg sync.WaitGroup
	errs := make(chan error, workers)

	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 42))

			for time.Now().Before(stop) {
				// A skewed choice, so that some keys are popular enough to
				// be kept in memory.
				k := int(float64(keys) * rng.Float64() * rng.Float64())
				key := fmt.Sprintf("key-%d", k)

				switch op := rng.IntN(100); {
				case op < 70:
					_, hit := s.Lookup(key, nil)
					if hit == nil {
						if err := storePutUses(t, s, key, nil, nil, bodyFor(key, sizeOf(k)), minUses); err != nil {
							errs <- fmt.Errorf("storing %s: %w", key, err)
							return
						}
						continue
					}
					var buf bytes.Buffer
					// Hold the response for a while, as a slow client does.
					if rng.IntN(4) == 0 {
						time.Sleep(time.Millisecond)
					}
					err := hit.WriteBody(&buf)
					hit.Close()
					if err != nil {
						errs <- fmt.Errorf("reading %s: %w", key, err)
						return
					}
					if !bytes.Equal(buf.Bytes(), bodyFor(key, sizeOf(k))) {
						errs <- fmt.Errorf("%s: served %d bytes that are not its body", key, buf.Len())
						return
					}
				case op < 90:
					if err := storePutUses(t, s, key, nil, nil, bodyFor(key, sizeOf(k)), minUses); err != nil {
						errs <- fmt.Errorf("storing %s: %w", key, err)
						return
					}
				case op < 97:
					s.Purge(key)
				case op < 99:
					s.PurgeMatch(func(candidate string) bool { return candidate == key })
				default:
					s.SetLimits(Limits{MaxSize: int64(2+rng.IntN(3)) << 20, MaxMemory: int64(1+rng.IntN(2)) << 20})
				}

				st := s.Stats()
				if st.DiskBytes > 4<<20 || st.MemoryBytes > 2<<20+int64(workers)*int64(sizeOf(keys)) {
					errs <- fmt.Errorf("limits exceeded: %+v", st)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	waitPersisted(t, s)
	st := s.Stats()
	t.Logf("%+v", st)
	if st.HotHits == 0 {
		t.Error("nothing was ever served from memory")
	}
	if minUses > 1 && st.Persisted == 0 {
		t.Error("nothing was ever written to disk from memory")
	}

	// Once idle, the accounting must match the disk exactly.
	limits := s.Limits()
	var onDisk int64
	files := 0
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && len(info.Name()) == 32 {
			onDisk += (info.Size() + diskBlock - 1) / diskBlock * diskBlock
			files++
		}
		return nil
	})
	if files != st.Entries-st.TransientEntries || onDisk != st.DiskBytes || onDisk > limits.MaxSize {
		t.Errorf("%d files of %d bytes on disk, %d entries of %d bytes accounted, %d of them in memory only, limit %d",
			files, onDisk, st.Entries, st.DiskBytes, st.TransientEntries, limits.MaxSize)
	}
	if left, _ := os.ReadDir(filepath.Join(dir, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}
	// And so must that of the memory the transient responses take.
	s.mu.Lock()
	blocks, meta, transients := 0, int64(0), 0
	for _, e := range s.index {
		if e.transient {
			transients++
		}
		if e.transient && e.hot != nil {
			blocks += len(e.hot.body.ids)
			meta += e.cost + e.hot.cost
		}
	}
	if blocks != s.transBlocks || meta != s.transMeta || transients != s.transients {
		t.Errorf("%d transient entries with %d blocks and %d bytes of index, %d with %d and %d accounted",
			transients, blocks, meta, s.transients, s.transBlocks, s.transMeta)
	}
	s.mu.Unlock()

	// Closing with nothing in flight gives all the memory back.
	_ = s.Close()
	if n := s.arena.inUse.Load(); n != 0 {
		t.Errorf("%d memory blocks still in use after closing", n)
	}
	if s.arena.segs != nil {
		t.Error("the memory was not unmapped after closing")
	}
}

func TestStoreClosed(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Limits{MaxMemory: 1 << 20})
	if err := storePut(t, s, "key", nil, nil, []byte("body")); err != nil {
		t.Fatal(err)
	}
	_, hit := s.Lookup("key", nil)
	if hit == nil {
		t.Fatal("not found")
	}

	_ = s.Close()

	// A request in flight finishes.
	var buf bytes.Buffer
	if err := hit.WriteBody(&buf); err != nil || buf.String() != "body" {
		t.Errorf("reading after close: %q, %v", buf.String(), err)
	}
	hit.Close()

	// New ones are told there is nothing, without failing.
	if _, hit := s.Lookup("key", nil); hit != nil {
		t.Error("a closed store served a response")
	}
	if err := storePut(t, s, "other", nil, nil, []byte("body")); err == nil {
		t.Error("a closed store accepted a response")
	}
	if err := s.Close(); err != nil {
		t.Errorf("closing twice: %v", err)
	}
}

func TestArena(t *testing.T) {
	a := newArena()
	bs := int64(a.blockSize)

	if a.alloc(1) != nil {
		t.Fatal("allocated without a limit set")
	}

	a.setLimit(10)
	b1 := a.alloc(4 * bs)
	b2 := a.alloc(5*bs + 1)
	if b1 == nil || b2 == nil {
		t.Fatal("allocation within the limit failed")
	}
	if a.alloc(1) != nil {
		t.Fatal("allocated over the limit")
	}

	// Blocks are private to their blob.
	for i, blk := range b1.blocks {
		for j := range blk {
			blk[j] = byte(i + 1)
		}
	}
	for _, blk := range b2.blocks {
		for j := range blk {
			if blk[j] != 0 {
				t.Fatal("blobs share memory")
			}
		}
	}

	// A reader keeps a blob alive after the index let go of it.
	b1.acquire()
	b1.release()
	if a.inUse.Load() != 10 {
		t.Fatalf("%d blocks in use, want 10", a.inUse.Load())
	}
	if b1.blocks[3][0] != 4 {
		t.Fatal("a blob still referenced was altered")
	}
	b1.release()
	if a.inUse.Load() != 6 {
		t.Fatalf("%d blocks in use after a release, want 6", a.inUse.Load())
	}

	// Lowering the limit returns the free memory to the system.
	a.setLimit(6)
	if got := a.residentBytes(); got != 6*bs {
		t.Errorf("%d bytes resident, want %d", got, 6*bs)
	}
	// And the blocks given back come back zeroed.
	a.setLimit(10)
	b3 := a.alloc(4 * bs)
	if b3 == nil {
		t.Fatal("allocation after raising the limit failed")
	}
	b3.release()

	a.retire()
	if a.alloc(1) != nil {
		t.Error("a retired arena allocated")
	}
	if a.segs == nil {
		t.Error("the memory was unmapped while a blob was in use")
	}
	b2.release()
	if a.segs != nil {
		t.Error("the memory was not unmapped once the last blob was released")
	}
}

func TestBlobReader(t *testing.T) {
	a := newArena()
	a.setLimit(100)

	body := bodyFor("blob", 3*a.blockSize+123)
	path := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(path, append([]byte("head"), body...), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	b := a.alloc(int64(len(body)))
	if err := b.fill(f, 4); err != nil {
		t.Fatal(err)
	}
	defer b.release()

	got, err := io.ReadAll(&blobReader{b: b})
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read %d bytes (%v), want the %d of the body", len(got), err, len(body))
	}

	r := &blobReader{b: b}
	for _, off := range []int64{0, 1, int64(a.blockSize) - 1, int64(a.blockSize), int64(len(body)) - 1} {
		if _, err := r.Seek(off, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		// An odd buffer size, to cross block boundaries.
		buf := make([]byte, 777)
		n, _ := io.ReadFull(r, buf)
		if !bytes.Equal(buf[:n], body[off:min(int(off)+777, len(body))]) {
			t.Errorf("wrong data at offset %d", off)
		}
	}
	if size, _ := r.Seek(0, io.SeekEnd); size != int64(len(body)) {
		t.Errorf("size %d, want %d", size, len(body))
	}
}
