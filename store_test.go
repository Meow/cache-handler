package httpcache

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
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

	now := time.Now()
	rec := &record{
		stored: now.UnixMilli(),
		fresh:  now.Add(time.Hour).UnixMilli(),
		status: http.StatusOK,
		header: http.Header{"Content-Type": {"application/octet-stream"}, "X-Key": {key}},
	}

	w, err := s.Create(key, vary, reqHeader, rec, 0)
	if err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		w.Abort()
		return err
	}

	return w.Commit()
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
	if st := s.Stats(); st.MemoryBytes > 1<<20 {
		t.Errorf("%d bytes of memory in use after lowering the limit to 1MiB", st.MemoryBytes)
	}
	s.SetLimits(Limits{MaxSize: 64 << 20})
	if st := s.Stats(); st.HotEntries != 0 || st.HotBytes != 0 {
		t.Errorf("memory still in use with the memory tier off: %+v", st)
	}
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
						if err := storePut(t, s, key, nil, nil, bodyFor(key, sizeOf(k))); err != nil {
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
					if err := storePut(t, s, key, nil, nil, bodyFor(key, sizeOf(k))); err != nil {
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

	st := s.Stats()
	t.Logf("%+v", st)
	if st.HotHits == 0 {
		t.Error("nothing was ever served from memory")
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
	if files != st.Entries || onDisk != st.DiskBytes || onDisk > limits.MaxSize {
		t.Errorf("%d files of %d bytes on disk, %d entries of %d bytes accounted, limit %d", files, onDisk, st.Entries, st.DiskBytes, limits.MaxSize)
	}
	if left, _ := os.ReadDir(filepath.Join(dir, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}

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
