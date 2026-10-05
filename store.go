package httpcache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	// entryOverhead is the memory an index entry takes besides its key.
	entryOverhead = 208
	// diskBlock is the granularity files are accounted with, so that many
	// small files count for the space they really take.
	diskBlock = 4096
	// decaySeconds is the period after which the hit count of an entry is
	// halved. It is what lets a formerly popular entry leave memory.
	decaySeconds = 300
	// promoteAfter is how many recent hits an entry needs to be copied to
	// memory.
	promoteAfter = 2
	// passMemoTTL is how long a key is remembered as uncacheable, during
	// which its requests are not made to wait for each other.
	passMemoTTL = time.Minute
	maxPassMemo = 16384

	tmpDirName  = "tmp"
	lockName    = ".lock"
	cacheDirTag = "CACHEDIR.TAG"
)

var (
	errStoreClosed = errors.New("cache store is closed")
	errTooLarge    = errors.New("response too large to cache")
	errNoSpace     = errors.New("no cache space left for the response")
)

// Limits bound the resources a Store uses.
type Limits struct {
	// MaxSize is the disk space the cache may take, in bytes.
	MaxSize int64
	// MaxMemory is the RAM the cache may take, in bytes: its index plus the
	// responses it keeps in memory. Zero keeps every response on disk only
	// and leaves the index unbounded.
	MaxMemory int64
	// Inactive removes the responses that were not requested for that long.
	// Zero keeps them until the space is needed.
	Inactive time.Duration
}

// entry is what the index knows about a cache file.
type entry struct {
	id  ID
	key string
	// vary is set on markers only, see record.vary.
	vary string
	// size and cost are the disk and index space the entry is accounted for.
	size  int64
	cost  int64
	atime int64
	// hits counts the recent requests, halved every decay period.
	hits      uint16
	epoch     uint32
	marker    bool
	promoting bool
	hot       *hotData

	prev, next   *entry
	hprev, hnext *entry
}

// hotData is the in-memory copy of a response.
type hotData struct {
	rec  *record
	body *blob
	cost int64
}

// StoreStats is a snapshot of the state of a Store.
type StoreStats struct {
	Path        string `json:"path"`
	Loading     bool   `json:"loading"`
	Entries     int    `json:"entries"`
	DiskBytes   int64  `json:"disk_bytes"`
	MaxSize     int64  `json:"max_size"`
	MemoryBytes int64  `json:"memory_bytes"`
	MaxMemory   int64  `json:"max_memory"`
	IndexBytes  int64  `json:"index_bytes"`
	HotEntries  int    `json:"hot_entries"`
	HotBytes    int64  `json:"hot_bytes"`
	Hits        int64  `json:"hits"`
	HotHits     int64  `json:"hot_hits"`
	Misses      int64  `json:"misses"`
	Stored      int64  `json:"stored"`
	Evicted     int64  `json:"evicted"`
	Promoted    int64  `json:"promoted"`
}

// Store is a two-tier response cache: every response is a file under dir and
// the most requested ones are also held in memory. Both tiers are bounded.
//
// Files are written to a temporary name and renamed into place, so a cache
// file is always complete and never changes. The index is rebuilt from the
// files when the store opens.
type Store struct {
	dir  string
	log  *zap.Logger
	lock *os.File

	mu       sync.Mutex
	index    map[ID]*entry
	head     *entry
	tail     *entry
	hotHead  *entry
	hotTail  *entry
	hotCount int
	metaUsed int64
	limits   Limits
	loaded   bool
	closed   bool
	// gen changes when the whole cache is purged, which invalidates what a
	// concurrent loader read from disk.
	gen uint64

	// diskUsed is written under mu and read without it.
	diskUsed  atomic.Int64
	tempBytes atomic.Int64
	maxSize   atomic.Int64

	arena     *arena
	promoteCh chan *entry

	// stripes serialize the operations that change which file an ID names.
	stripes  [256]sync.Mutex
	dirMade  [256]atomic.Bool
	markerMu sync.Mutex

	fmu      sync.Mutex
	flights  map[ID]*flight
	passMemo map[ID]int64

	hits, hotHits, misses, stored, evicted, promoted atomic.Int64
	lastWarn                                         atomic.Int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// OpenStore opens the cache held in dir, creating it if needed. The existing
// files are indexed in the background; until that is done they are found on
// demand.
func OpenStore(dir string, limits Limits, log *zap.Logger) (*Store, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, tmpDirName), 0o700); err != nil {
		return nil, fmt.Errorf("creating the cache directory: %w", err)
	}

	lock, err := lockDir(filepath.Join(dir, lockName))
	if err != nil {
		return nil, fmt.Errorf("locking the cache directory %s: %w", dir, err)
	}

	tag := filepath.Join(dir, cacheDirTag)
	if _, err := os.Stat(tag); errors.Is(err, os.ErrNotExist) {
		_ = os.WriteFile(tag, []byte("Signature: 8a477f597d28d172789f06886806bc55\n# This directory holds the HTTP cache of Caddy.\n"), 0o600)
	}

	// Whatever is left in tmp belongs to downloads a previous process did
	// not finish.
	if leftovers, err := os.ReadDir(filepath.Join(dir, tmpDirName)); err == nil {
		for _, f := range leftovers {
			_ = os.Remove(filepath.Join(dir, tmpDirName, f.Name()))
		}
	}

	if log == nil {
		log = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{
		dir:       dir,
		log:       log,
		lock:      lock,
		index:     make(map[ID]*entry),
		arena:     newArena(),
		promoteCh: make(chan *entry, 256),
		flights:   make(map[ID]*flight),
		passMemo:  make(map[ID]int64),
		cancel:    cancel,
	}
	s.SetLimits(limits)

	s.wg.Add(3)
	go s.load(ctx)
	go s.promoter(ctx)
	go s.janitor(ctx)

	return s, nil
}

// Destruct implements caddy.Destructor.
func (s *Store) Destruct() error {
	return s.Close()
}

// Close stops the store. The files stay on disk for the next one to pick up.
// Requests still being served from the store finish normally.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for s.hotTail != nil {
		s.demoteLocked(s.hotTail)
	}
	s.mu.Unlock()

	s.cancel()
	s.wg.Wait()
	s.arena.retire()

	return s.lock.Close()
}

// SetLimits changes the limits, evicting whatever no longer fits.
func (s *Store) SetLimits(limits Limits) {
	s.mu.Lock()
	s.limits = limits
	s.maxSize.Store(limits.MaxSize)
	victims := s.enforceLocked()
	s.mu.Unlock()

	s.unlink(victims)
}

// Limits returns the current limits.
func (s *Store) Limits() Limits {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.limits
}

// Stats returns a snapshot of the store.
func (s *Store) Stats() StoreStats {
	resident := s.arena.residentBytes()

	s.mu.Lock()
	defer s.mu.Unlock()

	return StoreStats{
		Path:        s.dir,
		Loading:     !s.loaded,
		Entries:     len(s.index),
		DiskBytes:   s.diskUsed.Load(),
		MaxSize:     s.limits.MaxSize,
		MemoryBytes: s.metaUsed + resident,
		MaxMemory:   s.limits.MaxMemory,
		IndexBytes:  s.metaUsed,
		HotEntries:  s.hotCount,
		HotBytes:    s.arena.inUse.Load() * int64(s.arena.blockSize),
		Hits:        s.hits.Load(),
		HotHits:     s.hotHits.Load(),
		Misses:      s.misses.Load(),
		Stored:      s.stored.Load(),
		Evicted:     s.evicted.Load(),
		Promoted:    s.promoted.Load(),
	}
}

func (s *Store) path(id ID) string {
	name := id.String()

	return filepath.Join(s.dir, name[:2], name)
}

// maxObject is the largest file the store accepts: an object that takes more
// than half of the cache would evict nearly everything else.
func (s *Store) maxObject() int64 {
	return s.maxSize.Load() / 2
}

func (s *Store) warn(msg string, err error) {
	now := time.Now().Unix()
	if last := s.lastWarn.Load(); now-last < 10 || !s.lastWarn.CompareAndSwap(last, now) {
		return
	}
	s.log.Warn(msg, zap.String("path", s.dir), zap.Error(err))
}

// Lookup finds the response stored for key that matches the request headers.
// It returns the ID to coordinate a fetch on and, when there is a response,
// a Hit the caller must close.
func (s *Store) Lookup(key string, reqHeader http.Header) (ID, *Hit) {
	full := key
	id := makeID(key)

	// The first round finds the response or the marker telling which request
	// headers select it, the second the selected response.
	for range 2 {
		e, hit := s.pin(id)
		if e == nil {
			break
		}
		if e.marker {
			if full != key {
				break
			}
			full = variantKey(key, e.vary, reqHeader)
			id = makeID(full)
			continue
		}

		if hit == nil {
			var err error
			if hit, err = s.open(e, full); err != nil {
				s.discard(e)
				break
			}
		} else if hit.rec.key != full {
			hit.Close()
			break
		}
		s.hits.Add(1)

		return id, hit
	}
	s.misses.Add(1)

	return id, nil
}

// pin finds the entry of id and marks it as used. When the entry is in
// memory it also returns a Hit holding a reference on it.
func (s *Store) pin(id ID) (*entry, *Hit) {
	s.mu.Lock()
	e := s.index[id]
	if e == nil && !s.loaded && !s.closed {
		// The loader has not reached this file yet.
		gen := s.gen
		s.mu.Unlock()
		s.loadFile(id, gen, true)
		s.mu.Lock()
		e = s.index[id]
	}
	if e == nil || s.closed {
		s.mu.Unlock()
		return nil, nil
	}

	now := time.Now().Unix()
	e.atime = now
	s.bumpLocked(e, now)
	if s.head != e {
		s.lruRemove(e)
		s.lruPushFront(e)
	}

	var hit *Hit
	switch {
	case e.hot != nil:
		if s.hotHead != e {
			s.hotRemove(e)
			s.hotPushFront(e)
		}
		e.hot.body.acquire()
		hit = &Hit{s: s, e: e, rec: e.hot.rec, blob: e.hot.body}
		s.hotHits.Add(1)
	case s.promotableLocked(e, now):
		select {
		case s.promoteCh <- e:
			e.promoting = true
		default:
		}
	}
	s.mu.Unlock()

	return e, hit
}

func (s *Store) open(e *entry, key string) (*Hit, error) {
	f, err := os.Open(s.path(e.id))
	if err != nil {
		return nil, err
	}

	rec, err := statRecord(f)
	if err == nil && (rec.key != key || rec.marker()) {
		err = errCorrupt
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &Hit{s: s, e: e, rec: rec, f: f}, nil
}

func statRecord(f *os.File) (*record, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return readRecord(f, fi.Size())
}

// discard forgets an entry whose file turned out to be missing or unusable.
func (s *Store) discard(e *entry) {
	s.mu.Lock()
	present := s.index[e.id] == e
	if present {
		s.removeLocked(e)
	}
	s.mu.Unlock()

	if present {
		s.unlink([]ID{e.id})
	}
}

// Hit is a stored response pinned for one request. Its content stays
// readable until Close even if the entry is evicted or replaced meanwhile.
type Hit struct {
	s      *Store
	e      *entry
	rec    *record
	f      *os.File
	blob   *blob
	closed bool
}

// Close releases the response. It is safe to call on a nil Hit and more than
// once.
func (h *Hit) Close() {
	if h == nil || h.closed {
		return
	}
	h.closed = true

	if h.f != nil {
		_ = h.f.Close()
	}
	if h.blob != nil {
		h.blob.release()
	}
}

// InMemory tells whether the response is served from memory.
func (h *Hit) InMemory() bool {
	return h.blob != nil
}

// Body returns the body for random access.
func (h *Hit) Body() io.ReadSeeker {
	if h.blob != nil {
		return &blobReader{b: h.blob}
	}

	return io.NewSectionReader(h.f, h.rec.bodyOff, h.rec.bodyLen)
}

// WriteBody copies the whole body to w.
func (h *Hit) WriteBody(w io.Writer) error {
	if h.blob != nil {
		_, err := io.Copy(w, &blobReader{b: h.blob})
		return err
	}

	// Copying from the file itself lets the server use sendfile.
	if _, err := h.f.Seek(h.rec.bodyOff, io.SeekStart); err != nil {
		return err
	}
	_, err := io.CopyN(w, h.f, h.rec.bodyLen)

	return err
}

// Discard removes the response from the cache, for when it cannot be read.
func (h *Hit) Discard() {
	h.s.discard(h.e)
}

// Writer stores one response. The response only becomes visible, whole, when
// Commit succeeds.
type Writer struct {
	s        *Store
	f        *os.File
	info     os.FileInfo
	tmp      string
	rec      *record
	head     []byte
	n        int64
	max      int64
	reserved int64
	done     bool

	// declared is the body length the upstream announced, or -1.
	declared int64
	// varySpec is the vary specification the key of the response was
	// derived with, empty when the response does not vary.
	varySpec string

	// What follows is shared with the requests served from the response
	// while it is still being written, see Tail.
	pmu   sync.Mutex
	avail int64
	state writerState
	// wake is closed when avail or state changes. It only exists while a
	// reader waits.
	wake chan struct{}
}

type writerState int

const (
	writerActive writerState = iota
	writerCommitted
	writerAborted
)

// progress records how far the response got and wakes the readers waiting
// for more.
func (w *Writer) progress(state writerState) {
	w.pmu.Lock()
	w.avail = w.n
	w.state = state
	if w.wake != nil {
		close(w.wake)
		w.wake = nil
	}
	w.pmu.Unlock()
}

// Create starts storing a response for key. vary lists the lowercase, sorted
// names of the request headers the response depends on, whose values are
// taken from reqHeader. rec describes the response; its key is set here.
// A positive maxBody further limits the size of the body. declared is the
// length the upstream announced for it, or -1.
func (s *Store) Create(key string, vary []string, reqHeader http.Header, rec *record, maxBody, declared int64) (*Writer, error) {
	var spec string

	rec.key = key
	if len(vary) > 0 {
		var err error
		if spec, err = s.ensureMarker(key, strings.Join(vary, ",")); err != nil {
			return nil, err
		}
		rec.key = variantKey(key, spec, reqHeader)
	}

	limit := s.maxObject()
	if maxBody > 0 && maxBody < limit {
		limit = maxBody
	}

	w, err := s.newWriter(rec, limit)
	if err != nil {
		return nil, err
	}
	w.declared = declared
	w.varySpec = spec

	return w, nil
}

// ensureMarker makes the entry of key a marker listing the given header
// names and returns its vary specification.
func (s *Store) ensureMarker(key, names string) (string, error) {
	s.markerMu.Lock()
	defer s.markerMu.Unlock()

	if e, hit := s.pin(makeID(key)); e != nil {
		hit.Close()
		if _, current, _ := strings.Cut(e.vary, "\x00"); e.marker && current == names {
			return e.vary, nil
		}
	}

	// The salt makes the responses stored under a previous marker
	// unreachable, which is how replacing or purging a marker invalidates
	// all of them at once.
	var salt [8]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return "", err
	}
	spec := hex.EncodeToString(salt[:]) + "\x00" + names

	w, err := s.newWriter(&record{key: key, flags: flagMarker, vary: spec, stored: time.Now().UnixMilli()}, 0)
	if err != nil {
		return "", err
	}

	return spec, w.Commit()
}

// variantKey derives the key of the response selected by the request headers
// under the vary specification of a marker.
func variantKey(key, spec string, reqHeader http.Header) string {
	salt, names, _ := strings.Cut(spec, "\x00")

	var b strings.Builder
	b.WriteString(key)
	b.WriteByte(0)
	b.WriteString(salt)
	for name := range strings.SplitSeq(names, ",") {
		b.WriteByte(0)
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(varyValue(reqHeader, name))
	}

	return b.String()
}

// primaryKey strips what variantKey added.
func primaryKey(key string) string {
	primary, _, _ := strings.Cut(key, "\x00")

	return primary
}

func (s *Store) newWriter(rec *record, maxBody int64) (*Writer, error) {
	head, err := encodeHead(rec)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, errStoreClosed
	}

	f, err := os.CreateTemp(filepath.Join(s.dir, tmpDirName), "w-*")
	if err != nil {
		return nil, err
	}

	w := &Writer{s: s, f: f, tmp: f.Name(), rec: rec, head: head, max: maxBody, declared: -1}
	if w.info, err = f.Stat(); err == nil {
		err = w.reserve(int64(len(head)))
	}
	if err == nil {
		_, err = f.Write(head)
	}
	if err != nil {
		w.Abort()
		return nil, err
	}

	return w, nil
}

// reserve accounts for n more bytes on disk, evicting to make room for them.
func (w *Writer) reserve(n int64) error {
	s := w.s
	w.reserved += n
	if s.tempBytes.Add(n)+s.diskUsed.Load() <= s.maxSize.Load() {
		return nil
	}

	// Eviction stops as soon as everything fits, so the space is only
	// missing when there is nothing left to evict: the responses being
	// written take it all.
	s.mu.Lock()
	victims := s.enforceLocked()
	full := s.tail == nil && s.tempBytes.Load()+s.diskUsed.Load() > s.limits.MaxSize
	s.mu.Unlock()
	s.unlink(victims)

	if full {
		return errNoSpace
	}

	return nil
}

// Write appends to the body.
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, os.ErrClosed
	}
	if w.max > 0 && w.n+int64(len(p)) > w.max {
		return 0, errTooLarge
	}
	if err := w.reserve(int64(len(p))); err != nil {
		return 0, err
	}

	n, err := w.f.Write(p)
	w.n += int64(n)
	w.progress(writerActive)

	return n, err
}

// Abort drops what was written.
func (w *Writer) Abort() {
	if w.done {
		return
	}
	w.done = true

	_ = w.f.Close()
	_ = os.Remove(w.tmp)
	w.s.tempBytes.Add(-w.reserved)
	w.progress(writerAborted)
}

// Commit makes the response available, replacing the one stored under the
// same key if any.
func (w *Writer) Commit() error {
	if w.done {
		return os.ErrClosed
	}
	w.done = true
	s := w.s

	finalizeHead(w.head, w.n)
	_, err := w.f.WriteAt(w.head[:headerSize], 0)
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}

	var victims []ID
	if err == nil {
		id := makeID(w.rec.key)
		total := int64(len(w.head)) + w.n
		e := &entry{
			id:     id,
			key:    w.rec.key,
			vary:   w.rec.vary,
			marker: w.rec.marker(),
			size:   (total + diskBlock - 1) / diskBlock * diskBlock,
			cost:   entryOverhead + int64(len(w.rec.key)+len(w.rec.vary)),
			atime:  time.Now().Unix(),
		}

		stripe := &s.stripes[id[0]]
		stripe.Lock()
		victims, err = s.install(w.tmp, e)
		stripe.Unlock()
	}

	s.tempBytes.Add(-w.reserved)
	if err != nil {
		_ = os.Remove(w.tmp)
		w.progress(writerAborted)
		s.warn("storing a response failed", err)
		return err
	}
	w.progress(writerCommitted)

	s.unlink(victims)
	if !w.rec.marker() {
		s.stored.Add(1)
	}

	return nil
}

// errTailAborted is what reading a response in progress returns when the
// response was given up.
var errTailAborted = errors.New("cache: the response being read was not completed")

// Tail reads the body of a response while it is being written, waiting for
// the bytes that have not arrived yet. It is how the requests that come in
// during a download are served from it rather than made to wait for its end.
type Tail struct {
	w   *Writer
	f   *os.File
	ctx context.Context
	off int64
	// sent counts the bytes read since the last flush.
	sent int64
	// flush, if set, is called before waiting for more of the body, so that
	// what was read reaches the client meanwhile.
	flush func()
	// err is the reason the body could not be read to its end.
	err error
}

// Tail opens the response for reading. It fails once the response is
// committed, from when it is found in the cache like any other.
func (w *Writer) Tail(ctx context.Context) (*Tail, error) {
	f, err := os.Open(w.tmp)
	if err != nil {
		return nil, err
	}

	// The name could have been given to another download since.
	if info, err := f.Stat(); err != nil || !os.SameFile(info, w.info) {
		_ = f.Close()
		return nil, os.ErrNotExist
	}

	return &Tail{w: w, f: f, ctx: ctx}, nil
}

// Close releases the file.
func (t *Tail) Close() {
	_ = t.f.Close()
}

// Read implements io.Reader. It blocks until some of the body is available
// past the current offset, and fails if the response is given up.
func (t *Tail) Read(p []byte) (int, error) {
	w := t.w

	for {
		w.pmu.Lock()
		avail, state := w.avail, w.state
		var wake chan struct{}
		if state == writerActive && t.off >= avail {
			if w.wake == nil {
				w.wake = make(chan struct{})
			}
			wake = w.wake
		}
		w.pmu.Unlock()

		switch {
		case state == writerAborted:
			t.err = errTailAborted
			return 0, t.err
		case w.declared >= 0 && t.off >= w.declared:
			return 0, io.EOF
		case t.off < avail:
			n, err := t.f.ReadAt(p[:min(int64(len(p)), avail-t.off)], int64(len(w.head))+t.off)
			t.off += int64(n)
			t.sent += int64(n)
			if n > 0 {
				err = nil
			} else if err != nil {
				t.err = err
			}
			return n, err
		case state == writerCommitted:
			return 0, io.EOF
		}

		if t.flush != nil && t.sent > 0 {
			t.sent = 0
			t.flush()
		}
		select {
		case <-wake:
		case <-t.ctx.Done():
			t.err = t.ctx.Err()
			return 0, t.err
		}
	}
}

// Seek implements io.Seeker, for a body whose length was announced.
func (t *Tail) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += t.off
	case io.SeekEnd:
		offset += t.w.declared
	}
	if offset < 0 || t.w.declared < 0 {
		return 0, os.ErrInvalid
	}
	t.off = offset

	return offset, nil
}

// install moves a finished file into place and indexes it. The caller holds
// the stripe of the entry.
func (s *Store) install(tmp string, e *entry) ([]ID, error) {
	shard := e.id[0]
	if !s.dirMade[shard].Load() {
		if err := os.MkdirAll(filepath.Dir(s.path(e.id)), 0o700); err != nil {
			return nil, err
		}
		s.dirMade[shard].Store(true)
	}

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, errStoreClosed
	}

	if err := os.Rename(tmp, s.path(e.id)); err != nil {
		// The directory may have been removed under us.
		s.dirMade[shard].Store(false)
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if old := s.index[e.id]; old != nil {
		// The popularity belongs to the key, not to one version of it.
		e.hits, e.epoch = old.hits, old.epoch
		s.removeLocked(old)
	}
	s.insertLocked(e, true)

	return s.enforceLocked(), nil
}

// unlink deletes the files of the entries that left the index. An ID that
// was stored again meanwhile names a new file, which is kept.
func (s *Store) unlink(ids []ID) {
	for _, id := range ids {
		stripe := &s.stripes[id[0]]
		stripe.Lock()
		s.mu.Lock()
		_, present := s.index[id]
		s.mu.Unlock()
		if !present {
			_ = os.Remove(s.path(id))
		}
		stripe.Unlock()
	}
}

func (s *Store) insertLocked(e *entry, front bool) {
	s.index[e.id] = e
	if front {
		s.lruPushFront(e)
	} else {
		s.lruPushBack(e)
	}
	s.diskUsed.Add(e.size)
	s.metaUsed += e.cost
}

func (s *Store) removeLocked(e *entry) {
	if e.hot != nil {
		s.demoteLocked(e)
	}
	delete(s.index, e.id)
	s.lruRemove(e)
	s.diskUsed.Add(-e.size)
	s.metaUsed -= e.cost
}

// demoteLocked drops the in-memory copy of an entry. Its memory is freed
// once the requests being served from it are done.
func (s *Store) demoteLocked(e *entry) {
	hot := e.hot
	e.hot = nil
	s.hotRemove(e)
	s.hotCount--
	s.metaUsed -= hot.cost
	hot.body.release()
}

// hotLimitLocked is how many arena blocks the memory budget leaves once the
// index is paid for.
func (s *Store) hotLimitLocked() int {
	if s.limits.MaxMemory <= s.metaUsed {
		return 0
	}

	return int((s.limits.MaxMemory - s.metaUsed) / int64(s.arena.blockSize))
}

// enforceLocked brings the store back within its limits and returns the IDs
// of the entries it evicted, for the caller to unlink.
func (s *Store) enforceLocked() []ID {
	var victims []ID

	for {
		limit := s.hotLimitLocked()
		s.arena.setLimit(limit)
		for s.hotTail != nil && int(s.arena.inUse.Load()) > limit {
			s.demoteLocked(s.hotTail)
			limit = s.hotLimitLocked()
		}

		overDisk := s.diskUsed.Load()+s.tempBytes.Load() > s.limits.MaxSize
		overIndex := s.limits.MaxMemory > 0 && s.metaUsed > s.limits.MaxMemory
		if s.tail == nil || (!overDisk && !overIndex) {
			return victims
		}

		victims = append(victims, s.tail.id)
		s.removeLocked(s.tail)
		s.evicted.Add(1)
	}
}

func (s *Store) bumpLocked(e *entry, now int64) {
	s.decayLocked(e, now)
	if e.hits < math.MaxUint16 {
		e.hits++
	}
}

func (s *Store) decayLocked(e *entry, now int64) {
	epoch := uint32(now / decaySeconds)
	if d := epoch - e.epoch; d > 0 {
		if d >= 16 {
			e.hits = 0
		} else {
			e.hits >>= d
		}
		e.epoch = epoch
	}
}

// promotableLocked tells whether copying the response of e to memory is
// worth attempting.
func (s *Store) promotableLocked(e *entry, now int64) bool {
	if s.limits.MaxMemory <= 0 || e.marker || e.promoting || e.hits < promoteAfter ||
		// A single response should not push a large share of the others
		// out of memory.
		e.size > s.limits.MaxMemory/8 {
		return false
	}

	need := s.arena.blocksFor(e.size)
	limit := s.hotLimitLocked()
	if need > limit {
		return false
	}
	if int(s.arena.inUse.Load())+need <= limit {
		return true
	}

	return s.hotTail != nil && s.challengeLocked(s.hotTail, e, now)
}

// challengeLocked tells whether cand deserves the memory of v, the least
// recently used response in memory. It does when it is requested more.
// Otherwise v is given another round, but at the cost of half its count:
// a response that was popular once and is no longer requested ends up
// giving way.
func (s *Store) challengeLocked(v, cand *entry, now int64) bool {
	s.decayLocked(v, now)
	if v.hits < cand.hits {
		return true
	}

	v.hits /= 2
	if s.hotHead != v {
		s.hotRemove(v)
		s.hotPushFront(v)
	}

	return false
}

func (s *Store) promoter(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-s.promoteCh:
			s.promote(e)

			s.mu.Lock()
			e.promoting = false
			s.mu.Unlock()
		}
	}
}

// promote copies the response of e to memory if it is requested more than
// what it would push out.
func (s *Store) promote(e *entry) {
	f, err := os.Open(s.path(e.id))
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	rec, err := statRecord(f)
	if err != nil || rec.key != e.key || rec.marker() {
		return
	}
	need := s.arena.blocksFor(rec.bodyLen)

	s.mu.Lock()
	ok := !s.closed && s.index[e.id] == e && e.hot == nil && s.makeHotRoomLocked(need, e)
	s.mu.Unlock()
	if !ok {
		return
	}

	body := s.arena.alloc(rec.bodyLen)
	if body == nil {
		return
	}
	if err := body.fill(f, rec.bodyOff); err != nil {
		body.release()
		return
	}

	hot := &hotData{rec: rec, body: body, cost: rec.memCost() + int64(need)*28}

	s.mu.Lock()
	if s.closed || s.index[e.id] != e || e.hot != nil {
		s.mu.Unlock()
		body.release()
		return
	}
	e.hot = hot
	s.hotPushFront(e)
	s.hotCount++
	s.metaUsed += hot.cost
	victims := s.enforceLocked()
	s.mu.Unlock()

	s.promoted.Add(1)
	s.unlink(victims)
}

// makeHotRoomLocked frees memory for need blocks by dropping the in-memory
// copies that are requested less than cand. It reports whether there is room.
func (s *Store) makeHotRoomLocked(need int, cand *entry) bool {
	limit := s.hotLimitLocked()
	if need > limit {
		return false
	}

	now := time.Now().Unix()
	s.decayLocked(cand, now)

	spared := 0
	for int(s.arena.inUse.Load())+need > limit {
		v := s.hotTail
		if v == nil || spared == 8 {
			return false
		}
		if !s.challengeLocked(v, cand, now) {
			spared++
			continue
		}
		s.demoteLocked(v)
	}

	return true
}

// load indexes the files found on disk.
func (s *Store) load(ctx context.Context) {
	defer s.wg.Done()

	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()

	start := time.Now()
	for shard := range 256 {
		dir := filepath.Join(s.dir, fmt.Sprintf("%02x", shard))
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		s.dirMade[shard].Store(true)

		for _, file := range files {
			// Anything not named like a cache file is not ours to touch.
			id, ok := parseID(file.Name())
			if !ok || id[0] != byte(shard) {
				continue
			}

			s.mu.Lock()
			_, known := s.index[id]
			stop := s.gen != gen || s.closed
			s.mu.Unlock()
			if stop || ctx.Err() != nil {
				return
			}
			if !known {
				s.loadFile(id, gen, false)
			}
		}
	}

	s.mu.Lock()
	if s.gen != gen {
		s.mu.Unlock()
		return
	}
	s.loaded = true
	entries := len(s.index)
	victims := s.enforceLocked()
	s.mu.Unlock()

	s.unlink(victims)
	s.log.Info("cache loaded", zap.String("path", s.dir), zap.Int("entries", entries),
		zap.Int64("bytes", s.diskUsed.Load()), zap.Duration("duration", time.Since(start)))
}

// loadFile indexes the file of id unless the index already knows better.
// front tells whether the entry was just requested.
func (s *Store) loadFile(id ID, gen uint64, front bool) {
	f, err := os.Open(s.path(id))
	if err != nil {
		return
	}

	var size int64
	rec, err := statRecord(f)
	if err == nil {
		size = rec.bodyOff + rec.bodyLen
		if makeID(rec.key) != id {
			err = errCorrupt
		}
	}
	_ = f.Close()

	if err != nil {
		if errors.Is(err, errCorrupt) {
			s.unlink([]ID{id})
		}
		return
	}

	e := &entry{
		id:     id,
		key:    rec.key,
		vary:   rec.vary,
		marker: rec.marker(),
		size:   (size + diskBlock - 1) / diskBlock * diskBlock,
		cost:   entryOverhead + int64(len(rec.key)+len(rec.vary)),
		atime:  time.Now().Unix(),
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.gen != gen || s.closed || s.index[id] != nil {
		return
	}
	s.insertLocked(e, front)
}

// janitor removes the responses nobody requested for the inactive period.
func (s *Store) janitor(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.expireInactive()
		}
	}
}

func (s *Store) expireInactive() {
	var victims []ID

	s.mu.Lock()
	if s.limits.Inactive > 0 {
		deadline := time.Now().Add(-s.limits.Inactive).Unix()
		// The list is ordered by last use, so the idle entries are at its
		// end.
		for s.tail != nil && s.tail.atime < deadline {
			victims = append(victims, s.tail.id)
			s.removeLocked(s.tail)
		}
	}
	s.mu.Unlock()

	s.unlink(victims)
}

// Purge removes the response stored for a key, in all its variants.
func (s *Store) Purge(key string) bool {
	id := makeID(key)

	s.mu.Lock()
	e := s.index[id]
	if e != nil {
		s.removeLocked(e)
	}
	s.mu.Unlock()

	// Without the index entry the file may still be there, not loaded yet.
	s.unlink([]ID{id})

	return e != nil
}

// PurgeMatch removes the responses whose key satisfies match and returns how
// many there were. It only sees the files indexed so far.
func (s *Store) PurgeMatch(match func(key string) bool) int {
	s.mu.Lock()
	entries := make([]*entry, 0, len(s.index))
	for _, e := range s.index {
		entries = append(entries, e)
	}
	s.mu.Unlock()

	// Matching can be slow, so it is done without holding up the requests.
	matched := entries[:0]
	for _, e := range entries {
		if match(primaryKey(e.key)) {
			matched = append(matched, e)
		}
	}

	victims := make([]ID, 0, len(matched))
	s.mu.Lock()
	for _, e := range matched {
		if s.index[e.id] == e {
			s.removeLocked(e)
			victims = append(victims, e.id)
		}
	}
	s.mu.Unlock()

	s.unlink(victims)

	return len(victims)
}

// PurgeAll empties the cache and returns how many entries it held.
func (s *Store) PurgeAll() int {
	s.mu.Lock()
	n := len(s.index)
	for s.tail != nil {
		s.removeLocked(s.tail)
	}
	// Stop a loader still running: what it read is about to be deleted.
	s.gen++
	s.loaded = true
	s.mu.Unlock()

	// Sweep the directories rather than the former index, so that the files
	// the loader had not reached go too. Responses stored meanwhile are in
	// the index and are kept.
	for shard := range 256 {
		files, err := os.ReadDir(filepath.Join(s.dir, fmt.Sprintf("%02x", shard)))
		if err != nil {
			continue
		}
		for _, file := range files {
			if id, ok := parseID(file.Name()); ok && id[0] == byte(shard) {
				s.unlink([]ID{id})
			}
		}
	}

	return n
}

func (s *Store) lruPushFront(e *entry) {
	e.prev, e.next = nil, s.head
	if s.head != nil {
		s.head.prev = e
	} else {
		s.tail = e
	}
	s.head = e
}

func (s *Store) lruPushBack(e *entry) {
	e.prev, e.next = s.tail, nil
	if s.tail != nil {
		s.tail.next = e
	} else {
		s.head = e
	}
	s.tail = e
}

func (s *Store) lruRemove(e *entry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		s.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (s *Store) hotPushFront(e *entry) {
	e.hprev, e.hnext = nil, s.hotHead
	if s.hotHead != nil {
		s.hotHead.hprev = e
	} else {
		s.hotTail = e
	}
	s.hotHead = e
}

func (s *Store) hotRemove(e *entry) {
	if e.hprev != nil {
		e.hprev.hnext = e.hnext
	} else {
		s.hotHead = e.hnext
	}
	if e.hnext != nil {
		e.hnext.hprev = e.hprev
	} else {
		s.hotTail = e.hprev
	}
	e.hprev, e.hnext = nil, nil
}

// flight is a fetch in progress that other requests for the same response
// wait for instead of all going to the upstream.
type flight struct {
	done chan struct{}
	// stored tells the waiters the response is now in the cache.
	stored bool
	// started is closed once the response is being stored, from when w can
	// be read by the waiters.
	started chan struct{}
	w       *Writer
}

// BeginFlight registers a fetch for id. The caller that gets leader set must
// call EndFlight; the others wait on the returned flight.
func (s *Store) BeginFlight(id ID) (f *flight, leader bool) {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	if f := s.flights[id]; f != nil {
		return f, false
	}
	f = &flight{done: make(chan struct{}), started: make(chan struct{})}
	s.flights[id] = f

	return f, true
}

// SharedFlight returns the response being stored for id, if a fetch for it
// got that far.
func (s *Store) SharedFlight(id ID) *Writer {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	if f := s.flights[id]; f != nil {
		return f.w
	}

	return nil
}

// ShareFlight lets the requests waiting on the flight read the response from
// w as it is written.
func (s *Store) ShareFlight(id ID, f *flight, w *Writer) {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	if s.flights[id] == f && f.w == nil {
		f.w = w
		close(f.started)
	}
}

// EndFlight wakes the requests waiting on the flight. stored tells them the
// response is now in the cache.
func (s *Store) EndFlight(id ID, f *flight, stored bool) {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	if s.flights[id] != f {
		return
	}
	delete(s.flights, id)

	f.stored = stored
	close(f.done)
}

// SetUncacheable records whether the response of id was found not to be
// cacheable. While it is, the requests for it do not wait for each other.
func (s *Store) SetUncacheable(id ID, uncacheable bool) {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	if !uncacheable {
		delete(s.passMemo, id)
		return
	}
	if len(s.passMemo) >= maxPassMemo {
		clear(s.passMemo)
	}
	s.passMemo[id] = time.Now().Add(passMemoTTL).Unix()
}

// Uncacheable tells whether the response of id was recently found not to be
// cacheable.
func (s *Store) Uncacheable(id ID) bool {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	until, ok := s.passMemo[id]
	if ok && until < time.Now().Unix() {
		delete(s.passMemo, id)
		ok = false
	}

	return ok
}

// Rewrite stores rec with the body of a response already in the cache. It is
// how the headers and freshness of a response are updated once the upstream
// confirmed its body is still current.
func (s *Store) Rewrite(old *Hit, rec *record) error {
	w, err := s.newWriter(rec, 0)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, old.Body()); err != nil {
		w.Abort()
		return err
	}

	return w.Commit()
}
