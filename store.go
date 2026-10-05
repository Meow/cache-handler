package httpcache

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
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
	// transientShare is the share of the memory the responses that are held
	// in memory only, waiting to be requested again, may take together, and
	// transientBodyShare the share the body of one of them may take.
	transientShare     = 2
	transientBodyShare = 8
	// ghostShare is the share of the memory spent on remembering the
	// responses that were dropped before they were requested again.
	ghostShare = 128

	tmpDirName  = "tmp"
	lockName    = ".lock"
	cacheDirTag = "CACHEDIR.TAG"
)

var (
	errStoreClosed = errors.New("cache store is closed")
	errTooLarge    = errors.New("response too large to cache")
	errNoSpace     = errors.New("no cache space left for the response")
	// errGone tells that the entry a file was written for left the cache
	// meanwhile.
	errGone = errors.New("the response is no longer in the cache")
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
	// MaxFiles is the number of files the cache may hold, downloads in
	// progress included. Zero does not limit it.
	MaxFiles int64
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
	hits uint16
	// left is how many more requests a transient response needs to be
	// written to disk.
	left      uint16
	epoch     uint32
	marker    bool
	promoting bool
	// transient tells that the entry has no file: it only exists in memory,
	// until it is requested enough to be worth writing to disk. A transient
	// response always has its body in hot, and is listed apart from the
	// others; a transient marker has nothing but its entry.
	transient bool
	// persisting tells that the persister was asked to write the entry.
	persisting bool
	hot        *hotData

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
	MaxFiles    int64  `json:"max_file_count"`
	DiskBytes   int64  `json:"disk_bytes"`
	MaxSize     int64  `json:"max_size"`
	MemoryBytes int64  `json:"memory_bytes"`
	MaxMemory   int64  `json:"max_memory"`
	IndexBytes  int64  `json:"index_bytes"`
	HotEntries  int    `json:"hot_entries"`
	HotBytes    int64  `json:"hot_bytes"`
	// TransientEntries counts the entries that are in memory only, which
	// HotEntries does not include.
	TransientEntries int   `json:"transient_entries"`
	Hits             int64 `json:"hits"`
	HotHits          int64 `json:"hot_hits"`
	Misses           int64 `json:"misses"`
	Stored           int64 `json:"stored"`
	Evicted          int64 `json:"evicted"`
	Promoted         int64 `json:"promoted"`
	// Persisted counts the responses written to disk after a time in memory
	// only, Dropped those that left memory without having been.
	Persisted int64 `json:"persisted"`
	Dropped   int64 `json:"dropped"`
}

// Store is a two-tier response cache: every response is a file under dir and
// the most requested ones are also held in memory. Both tiers are bounded.
//
// Files are written to a temporary name and renamed into place, so a cache
// file is always complete and never changes. The index is rebuilt from the
// files when the store opens.
//
// A response may be asked to stay out of the disk until it was requested a
// number of times (min_uses). It is then transient: held in memory only, and
// written to a file by the persister once it has been requested enough. Most
// responses are requested once and never again; these are dropped from memory
// by the ones that follow and never cost a write.
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
	// transHead and transTail are the list of the transient responses, by
	// last use. They are not part of the list above, which is the one of the
	// files.
	transHead *entry
	transTail *entry
	// transients counts the entries that have no file. transBlocks and
	// transMeta are the memory blocks and the index space taken by the
	// transient responses, those being received included for the former.
	transients  int
	transBlocks int
	transMeta   int64
	// ghosts remembers the transient responses that were dropped, see
	// rememberLocked.
	ghosts   []uint64
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
	// tempFiles counts the responses being written.
	tempFiles atomic.Int64

	arena     *arena
	promoteCh chan *entry
	persistCh chan *entry
	// trimCh wakes the janitor when memory is left to give back.
	trimCh chan struct{}

	// stripes serialize the operations that change which file an ID names.
	stripes  [256]sync.Mutex
	dirMade  [256]atomic.Bool
	markerMu sync.Mutex

	fmu      sync.Mutex
	flights  map[ID]*flight
	passMemo map[ID]int64

	hits, hotHits, misses, stored, evicted, promoted atomic.Int64
	persisted, dropped                               atomic.Int64
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
		persistCh: make(chan *entry, 256),
		trimCh:    make(chan struct{}, 1),
		flights:   make(map[ID]*flight),
		passMemo:  make(map[ID]int64),
		cancel:    cancel,
	}
	s.SetLimits(limits)

	s.wg.Add(4)
	go s.load(ctx)
	go s.promoter(ctx)
	go s.persister(ctx)
	go s.janitor(ctx)

	return s, nil
}

// Destruct implements caddy.Destructor.
func (s *Store) Destruct() error {
	return s.Close()
}

// Close stops the store. The files stay on disk for the next one to pick up;
// the responses that were in memory only are lost. Requests still being
// served from the store finish normally.
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
	for s.transTail != nil {
		s.removeLocked(s.transTail)
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
	if s.ghosts != nil && limits.MaxMemory != s.limits.MaxMemory {
		// Their number follows the memory.
		s.metaUsed -= 8 * int64(len(s.ghosts))
		s.ghosts = nil
	}
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
		MaxFiles:    s.limits.MaxFiles,
		DiskBytes:   s.diskUsed.Load(),
		MaxSize:     s.limits.MaxSize,
		MemoryBytes: s.metaUsed + resident,
		MaxMemory:   s.limits.MaxMemory,
		IndexBytes:  s.metaUsed,
		HotEntries:  s.hotCount,
		HotBytes:    s.arena.inUse.Load() * int64(s.arena.blockSize),

		TransientEntries: s.transients,
		Hits:             s.hits.Load(),
		HotHits:          s.hotHits.Load(),
		Misses:           s.misses.Load(),
		Stored:           s.stored.Load(),
		Evicted:          s.evicted.Load(),
		Promoted:         s.promoted.Load(),
		Persisted:        s.persisted.Load(),
		Dropped:          s.dropped.Load(),
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
	id, hit := s.find(key, reqHeader)
	s.count(hit)

	return id, hit
}

// count records the outcome of a lookup in the statistics.
func (s *Store) count(hit *Hit) {
	if hit != nil {
		s.hits.Add(1)
	} else {
		s.misses.Add(1)
	}
}

// find is Lookup without the statistics, for the caller that looks in more
// than one place for one request and counts the outcome itself.
func (s *Store) find(key string, reqHeader http.Header) (ID, *Hit) {
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
				// Only a file that is gone or damaged is forgotten: failing
				// to open it for lack of file descriptors, say, is no
				// reason to lose it.
				if errors.Is(err, fs.ErrNotExist) || errors.Is(err, errCorrupt) {
					s.discard(e)
				}
				break
			}
		} else if hit.rec.key != full {
			hit.Close()
			break
		}

		return id, hit
	}

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
	if head, _ := s.ends(e); *head != e {
		s.lruRemove(e)
		s.lruPushFront(e)
	}

	var hit *Hit
	switch {
	case e.hot != nil:
		e.hot.body.acquire()
		hit = &Hit{s: s, e: e, rec: e.hot.rec, blob: e.hot.body}
		s.hotHits.Add(1)
		if e.transient {
			// One request less to wait for before the response is written
			// to disk.
			if e.left > 0 {
				e.left--
			}
			hit.persist = e.left == 0 && !e.persisting
		} else if s.hotHead != e {
			s.hotRemove(e)
			s.hotPushFront(e)
		}
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
	s    *Store
	e    *entry
	rec  *record
	f    *os.File
	blob *blob
	// persist tells that the response is in memory only and was now
	// requested enough to be written to disk. That is asked for when the
	// request is done with the response: if it turned out to be stale and
	// was replaced meanwhile, there is nothing left to write.
	persist bool
	closed  bool
}

// Close releases the response. It is safe to call on a nil Hit and more than
// once.
func (h *Hit) Close() {
	if h == nil || h.closed {
		return
	}
	h.closed = true

	if h.persist {
		h.s.requestPersist(h.e)
	}
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
//
// The body goes to a temporary file, or to memory for a response that is to
// be transient. One that turns out not to fit there moves to a file on its
// way, see spill.
type Writer struct {
	s  *Store
	id ID
	// f, info and tmp are the temporary file. A response that moved from
	// memory to a file gets them late, so they are only set under pmu.
	f        *os.File
	info     os.FileInfo
	tmp      string
	rec      *record
	head     []byte
	n        int64
	max      int64
	reserved int64
	// counted tells the file is part of tempFiles.
	counted bool
	done    bool

	// mem is the body of a response received in memory, whose blocks are
	// part of transBlocks. It is only changed under pmu.
	mem *blob
	// memMax is the largest body mem may hold.
	memMax int64
	// left is how many more requests the response needed to be written to
	// disk when it started being received.
	left uint16
	// adopt is the transient entry the file is written for, when it is not
	// for a new response.
	adopt *entry

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
	// readers lists the Tails reading from mem, which are to be given the
	// file if the response moves to one. uses counts the requests served
	// from the response while it was received in memory. sealed tells that
	// the count was taken, for the entry the response is becoming: no
	// request is to start reading it from here anymore.
	readers []*Tail
	uses    int
	sealed  bool
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
//
// minUses is the number of requests after which the response is written to
// disk. With more than one, the response is kept in memory meanwhile, if
// memory can hold it; otherwise it is written at once.
func (s *Store) Create(key string, vary []string, reqHeader http.Header, rec *record, maxBody, declared int64, minUses int) (*Writer, error) {
	var spec string

	rec.key = key
	if len(vary) > 0 {
		var err error
		if spec, err = s.ensureMarker(key, strings.Join(vary, ","), minUses); err != nil {
			return nil, err
		}
		rec.key = variantKey(key, spec, reqHeader)
	}

	limit := s.maxObject()
	if maxBody > 0 && maxBody < limit {
		limit = maxBody
	}

	w, err := s.newWriter(rec, limit, declared, minUses)
	if err != nil {
		return nil, err
	}
	w.varySpec = spec

	return w, nil
}

// ensureMarker makes the entry of key a marker listing the given header
// names and returns its vary specification. A marker created for responses
// that are not written to disk at once (minUses) is not either: it is when
// the first of them is, see saveMarker.
//
// A marker that lists these names and others is kept as it is. A response
// stored under more names than it varies on is only found by fewer requests
// than it could answer, whereas replacing the marker loses every response
// stored under it: an upstream that only names a header when the response
// depends on its value, as one that compresses for those who accept it
// does, would otherwise have each kind of response evict the other.
func (s *Store) ensureMarker(key, names string, minUses int) (string, error) {
	s.markerMu.Lock()
	defer s.markerMu.Unlock()

	if e, hit := s.pin(makeID(key)); e != nil {
		hit.Close()
		if _, current, _ := strings.Cut(e.vary, "\x00"); e.marker && listsAll(current, names) {
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

	w, err := s.newWriter(&record{key: key, flags: flagMarker, vary: spec, stored: time.Now().UnixMilli()}, 0, -1, minUses)
	if err != nil {
		return "", err
	}

	return spec, w.Commit()
}

// listsAll tells whether every name of the comma separated list names is in
// the list current.
func listsAll(current, names string) bool {
	if current == names {
		return true
	}

	listed := strings.Split(current, ",")
	for name := range strings.SplitSeq(names, ",") {
		if !slices.Contains(listed, name) {
			return false
		}
	}

	return true
}

// saveMarker writes to disk the marker through which the variant stored under
// key is found, if that marker is transient: without it a variant on disk is
// lost to the next start. It reports whether the variant can be found at
// all, which it cannot once its marker was replaced or removed.
func (s *Store) saveMarker(key string) bool {
	primary := primaryKey(key)
	id := makeID(primary)

	s.mu.Lock()
	mk := s.index[id]
	found := false
	if mk != nil && mk.marker {
		salt, _, _ := strings.Cut(mk.vary, "\x00")
		found = strings.HasPrefix(key[len(primary)+1:], salt+"\x00")
	}
	transient := found && mk.transient
	var spec string
	if transient {
		spec = mk.vary
	}
	s.mu.Unlock()

	if !transient {
		return found
	}

	w, err := s.newWriter(&record{key: primary, flags: flagMarker, vary: spec, stored: time.Now().UnixMilli()}, 0, -1, 1)
	if err != nil {
		return false
	}
	w.adopt = mk
	if w.Commit() == nil {
		return true
	}

	// Another variant may have had it written meanwhile.
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.index[id] == mk && !mk.transient
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

// newWriter starts storing rec: in memory if the response is to be transient
// and memory can hold it, in a temporary file otherwise.
func (s *Store) newWriter(rec *record, maxBody, declared int64, minUses int) (*Writer, error) {
	head, err := encodeHead(rec)
	if err != nil {
		return nil, err
	}

	w := &Writer{s: s, id: makeID(rec.key), rec: rec, head: head, max: maxBody, declared: declared}

	inMemory := false
	if minUses > 1 {
		s.mu.Lock()
		switch {
		case s.closed || s.limits.MaxMemory <= 0:
		case rec.marker():
			inMemory = true
		default:
			w.left = s.usesLeftLocked(w.id, minUses)
			w.memMax = s.limits.MaxMemory / transientBodyShare
			// The request this response is fetched for is one of those it
			// needs, so with one left it is due on disk already.
			inMemory = w.left > 1 && declared <= w.memMax
		}
		s.mu.Unlock()
	}

	if inMemory {
		w.mem = s.arena.newBlob()
		// A body of known length gets its memory at once, rather than find
		// out midway that there is none left.
		if declared <= 0 || w.growMem(s.arena.blocksFor(declared)) {
			return w, nil
		}
		w.dropMem()
	}

	f, info, err := w.createFile()
	if err != nil {
		return nil, err
	}
	w.f, w.info, w.tmp = f, info, f.Name()

	if err = w.reserve(int64(len(head))); err == nil {
		_, err = f.Write(head)
	}
	if err != nil {
		w.Abort()
		return nil, err
	}

	return w, nil
}

// usesLeftLocked returns how many more requests the response about to be
// stored under id needs before it is written to disk, counting the one it is
// fetched for. Zero means it is to be written whatever is asked.
func (s *Store) usesLeftLocked(id ID, minUses int) uint16 {
	if old := s.index[id]; old != nil && (!old.transient || !old.marker) {
		// What is on disk stays there, and a new version of a transient
		// response carries on with its count. The request it is fetched for
		// was counted when it found the old one, and will be again.
		if !old.transient || old.left == 0 {
			return 0
		}

		return uint16(min(int(old.left)+1, math.MaxUint16))
	}
	if left, ok := s.recallLocked(id); ok {
		return left
	}

	return uint16(min(minUses, math.MaxUint16))
}

// createFile creates the temporary file of the response.
func (w *Writer) createFile() (*os.File, os.FileInfo, error) {
	s := w.s

	// The file about to be created counts against the limit on their number
	// from now on, like the bytes written to it will against the size.
	s.mu.Lock()
	closed := s.closed
	var victims []ID
	full := false
	if !closed {
		s.tempFiles.Add(1)
		victims = s.enforceLocked()
		full = s.tail == nil && s.overFilesLocked()
	}
	s.mu.Unlock()
	s.unlink(victims)

	if closed {
		return nil, nil, errStoreClosed
	}
	if full {
		s.tempFiles.Add(-1)
		return nil, nil, errNoSpace
	}

	tmp := filepath.Join(s.dir, tmpDirName)
	f, err := os.CreateTemp(tmp, "w-*")
	if errors.Is(err, fs.ErrNotExist) {
		// The directory was emptied by hand.
		if err = os.MkdirAll(tmp, 0o700); err == nil {
			f, err = os.CreateTemp(tmp, "w-*")
		}
	}
	if err != nil {
		s.tempFiles.Add(-1)
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		s.tempFiles.Add(-1)
		return nil, nil, err
	}
	w.counted = true

	return f, info, nil
}

// growMem obtains n more memory blocks for the body, at the expense of the
// transient responses requested least recently and of the copies in memory
// of what is on disk. It reports whether it could.
func (w *Writer) growMem(n int) bool {
	s := w.s

	s.mu.Lock()
	ok := !s.closed && s.transientRoomLocked(n)
	if ok {
		s.transBlocks += n
	}
	s.mu.Unlock()
	if !ok {
		return false
	}

	// The readers look at the blocks under this lock.
	w.pmu.Lock()
	ok = s.arena.grow(w.mem, n)
	w.pmu.Unlock()
	if !ok {
		s.mu.Lock()
		s.transBlocks -= n
		s.mu.Unlock()
	}

	return ok
}

// writeMem appends to the body held in memory. It reports whether it could:
// not when the body outgrows what memory may hold of it, or of all of them.
func (w *Writer) writeMem(p []byte) bool {
	end := w.n + int64(len(p))
	if end > w.memMax {
		return false
	}
	if need := w.s.arena.blocksFor(end) - len(w.mem.blocks); need > 0 && !w.growMem(need) {
		return false
	}

	w.mem.write(p, w.n)
	w.n = end
	w.progress(writerActive)

	return true
}

// spill moves a response received in memory to a temporary file, where the
// rest of it goes: it will be written to disk rather than held in memory.
// The requests reading it are switched to the file.
func (w *Writer) spill() error {
	f, info, err := w.createFile()
	if err != nil {
		return err
	}
	tmp := f.Name()

	if err = w.reserve(int64(len(w.head)) + w.n); err == nil {
		_, err = f.Write(w.head)
	}
	if err == nil {
		err = w.mem.writeTo(f, w.n)
	}

	var mem *blob
	if err == nil {
		// The readers go from memory to the file at once, all of them: one
		// left reading memory would not find there what comes next.
		w.pmu.Lock()
		for _, t := range w.readers {
			if t.f, err = os.Open(tmp); err != nil {
				break
			}
		}
		if err == nil {
			w.f, w.info, w.tmp = f, info, tmp
			mem, w.mem, w.readers = w.mem, nil, nil
		} else {
			for _, t := range w.readers {
				if t.f != nil {
					_ = t.f.Close()
					t.f = nil
				}
			}
		}
		w.pmu.Unlock()
	}

	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		w.release()
		return err
	}
	w.s.freeMem(mem)

	return nil
}

// takeMem ends the use of memory by the writer and returns what it held.
func (w *Writer) takeMem() *blob {
	w.pmu.Lock()
	mem := w.mem
	w.mem, w.readers = nil, nil
	w.pmu.Unlock()

	return mem
}

// dropMem gives back the memory the body was received in.
func (w *Writer) dropMem() {
	if mem := w.takeMem(); mem != nil {
		w.s.freeMem(mem)
	}
}

// freeMem gives back the memory a body that is not stored was received in.
// The requests reading it keep it for as long as they need.
func (s *Store) freeMem(mem *blob) {
	s.mu.Lock()
	s.transBlocks -= len(mem.ids)
	s.mu.Unlock()
	mem.release()
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

// release gives back what the file being written was accounted for. It is
// called when the file is dropped, or under the store lock at the moment it
// becomes an entry, so that it is never counted twice.
func (w *Writer) release() {
	w.s.tempBytes.Add(-w.reserved)
	w.reserved = 0
	if w.counted {
		w.s.tempFiles.Add(-1)
		w.counted = false
	}
}

// Write appends to the body.
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, os.ErrClosed
	}
	if w.max > 0 && w.n+int64(len(p)) > w.max {
		return 0, errTooLarge
	}
	if w.mem != nil {
		if w.writeMem(p) {
			return len(p), nil
		}
		// Memory cannot hold the response after all: it goes on in a file.
		if err := w.spill(); err != nil {
			return 0, err
		}
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

	if w.f != nil {
		_ = w.f.Close()
		_ = os.Remove(w.tmp)
	}
	w.release()
	w.progress(writerAborted)
	w.dropMem()
}

// Commit makes the response available, replacing the one stored under the
// same key if any.
func (w *Writer) Commit() error {
	if w.done {
		return os.ErrClosed
	}
	w.done = true
	s := w.s

	var (
		victims []ID
		err     error
	)
	if w.mem != nil {
		victims, err = s.installMem(w)
	} else {
		victims, err = w.commitFile()
	}

	if err != nil {
		if w.f != nil {
			_ = os.Remove(w.tmp)
		}
		w.release()
		w.progress(writerAborted)
		w.dropMem()
		if err != errGone {
			s.warn("storing a response failed", err)
		}
		return err
	}
	w.progress(writerCommitted)
	// The memory is the entry's now. The writer kept a hold on it until
	// here for the requests that started reading it meanwhile.
	if mem := w.takeMem(); mem != nil {
		mem.release()
	}

	s.unlink(victims)
	if !w.rec.marker() && w.adopt == nil {
		s.stored.Add(1)
	}

	return nil
}

// commitFile seals the temporary file and makes it the file of its entry.
func (w *Writer) commitFile() ([]ID, error) {
	s := w.s

	finalizeHead(w.head, w.n)
	_, err := w.f.WriteAt(w.head[:headerSize], 0)
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}

	total := int64(len(w.head)) + w.n
	size := (total + diskBlock - 1) / diskBlock * diskBlock

	if w.adopt == nil && primaryKey(w.rec.key) != w.rec.key {
		s.saveMarker(w.rec.key)
	}

	stripe := &s.stripes[w.id[0]]
	stripe.Lock()
	defer stripe.Unlock()

	if w.adopt != nil {
		return s.adopt(w, w.adopt, size)
	}

	return s.install(w, &entry{
		id:     w.id,
		key:    w.rec.key,
		vary:   w.rec.vary,
		marker: w.rec.marker(),
		size:   size,
		cost:   entryOverhead + int64(len(w.rec.key)+len(w.rec.vary)),
		atime:  time.Now().Unix(),
	})
}

// errTailAborted is what reading a response in progress returns when the
// response was given up.
var errTailAborted = errors.New("cache: the response being read was not completed")

// Tail reads the body of a response while it is being written, waiting for
// the bytes that have not arrived yet. It is how the requests that come in
// during a download are served from it rather than made to wait for its end.
type Tail struct {
	w *Writer
	// f is the file of the response. A response received in memory is read
	// from mem instead, until it moves to a file: f is then set by the
	// writer, under its lock.
	f   *os.File
	mem *blob
	ctx context.Context
	off int64
	// sent counts the bytes read since the last flush.
	sent int64
	// flush, if set, is called before waiting for more of the body, so that
	// what was read reaches the client meanwhile.
	flush func()
	// limit, if not negative, is where the body ends for this reader
	// whatever becomes of the response. It is guarded by the lock of w.
	limit int64
	// err is the reason the body could not be read to its end.
	err error
}

// Tail opens the response for reading. It fails once the response is
// committed, from when it is found in the cache like any other. For a
// response received in memory that is as soon as Commit is at work: the
// request would not count among those the response waits for otherwise.
func (w *Writer) Tail(ctx context.Context) (*Tail, error) {
	t := &Tail{w: w, ctx: ctx, limit: -1}

	w.pmu.Lock()
	if w.mem != nil && w.state == writerActive && !w.sealed {
		// The writer holds the memory for as long as it is in this state,
		// which makes it safe to take a hold on it too.
		w.mem.acquire()
		t.mem = w.mem
		w.readers = append(w.readers, t)
		w.uses++
		w.pmu.Unlock()

		return t, nil
	}
	tmp, info := w.tmp, w.info
	w.pmu.Unlock()

	if tmp == "" {
		return nil, os.ErrNotExist
	}
	f, err := os.Open(tmp)
	if err != nil {
		return nil, err
	}

	// The name could have been given to another download since.
	if fi, err := f.Stat(); err != nil || !os.SameFile(fi, info) {
		_ = f.Close()
		return nil, os.ErrNotExist
	}
	t.f = f

	return t, nil
}

// finishAt makes the reader stop at offset n, which must have been written,
// instead of following the response to its end. The reader then gets these
// bytes even if the response is given up: it is how the client of a
// response that cannot be stored after all is still sent what was written.
func (t *Tail) finishAt(n int64) {
	w := t.w

	w.pmu.Lock()
	t.limit = n
	if w.wake != nil {
		close(w.wake)
		w.wake = nil
	}
	w.pmu.Unlock()
}

// Close releases the file or the memory read from.
func (t *Tail) Close() {
	w := t.w

	w.pmu.Lock()
	if i := slices.Index(w.readers, t); i >= 0 {
		w.readers = slices.Delete(w.readers, i, i+1)
	}
	f := t.f
	w.pmu.Unlock()

	if f != nil {
		_ = f.Close()
	}
	if t.mem != nil {
		t.mem.release()
		t.mem = nil
	}
}

// Read implements io.Reader. It blocks until some of the body is available
// past the current offset, and fails if the response is given up.
func (t *Tail) Read(p []byte) (int, error) {
	w := t.w

	for {
		w.pmu.Lock()
		avail, state, limit := w.avail, w.state, t.limit
		if limit >= 0 {
			avail = min(avail, limit)
		}
		f := t.f
		var blocks [][]byte
		if f == nil {
			blocks = t.mem.blocks
		}
		var wake chan struct{}
		if state == writerActive && limit < 0 && t.off >= avail {
			if w.wake == nil {
				w.wake = make(chan struct{})
			}
			wake = w.wake
		}
		w.pmu.Unlock()

		switch {
		case limit >= 0 && t.off >= limit:
			return 0, io.EOF
		case state == writerAborted && limit < 0:
			t.err = errTailAborted
			return 0, t.err
		case w.declared >= 0 && t.off >= w.declared:
			return 0, io.EOF
		case t.off < avail:
			var (
				n   int
				err error
			)
			if p = p[:min(int64(len(p)), avail-t.off)]; f != nil {
				n, err = f.ReadAt(p, int64(len(w.head))+t.off)
			} else {
				// What is below avail was written before avail said so.
				n = readBlocks(p, blocks, int64(t.mem.blockSize), t.off, avail)
			}
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

// ensureShard creates the directory of the file of id if needed.
func (s *Store) ensureShard(id ID) error {
	shard := id[0]
	if !s.dirMade[shard].Load() {
		if err := os.MkdirAll(filepath.Dir(s.path(id)), 0o700); err != nil {
			return err
		}
		s.dirMade[shard].Store(true)
	}

	return nil
}

// install moves a finished file into place and indexes it. The caller holds
// the stripe of the entry.
func (s *Store) install(w *Writer, e *entry) ([]ID, error) {
	shard := e.id[0]
	if err := s.ensureShard(e.id); err != nil {
		return nil, err
	}

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, errStoreClosed
	}

	if err := os.Rename(w.tmp, s.path(e.id)); err != nil {
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
	// The file stops being a download in progress as it becomes an entry:
	// counted as both, it would evict its own weight in other responses.
	w.release()
	s.forgetLocked(e.id)
	s.insertLocked(e, true)

	return s.enforceLocked(), nil
}

// installMem indexes a response received in memory, or a marker, as a
// transient entry: one without a file.
func (s *Store) installMem(w *Writer) ([]ID, error) {
	rec := *w.rec
	rec.bodyLen = w.n

	e := &entry{
		id:        w.id,
		key:       rec.key,
		vary:      rec.vary,
		marker:    rec.marker(),
		cost:      entryOverhead + int64(len(rec.key)+len(rec.vary)),
		atime:     time.Now().Unix(),
		transient: true,
	}
	if !e.marker {
		w.mem.size = w.n
		e.hot = &hotData{rec: &rec, body: w.mem, cost: rec.memCost() + int64(len(w.mem.ids))*28}
	}

	// The requests that come from here on find the response in the cache,
	// which counts them. One that started reading it from the writer after
	// its count was taken would be missed, and the response left waiting for
	// a request more than it needs to be written to disk.
	w.pmu.Lock()
	uses := w.uses
	w.sealed = true
	w.pmu.Unlock()

	stripe := &s.stripes[e.id[0]]
	stripe.Lock()
	defer stripe.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errStoreClosed
	}

	// A file the index does not know of may be there while the store loads.
	hasFile := !s.loaded
	left := int(w.left)
	if old := s.index[e.id]; old != nil {
		e.hits, e.epoch = old.hits, old.epoch
		// The requests the previous version got meanwhile count, see
		// usesLeftLocked.
		if old.transient && !old.marker {
			left = min(left, int(old.left)+1)
		}
		hasFile = hasFile || !old.transient
		s.removeLocked(old)
	}
	if !e.marker {
		// The requests served from the response as it arrived, the one it
		// was fetched for among them, are as many it no longer waits for.
		e.left = uint16(max(left-uses, 0))
		// The blocks were counted when the writer obtained them. The index
		// gets a hold of its own on them: the writer keeps its until it is
		// done, for those who start reading from it meanwhile.
		w.mem.acquire()
	}
	s.forgetLocked(e.id)
	s.insertLocked(e, true)
	if !e.marker && e.left == 0 {
		s.queuePersistLocked(e)
	}
	victims := s.enforceLocked()
	s.mu.Unlock()

	if hasFile {
		// The file of the version this one replaces. Left there, it would
		// come back at the next start.
		_ = os.Remove(s.path(e.id))
	}

	return victims, nil
}

// adopt gives a transient entry the file that was written for it, which
// makes it an entry like the others. The caller holds the stripe of the
// entry.
func (s *Store) adopt(w *Writer, e *entry, size int64) ([]ID, error) {
	if err := s.ensureShard(e.id); err != nil {
		return nil, err
	}

	s.mu.Lock()
	ok := !s.closed && s.index[e.id] == e && e.transient
	s.mu.Unlock()
	if !ok {
		return nil, errGone
	}

	if err := os.Rename(w.tmp, s.path(e.id)); err != nil {
		s.dirMade[e.id[0]].Store(false)
		return nil, err
	}

	s.mu.Lock()
	if s.index[e.id] != e {
		s.mu.Unlock()
		// The entry was removed meanwhile. Nothing else can have been
		// stored under its name, which takes the stripe: the file is the
		// one just put there.
		_ = os.Remove(s.path(e.id))

		return nil, errGone
	}

	w.release()
	s.lruRemove(e)
	if e.hot != nil {
		// The body in memory becomes the copy of a file.
		s.transBlocks -= len(e.hot.body.ids)
		s.transMeta -= e.cost + e.hot.cost
		s.hotPushFront(e)
		s.hotCount++
	}
	e.transient, e.left = false, 0
	s.transients--
	e.size = size
	s.diskUsed.Add(size)
	s.lruPushFront(e)
	victims := s.enforceLocked()
	s.mu.Unlock()

	return victims, nil
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
	if e.transient {
		s.transients++
		if e.hot != nil {
			s.metaUsed += e.hot.cost
			s.transMeta += e.cost + e.hot.cost
		}
	}
}

func (s *Store) removeLocked(e *entry) {
	if e.hot != nil {
		s.demoteLocked(e)
	}
	delete(s.index, e.id)
	s.lruRemove(e)
	s.diskUsed.Add(-e.size)
	s.metaUsed -= e.cost
	if e.transient {
		s.transients--
	}
}

// demoteLocked drops the in-memory copy of an entry. Its memory is freed
// once the requests being served from it are done. For a transient response
// that copy is the response itself: it is only dropped with its entry.
func (s *Store) demoteLocked(e *entry) {
	hot := e.hot
	e.hot = nil
	if e.transient {
		s.transBlocks -= len(hot.body.ids)
		s.transMeta -= e.cost + hot.cost
	} else {
		s.hotRemove(e)
		s.hotCount--
	}
	s.metaUsed -= hot.cost
	hot.body.release()
}

// dropLocked removes a transient response that was not requested enough to
// be written to disk, which leaves nothing of it but the memory of how far
// it got.
func (s *Store) dropLocked(e *entry) {
	if s.limits.MaxMemory > 0 {
		s.rememberLocked(e)
	}
	s.removeLocked(e)
	s.dropped.Add(1)
}

// overTransientLocked tells whether the transient responses, with n more
// memory blocks, take more than they may: their share of the memory, or
// more than the index leaves.
func (s *Store) overTransientLocked(n int) bool {
	blocks := s.transBlocks + n

	return blocks > s.hotLimitLocked() ||
		s.transMeta+int64(blocks)*int64(s.arena.blockSize) > s.limits.MaxMemory/transientShare
}

// transientRoomLocked makes room for n more memory blocks of transient
// responses. Those requested least recently are dropped to stay within
// their share, and the copies of what is on disk make way. It reports
// whether there is room.
func (s *Store) transientRoomLocked(n int) bool {
	if n > s.hotLimitLocked() || int64(n)*int64(s.arena.blockSize) > s.limits.MaxMemory/transientShare {
		return false
	}

	for s.overTransientLocked(n) {
		if s.transTail == nil {
			// What is left is being received.
			return false
		}
		s.dropLocked(s.transTail)
	}
	for s.hotTail != nil && int(s.arena.inUse.Load())+n > s.hotLimitLocked() {
		s.demoteLocked(s.hotTail)
	}

	return true
}

// The transient responses that are dropped are remembered for a while, by
// their ID and the number of requests they still needed, in a table that
// forgets by itself: each ID has one slot, which the next to fall on it
// takes. A response that comes back is thereby not made to start over, and
// one that has been requested enough by then is written to disk at once:
// were it not, a response requested at intervals longer than memory holds
// it for would be fetched again every time.

// ghostSlotLocked returns the slot of id in the table and the value that
// names it there, less the count.
func (s *Store) ghostSlotLocked(id ID) (*uint64, uint64) {
	slot := binary.LittleEndian.Uint64(id[:8]) % uint64(len(s.ghosts))

	return &s.ghosts[slot], binary.LittleEndian.Uint64(id[8:]) &^ math.MaxUint16
}

func (s *Store) rememberLocked(e *entry) {
	if s.ghosts == nil {
		n := min(max(s.limits.MaxMemory/ghostShare/8, 1<<10), 1<<20)
		s.ghosts = make([]uint64, n)
		s.metaUsed += 8 * n
	}

	slot, tag := s.ghostSlotLocked(e.id)
	*slot = tag | uint64(e.left)
}

// recallLocked returns how many requests the response of id still needed
// when it was dropped, if it is remembered.
func (s *Store) recallLocked(id ID) (uint16, bool) {
	if s.ghosts == nil {
		return 0, false
	}

	slot, tag := s.ghostSlotLocked(id)
	if *slot == 0 || *slot&^math.MaxUint16 != tag {
		return 0, false
	}

	return uint16(*slot), true
}

func (s *Store) forgetLocked(id ID) {
	if _, ok := s.recallLocked(id); ok {
		slot, _ := s.ghostSlotLocked(id)
		*slot = 0
	}
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
		// The copies of what is on disk went first: they cost nothing to
		// lose. The transient responses have to fit in what is left, and in
		// their share.
		dropped := false
		for s.transTail != nil && s.overTransientLocked(0) {
			s.dropLocked(s.transTail)
			dropped = true
		}
		if dropped {
			continue
		}
		// Giving a lot of memory back takes time, which is not spent here
		// with the requests waiting.
		if s.arena.excess() {
			select {
			case s.trimCh <- struct{}{}:
			default:
			}
		}

		overDisk := s.diskUsed.Load()+s.tempBytes.Load() > s.limits.MaxSize
		overIndex := s.limits.MaxMemory > 0 && s.metaUsed > s.limits.MaxMemory
		if overIndex && s.transTail != nil {
			// They take more of the index than the files do.
			s.dropLocked(s.transTail)
			continue
		}
		if s.tail == nil || (!overDisk && !overIndex && !s.overFilesLocked()) {
			return victims
		}

		// A transient marker is listed with the files, without being one.
		if !s.tail.transient {
			victims = append(victims, s.tail.id)
		}
		s.removeLocked(s.tail)
		s.evicted.Add(1)
	}
}

// overFilesLocked tells whether the cache holds more files than allowed.
func (s *Store) overFilesLocked() bool {
	return s.limits.MaxFiles > 0 && int64(len(s.index)-s.transients)+s.tempFiles.Load() > s.limits.MaxFiles
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

// requestPersist asks for a transient response that was requested enough to
// be written to disk.
func (s *Store) requestPersist(e *entry) {
	// A response that is being fetched again is about to be replaced.
	s.fmu.Lock()
	fetching := s.flights[e.id] != nil
	s.fmu.Unlock()
	if fetching {
		return
	}

	s.mu.Lock()
	if s.index[e.id] == e {
		s.queuePersistLocked(e)
	}
	s.mu.Unlock()
}

// queuePersistLocked hands e to the persister. When that one is too far
// behind, the next request for the response asks again.
func (s *Store) queuePersistLocked(e *entry) {
	if s.closed || !e.transient || e.marker || e.persisting {
		return
	}

	select {
	case s.persistCh <- e:
		e.persisting = true
	default:
	}
}

func (s *Store) persister(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-s.persistCh:
			s.persist(e)

			s.mu.Lock()
			e.persisting = false
			s.mu.Unlock()
		}
	}
}

// persist writes a transient response to a file, from which point it is
// stored like any other and its body in memory is a copy.
func (s *Store) persist(e *entry) {
	var hot *hotData

	s.mu.Lock()
	if !s.closed && s.index[e.id] == e && e.transient && e.hot != nil {
		hot = e.hot
		hot.body.acquire()
	}
	s.mu.Unlock()
	if hot == nil {
		return
	}
	defer hot.body.release()

	// A variant nothing leads to anymore is not worth a file.
	if primaryKey(e.key) != e.key && !s.saveMarker(e.key) {
		return
	}

	rec := *hot.rec
	w, err := s.newWriter(&rec, 0, -1, 1)
	if err != nil {
		s.warn("writing a response held in memory to disk failed", err)
		return
	}
	w.adopt = e

	if err := hot.body.writeTo(w, hot.body.size); err != nil {
		w.Abort()
		s.warn("writing a response held in memory to disk failed", err)
		return
	}
	if w.Commit() == nil {
		s.persisted.Add(1)
	}
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
			s.arena.trim()
		case <-s.trimCh:
			s.arena.trim()
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
			if !s.tail.transient {
				victims = append(victims, s.tail.id)
			}
			s.removeLocked(s.tail)
		}
		for s.transTail != nil && s.transTail.atime < deadline {
			s.dropLocked(s.transTail)
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
	for s.transTail != nil {
		s.removeLocked(s.transTail)
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

// ends returns the ends of the list e belongs in by last use: the one of the
// transient responses or the one of the files, which the transient markers
// are in too.
func (s *Store) ends(e *entry) (head, tail **entry) {
	if e.transient && !e.marker {
		return &s.transHead, &s.transTail
	}

	return &s.head, &s.tail
}

func (s *Store) lruPushFront(e *entry) {
	head, tail := s.ends(e)

	e.prev, e.next = nil, *head
	if *head != nil {
		(*head).prev = e
	} else {
		*tail = e
	}
	*head = e
}

func (s *Store) lruPushBack(e *entry) {
	head, tail := s.ends(e)

	e.prev, e.next = *tail, nil
	if *tail != nil {
		(*tail).next = e
	} else {
		*head = e
	}
	*tail = e
}

func (s *Store) lruRemove(e *entry) {
	head, tail := s.ends(e)

	if e.prev != nil {
		e.prev.next = e.next
	} else {
		*head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		*tail = e.prev
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
	// alias is the ID the response is stored under when that is not the one
	// the flight was begun for, which aliased tells.
	alias   ID
	aliased bool
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
//
// A response that turns out to vary is stored under another ID than the one
// it was looked for, and fetched, under: there was no marker to tell. The
// requests for it that come from now on find the marker, and are to find
// the flight where it leads them rather than begin one of their own.
func (s *Store) ShareFlight(id ID, f *flight, w *Writer) {
	s.fmu.Lock()
	defer s.fmu.Unlock()

	if s.flights[id] == f && f.w == nil {
		f.w = w
		if w.id != id && s.flights[w.id] == nil {
			s.flights[w.id] = f
			f.alias, f.aliased = w.id, true
		}
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
	if f.aliased && s.flights[f.alias] == f {
		delete(s.flights, f.alias)
	}

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
// confirmed its body is still current. minUses is as for Create.
func (s *Store) Rewrite(old *Hit, rec *record, minUses int) error {
	w, err := s.newWriter(rec, 0, old.rec.bodyLen, minUses)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, old.Body()); err != nil {
		w.Abort()
		return err
	}

	return w.Commit()
}
