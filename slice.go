package httpcache

import (
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

// With the slice option, a response is asked of the upstream in ranges of a
// fixed size, the slices, and each slice is stored as a response of its own.
// A request for the middle of a large response then fetches the slices it
// reads and nothing else, instead of waiting for a download that starts at
// the first byte.
//
// The slices of a response are stored like the variants of a response that
// varies: the entry of the key is a marker, and the range of a slice takes
// part in the selection of what the marker leads to, beside the request
// headers the response varies on. What holds for variants therefore holds
// for slices without a line of the store knowing about them: one request to
// the upstream per slice however many requests want it, a slice served
// while it downloads, min_uses, and a purge of the key that leaves no slice
// reachable.
//
// A slice is stored as the upstream sent it: a 206 response whose
// Content-Range tells which part of what it is. No other response is ever
// stored with that status, which is how a slice is told from a response
// stored whole. A response is stored whole, under the slice option too, when
// it is not a slice of something larger: it fits in the first slice, it has
// another status than 206, or it cannot be had in slices at all.
//
// A request is answered by http.ServeContent reading a sliceReader, which
// makes one body of the slices: ranges and preconditions are evaluated by
// the standard library, as they are for a response stored whole.

// sliceSelector is the name under which the range of a slice takes part in
// the selection of what is stored for a key. It cannot be the name of a
// header.
const sliceSelector = ":slice"

var (
	// errSliceAnswered tells that the request was answered without slices.
	errSliceAnswered = errors.New("cache: the request was answered without slices")
	// errSliceBeyond tells that the response ends before the slice.
	errSliceBeyond = errors.New("cache: the response ends before the slice")
	// errSliceWhole tells that the response cannot be had in slices.
	errSliceWhole = errors.New("cache: the response cannot be fetched in slices")
	// errSliceChanged tells that the upstream no longer has the response
	// the slices sent so far belong to.
	errSliceChanged = errors.New("cache: the response changed while it was being sent")
	errSliceLost    = errors.New("cache: a slice of the response could not be fetched")
	errSliceClosed  = errors.New("cache: the response is not being read anymore")
)

// withSliceSelector adds the selector of the slices to the names of the
// request headers a response varies on.
func withSliceSelector(names []string) []string {
	names = append(names, sliceSelector)
	slices.Sort(names)

	return names
}

// sliceRange formats the range of a slice, as it selects the slice in the
// cache and as the upstream is asked for it.
func sliceRange(first, last int64) string {
	return strconv.FormatInt(first, 10) + "-" + strconv.FormatInt(last, 10)
}

// parseContentRange reads the Content-Range of a 206 response: the first
// and last byte it holds and the size of the whole. A response that does
// not tell all three is of no use here.
func parseContentRange(value string) (first, last, total int64, ok bool) {
	spec, isBytes := strings.CutPrefix(value, "bytes ")
	if !isBytes {
		return 0, 0, 0, false
	}
	span, size, _ := strings.Cut(spec, "/")
	from, to, _ := strings.Cut(span, "-")

	var err [3]error
	first, err[0] = strconv.ParseInt(strings.TrimSpace(from), 10, 64)
	last, err[1] = strconv.ParseInt(strings.TrimSpace(to), 10, 64)
	total, err[2] = strconv.ParseInt(strings.TrimSpace(size), 10, 64)
	if err != [3]error{} || first < 0 || first > last || last >= total {
		return 0, 0, 0, false
	}

	return first, last, total, true
}

// firstSlice returns the slice a request reads first: the one its range
// starts in, when that can be told before the size of the response is
// known, and the first one otherwise.
func firstSlice(r *http.Request, size int64) int64 {
	if r.Method != http.MethodGet {
		return 0
	}
	spec, isBytes := strings.CutPrefix(r.Header.Get("Range"), "bytes=")
	if !isBytes {
		return 0
	}

	from, _, _ := strings.Cut(spec, "-")
	start, err := strconv.ParseInt(strings.TrimSpace(from), 10, 64)
	if err != nil || start < 0 || start > math.MaxInt64-size {
		return 0
	}

	return start / size
}

// sliceFetch is the fetch of one slice. It runs beside the request it is
// for, which reads the slice from the cache as it arrives, like any other
// request for it does.
type sliceFetch struct {
	// first and last bound the slice in the response.
	first, last int64
	// header holds the request headers and the range of the slice, which
	// together select the slice among what is stored for the key.
	header http.Header
	// whole is the ID the response has when it is stored whole. Unlike the
	// ID of the slice, it is the same for every slice of the response.
	whole ID
	// lead tells that the client was not sent anything yet. The fetch may
	// then answer the request itself, which it does when the upstream sends
	// something else than a slice. Otherwise nothing but a slice is of use.
	lead bool
	// request is held while the request is changed into the one for the
	// slice and back, see sliceWriter.
	request *sync.Mutex
	done    chan struct{}

	// What follows is the outcome, for the request to read once done is
	// closed.
	//
	// sliced tells that the upstream sent the slice, and stored that it is
	// in the cache now.
	sliced, stored bool
	// beyond tells that the response ends before the slice.
	beyond bool
	// unsliced tells that the upstream sent something that cannot be stored
	// as a slice, where the response may well be storable whole.
	unsliced bool
	// changed tells that the upstream answered with another response than
	// the one the client is being sent.
	changed bool
	// hit is the stored slice to serve, when the upstream confirmed it or
	// failed, with the parameters of Cache-Status that say so.
	hit    *Hit
	params string
	// err is the result of the handler when the fetch answered the request,
	// and panicked what it panicked with.
	err      error
	panicked any
}

// passable tells whether the request can still be sent to the upstream as
// the client made it.
func (sf *sliceFetch) passable() bool {
	return sf.lead && !sf.beyond && !sf.unsliced
}

// sliceSink stands for the client in the fetch of a slice that follows
// others the client was already sent: nothing of that fetch is for the
// client to see.
type sliceSink struct {
	header http.Header
}

func (s *sliceSink) Header() http.Header       { return s.header }
func (*sliceSink) WriteHeader(int)             {}
func (*sliceSink) Write(p []byte) (int, error) { return len(p), nil }

// sliceWriter is the client as the response made of slices is written to
// it. It records the status of the response, and keeps the request still
// while the header is written: that is when the handlers in front of the
// cache set the response headers they were asked to set late, for which
// they may look at the request, and the fetch of the slice being sent may
// be done at that very moment, and putting the request back as it was.
//
// The body is written without the lock: a client that reads slowly is not
// to hold up a fetch.
type sliceWriter struct {
	*caddyhttp.ResponseWriterWrapper
	status  int
	request *sync.Mutex
}

func (w *sliceWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}

	w.request.Lock()
	defer w.request.Unlock()
	w.ResponseWriterWrapper.WriteHeader(code)
}

// sliceHeaderLocked decides what becomes of the response to a request for
// one slice. It reports whether it did. A response that is not a slice of a
// larger one is left to be handled like the answer to any request, provided
// the client can still be sent it.
func (fw *fetchWriter) sliceHeaderLocked(sf *sliceFetch, now time.Time) bool {
	switch fw.status {
	case http.StatusRequestedRangeNotSatisfiable:
		sf.beyond = true
		fw.giveUpLocked()

		return true

	case http.StatusPartialContent:
		first, last, total, ok := parseContentRange(fw.hdr.Get("Content-Range"))
		if !ok || first != sf.first || last != min(sf.last, total-1) {
			// Not the range that was asked for, or of a response whose size
			// is not known.
			sf.unsliced = true
			fw.giveUpLocked()

			return true
		}

		if first == 0 && last == total-1 {
			// The response fits in its first slice. It is what a request
			// without a range would have been answered, and is stored as
			// such: one file, and nothing to put together.
			fw.hdr.Del("Content-Range")
			fw.status = http.StatusOK
			break
		}

		// A body that a handler on the way transformed, by compressing it
		// for instance, is no longer the range it claims to be. Such a
		// handler removes the length, which it cannot know.
		if length, err := strconv.ParseInt(fw.hdr.Get("Content-Length"), 10, 64); err != nil || length != last-first+1 {
			sf.unsliced = true
			fw.giveUpLocked()

			return true
		}

		fw.storeSliceLocked(sf, last-first+1, total, now)

		return true
	}

	if sf.lead {
		return false
	}

	// The client is being sent a response the upstream has no more slices
	// of. An error that may pass says nothing of the response.
	sf.changed = fw.status < http.StatusBadRequest || fw.status == http.StatusNotFound || fw.status == http.StatusGone
	fw.giveUpLocked()

	return true
}

// storeSliceLocked starts storing the slice the upstream is sending, if the
// response it is part of may be stored.
func (fw *fetchWriter) storeSliceLocked(sf *sliceFetch, length, total int64, now time.Time) {
	x := fw.x
	c := x.c
	s := x.store

	own := ownHeaders(fw.base, fw.hdr)
	if c.defaultCC != "" && own.Get("Cache-Control") == "" {
		own.Set("Cache-Control", c.defaultCC)
		fw.hdr.Add("Cache-Control", c.defaultCC)
	}

	// A slice is judged like the response it is part of.
	v := c.evaluate(x.r, http.StatusOK, own, now)
	if v.store && c.maxBody > 0 && total > c.maxBody {
		v = reject("TOO-LARGE")
	}
	uncacheable := !v.store

	if v.store {
		fw.declared = length

		w, err := s.Create(x.key, withSliceSelector(v.vary), sf.header, fw.recordLocked(v, now), 0, length, c.minUses)
		if err == nil {
			fw.w = w
			fw.mode = modeSlice
			sf.sliced = true
			// The response was stored whole and no longer fits in a slice.
			// Where it varies, what was stored is not replaced by what leads
			// to the slices, and would be found before them forever.
			if fw.stale != nil && fw.stale.rec.status != http.StatusPartialContent {
				fw.stale.Discard()
			}
			// The request the slice is fetched for reads it this way too.
			if x.flight != nil {
				s.ShareFlight(x.flightID, x.flight, w)
			}

			return
		}
		s.warn("storing a response failed", err)
		v = reject("STORAGE-ERROR")
	}

	fw.reason = v.reason
	if uncacheable {
		s.SetUncacheable(fw.id, true)
		if fw.stale != nil {
			fw.stale.Discard()
		}
	}
	fw.giveUpLocked()
}

// giveUpLocked ends a fetch whose response is of no use.
func (fw *fetchWriter) giveUpLocked() {
	fw.mode = modeAbort
	fw.selfAborted = true
	fw.x.endFlight(false)
	fw.cancel()
}

// slicePart is one slice, open for reading.
type slicePart struct {
	n   int64
	rec *record
	// total is the size of the response the slice says it is part of, and
	// length its own.
	total, length int64
	// body reads the slice from the cache: from hit when it is stored, from
	// tail while it is being received.
	body io.ReadSeeker
	pos  int64
	hit  *Hit
	tail *Tail
	// params are the parameters of Cache-Status telling how the slice was
	// come by.
	params string
}

func (p *slicePart) close() {
	if p.tail != nil {
		p.tail.Close()
	}
	p.hit.Close()
}

// sliceReader is the body of a response stored in slices, read and seeked
// like a file. The slices are taken from the cache, or fetched, as the
// reading gets to them.
//
// http.ServeContent reads the body of a response to a request for several
// ranges from a goroutine of its own, which it does not wait for when the
// client leaves. Reading is therefore done under a lock, and comes to an end
// with shut: from then on that goroutine finds nothing to read, and the
// request can release what the reader holds.
type sliceReader struct {
	x    *exchange
	size int64
	// ctx ends with the request, or when the body is not read anymore.
	ctx    context.Context
	cancel context.CancelFunc
	// base holds the response headers as the cache found them, and plain
	// the request headers. The request and the response themselves are not
	// to be read while a fetch is running, which works on both.
	base  http.Header
	plain http.Header
	// header is plain with the range of the slice wanted, which selects the
	// slice in the cache.
	header http.Header
	// primary is the ID of the key, and whole the one the response has when
	// it is stored whole.
	primary, whole ID
	// request is held by whoever changes the request, or lets others look
	// at it, while a fetch may be running.
	request sync.Mutex
	sw      *sliceWriter

	// lead tells that the client was not sent anything yet: until then the
	// request can still be answered another way than from slices. answer is
	// the result of the handler when it was.
	lead   bool
	answer error

	// total is the size of the response, and etag and modified what
	// identifies it: every slice has to be part of the same response as the
	// first one sent.
	total          int64
	etag, modified string
	off            int64
	part           *slicePart
	// pending is the fetch started last, which is waited for before the
	// next one and before the request ends.
	pending *sliceFetch
	// panicked is what a fetch panicked with, for the request to raise
	// again when it ends, see wait.
	panicked any
	// flushing tells that the client is sent what there is while more is
	// awaited. Not when it asked for several ranges: the one waiting is then
	// not the one writing to it.
	flushing bool

	// mu guards the reading, from when the response is being sent: closed
	// tells that it came to an end, and err is the reason the body could
	// not be read to its end.
	mu     sync.Mutex
	closed bool
	err    error
}

// serveSliced answers a GET or HEAD request from the slices of the
// response, fetching the missing ones as they are read.
func (x *exchange) serveSliced() error {
	sr := &sliceReader{
		x:       x,
		size:    x.c.slice,
		base:    x.w.Header().Clone(),
		plain:   x.r.Header.Clone(),
		header:  x.r.Header.Clone(),
		primary: makeID(x.key),
		lead:    true,
	}
	sr.ctx, sr.cancel = context.WithCancel(x.r.Context())
	sr.flushing = !strings.Contains(sr.plain.Get("Range"), ",")
	sr.sw = &sliceWriter{ResponseWriterWrapper: &caddyhttp.ResponseWriterWrapper{ResponseWriter: x.w}, request: &sr.request}
	defer sr.close()

	n := firstSlice(x.r, sr.size)
	part, err := sr.acquire(n)
	if err == errSliceBeyond && n > 0 {
		// The response ends before the range asked for. Its first slice
		// tells where, which the answer has to say.
		part, err = sr.acquire(0)
	}

	switch err {
	case nil:
		return sr.serve(part)
	case errSliceAnswered:
		return sr.answer
	case errSliceBeyond, errSliceWhole:
		// There are no slices to be had, not even a first one: the response
		// is empty, or the upstream does not send it that way.
		return x.serve()
	}
	if err := x.r.Context().Err(); err != nil {
		return err
	}

	return caddyhttp.Error(http.StatusBadGateway, err)
}

// serve sends the client the response the given slice is part of, starting
// with what that slice says of it.
func (sr *sliceReader) serve(part *slicePart) error {
	x := sr.x
	rec := part.rec

	sr.lead = false
	sr.part = part
	sr.total = part.total
	sr.etag, sr.modified = rec.header.Get("Etag"), rec.header.Get("Last-Modified")

	header := x.w.Header()
	applyHeaders(header, rec.header)
	// It describes the slice, not what the client is sent.
	header.Del("Content-Range")
	header.Set("Age", strconv.FormatInt(int64(age(rec, time.Now())/time.Second), 10))
	header.Add("Cache-Status", x.status(part.params))

	var modified time.Time
	if t, err := http.ParseTime(header.Get("Last-Modified")); err == nil {
		modified = t
	}

	// The request as it was received: a fetch may be working on the real
	// one at this very moment.
	r := x.r.WithContext(x.r.Context())
	r.Header = sr.plain

	http.ServeContent(bodyWriter{sr.sw}, r, "", modified, sr)

	if sr.shut() != nil {
		// A slice is missing or the client left: the response is cut short,
		// which the client must be able to tell.
		panic(http.ErrAbortHandler)
	}

	return nil
}

// shut ends the reading, and returns the reason the body was not read to
// its end if it was not. It returns once nobody is reading.
func (sr *sliceReader) shut() error {
	// Whoever is still reading is waiting for a slice nobody will be sent.
	sr.cancel()

	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.closed = true

	return sr.err
}

// answered records that the request was answered without slices.
func (sr *sliceReader) answered(err error) error {
	sr.answer = err

	return errSliceAnswered
}

// close releases the slice being read and waits for the fetch in progress,
// which is not to outlive the request it works on. It is called by the
// request itself, which is where the panic of a fetch belongs.
func (sr *sliceReader) close() {
	_ = sr.shut()
	sr.closePart()
	sr.wait()

	if sr.panicked != nil {
		// As if the handlers had been called from here.
		panic(sr.panicked)
	}
}

func (sr *sliceReader) closePart() {
	if sr.part != nil {
		sr.part.close()
		sr.part = nil
	}
}

// wait returns once no fetch is running for the request. A fetch that
// panicked is not made to do so again from here: the body may be read from
// a goroutine of http.ServeContent, where a panic is not that of a request
// anymore but the end of the server. It is kept for close to raise.
func (sr *sliceReader) wait() {
	sf := sr.pending
	if sf == nil {
		return
	}
	sr.pending = nil

	<-sf.done
	if sf.panicked != nil && sr.panicked == nil {
		sr.panicked = sf.panicked
	}
}

// flush sends the client what it was written, once that is a response.
func (sr *sliceReader) flush() {
	if sr.flushing && sr.sw.status != 0 {
		_ = http.NewResponseController(sr.x.w).Flush()
	}
}

// Seek implements io.Seeker.
func (sr *sliceReader) Seek(offset int64, whence int) (int64, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	switch whence {
	case io.SeekCurrent:
		offset += sr.off
	case io.SeekEnd:
		offset += sr.total
	}
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	sr.off = offset

	return offset, nil
}

// Read implements io.Reader. It reads from the slice the current offset is
// in, which it gets hold of first if it is not the one read last.
func (sr *sliceReader) Read(p []byte) (int, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.closed {
		return 0, errSliceClosed
	}
	if sr.err != nil {
		return 0, sr.err
	}
	if sr.off >= sr.total {
		return 0, io.EOF
	}

	n := sr.off / sr.size
	if sr.part == nil || sr.part.n != n {
		sr.closePart()

		part, err := sr.acquire(n)
		if err == nil && (part.total != sr.total || part.rec.header.Get("Etag") != sr.etag || part.rec.header.Get("Last-Modified") != sr.modified) {
			part.close()
			err = errSliceChanged
		}
		if err == errSliceBeyond {
			err = errSliceChanged
		}
		if err == errSliceChanged {
			// The slices the cache holds are those of a response the
			// upstream no longer has, or a mix of two. The next request
			// starts over.
			sr.x.store.Purge(sr.x.key)
			sr.x.h.logger.Warn("a response changed while it was being sent in slices", zap.String("key", sr.x.key))
		}
		if err != nil {
			sr.err = err
			return 0, err
		}
		sr.part = part
	}

	part := sr.part
	if at := sr.off - n*sr.size; part.pos != at {
		if _, err := part.body.Seek(at, io.SeekStart); err != nil {
			sr.err = err
			return 0, err
		}
		part.pos = at
	}

	p = p[:min(int64(len(p)), part.length-part.pos)]
	m, err := part.body.Read(p)
	part.pos += int64(m)
	sr.off += int64(m)
	if m > 0 || err == nil {
		return m, nil
	}

	if err == io.EOF {
		// The slice is shorter than it says.
		err = io.ErrUnexpectedEOF
	}
	if errors.Is(err, syscall.EIO) && part.hit != nil {
		// The file cannot be read back: stop serving it.
		part.hit.Discard()
		sr.x.h.logger.Error("reading a cached response failed", zap.String("key", sr.x.key), zap.Error(err))
	}
	sr.err = err

	return 0, err
}

// newPart describes slice n as rec stores it, with a body of the given
// length. It returns nil if rec is not that slice.
func (sr *sliceReader) newPart(n int64, rec *record, length int64) *slicePart {
	first, last, total, ok := parseContentRange(rec.header.Get("Content-Range"))
	if !ok || first != n*sr.size || last != min(first+sr.size, total)-1 || length != last-first+1 {
		return nil
	}

	return &slicePart{n: n, rec: rec, total: total, length: length}
}

// tailPart opens slice n for reading while w receives it. It returns nil if
// that is not what w receives, or if it cannot be read anymore.
func (sr *sliceReader) tailPart(n int64, w *Writer) *slicePart {
	rec := w.rec
	if rec.status != http.StatusPartialContent || rec.key != variantKey(sr.x.key, w.varySpec, sr.header) {
		return nil
	}

	part := sr.newPart(n, rec, w.declared)
	if part == nil {
		return nil
	}
	tail, err := w.Tail(sr.ctx)
	if err != nil {
		return nil
	}
	// While the rest of the slice is awaited, the client gets what there is.
	tail.flush = sr.flush
	part.tail, part.body = tail, tail

	return part
}

// lookup finds what is stored for slice n. For a request that was not
// answered yet that may be the whole response, which is looked for first:
// where both are found, a response that was stored whole since is the one
// that says what the upstream sends now.
func (sr *sliceReader) lookup(n int64) (ID, *Hit) {
	x := sr.x
	s := x.store

	if sr.lead {
		id, hit := s.find(x.key, sr.plain)
		sr.whole = id
		// Without a marker there is one place to look in.
		if hit != nil || id == sr.primary {
			s.count(hit)
			return id, hit
		}
	}

	id, hit := s.find(x.key, sr.header)
	s.count(hit)

	return id, hit
}

// fetch starts fetching slice n for the request, which holds the flight fl
// for it. stale is the expired response the cache holds, if any.
func (sr *sliceReader) fetch(id ID, fl *flight, stale *Hit, n int64) *sliceFetch {
	x := sr.x
	first := n * sr.size
	sf := &sliceFetch{
		first:   first,
		last:    first + sr.size - 1,
		header:  maps.Clone(sr.header),
		whole:   sr.whole,
		lead:    sr.lead,
		request: &sr.request,
		done:    make(chan struct{}),
	}
	sr.pending = sf

	sub := &exchange{h: x.h, c: x.c, store: x.store, w: x.w, r: x.r, next: x.next, key: x.key, start: x.start, reqCC: x.reqCC, slice: sf}
	if !sr.lead {
		// The headers of the client response are those of the first slice
		// by now, which is not what a response is to be compared with to
		// tell what the upstream gave it.
		sub.w = &sliceSink{header: sr.base.Clone()}
	}

	go func() {
		defer close(sf.done)
		defer func() {
			// A panic is the request's to raise, not this goroutine's.
			sf.panicked = recover()
			if sf.hit != stale {
				stale.Close()
			}
		}()

		sf.err = sub.fetch(id, fl, stale)
	}()

	return sf
}

// acquire gets hold of slice n, from the cache or from the upstream. It is
// to slices what exchange.serve is to whole responses, and answers the
// request like it does when there turn out to be no slices to read: the
// error is then errSliceAnswered. That only happens while the client was
// not sent anything.
func (sr *sliceReader) acquire(n int64) (*slicePart, error) {
	x := sr.x
	s := x.store

	// The fetches of a request run one at a time: the upstream handlers work
	// on the request itself, and on what its context carries. The one for
	// the slice read before this one is over, or about to be.
	sr.wait()
	if sr.panicked != nil {
		return nil, errSliceLost
	}

	// From here on the request is one for this slice, to the cache and to
	// those it shares a fetch with.
	first := n * sr.size
	sr.header[sliceSelector] = []string{sliceRange(first, first+sr.size-1)}

	var (
		joined, seen *flight
		deadline     time.Time
		// fetched tells how the request came by the slice it fetched itself.
		fetched string
	)
	for {
		id, hit := sr.lookup(n)
		now := time.Now()

		var part *slicePart
		if hit != nil && hit.rec.status == http.StatusPartialContent {
			if part = sr.newPart(n, hit.rec, hit.rec.bodyLen); part == nil {
				// Not the slice its key says it is.
				hit.Discard()
				hit.Close()
				hit = nil
			} else {
				part.hit, part.body = hit, hit.Body()
			}
		}
		// What is not a slice is a response stored whole, which is of no use
		// to a client that was sent the beginning of another.
		if hit != nil && part == nil && !sr.lead {
			hit.Close()
			return nil, errSliceLost
		}

		if hit != nil && x.usable(hit.rec, now, joined) {
			params := x.hitParams(hit, now)
			switch {
			case fetched != "":
				params = fetched
			case joined != nil:
				params = "fwd=uri-miss; collapsed"
			}
			if part == nil {
				return nil, sr.answered(x.serveHit(hit, now, params))
			}
			part.params = params

			return part, nil
		}

		// One fetch is all a request makes for a slice.
		if fetched != "" {
			hit.Close()
			return nil, errSliceLost
		}

		if !sr.lead {
			if x.reqCC.has("only-if-cached") || x.r.Method == http.MethodHead {
				hit.Close()
				return nil, errSliceLost
			}
		} else {
			if x.reqCC.has("only-if-cached") {
				hit.Close()
				return nil, sr.answered(x.notCached())
			}

			// The cache is filled by GET requests only, but a HEAD request
			// can be answered from a response on its way in.
			if x.r.Method == http.MethodHead {
				hit.Close()
				if w := s.SharedFlight(id); w != nil {
					if part := sr.tailPart(n, w); part != nil {
						part.params = "fwd=uri-miss; collapsed"
						return part, nil
					}
					if w.rec.status != http.StatusPartialContent && x.serveTail(w) {
						return nil, sr.answered(nil)
					}
				}

				return nil, sr.answered(x.pass("HEAD"))
			}

			// Finding out again that the response cannot be stored would
			// take a request for a slice nobody has a use for.
			if s.Uncacheable(sr.whole) {
				hit.Close()
				return nil, sr.answered(x.pass("UNCACHEABLE"))
			}
		}

		fl, leader := s.BeginFlight(id)
		if leader {
			forward := "fwd=uri-miss"
			if hit != nil {
				forward = "fwd=stale"
			}

			// The fetch runs beside the request, which reads the slice from
			// the cache as soon as it is on its way there, like the requests
			// waiting on the flight do.
			sf := sr.fetch(id, fl, hit, n)
			select {
			case <-fl.started:
				if part := sr.tailPart(n, fl.w); part != nil {
					part.params = forward + "; stored"
					return part, nil
				}
				// A whole response, which the fetch sends the client by
				// itself, or a slice that was stored before it could be
				// read.
				<-sf.done
			case <-sf.done:
			}
			sr.wait()

			switch {
			case sf.panicked != nil:
				// The request raises it when it ends, see close.
				return nil, errSliceLost
			case sf.hit != nil:
				part := sr.newPart(n, sf.hit.rec, sf.hit.rec.bodyLen)
				if part == nil {
					sf.hit.Close()
					return nil, errSliceLost
				}
				part.hit, part.body, part.params = sf.hit, sf.hit.Body(), sf.params

				return part, nil
			case sf.stored:
				joined, fetched = fl, forward+"; stored"
				continue
			case sf.beyond:
				return nil, errSliceBeyond
			case sf.unsliced && sr.lead:
				return nil, errSliceWhole
			case sf.changed:
				return nil, errSliceChanged
			case sf.sliced || !sr.lead:
				return nil, errSliceLost
			}

			// The fetch answered the request.
			return nil, sr.answered(sf.err)
		}

		// Another request is fetching the response.
		if hit != nil && x.staleUsable(hit.rec, now, hit.rec.swr) {
			params := x.hitParams(hit, now) + "; detail=UPDATING"
			if part == nil {
				return nil, sr.answered(x.serveHit(hit, now, params))
			}
			part.params = params

			return part, nil
		}
		hit.Close()

		if deadline.IsZero() {
			deadline = now.Add(x.c.lockTimeout)
		}
		timer := time.NewTimer(time.Until(deadline))
		started := fl.started
		if fl == seen {
			started = nil
		}
	wait:
		for joined != fl {
			select {
			case <-fl.done:
				joined = fl
			case <-started:
				started, seen = nil, fl

				if part := sr.tailPart(n, fl.w); part != nil {
					timer.Stop()
					part.params = "fwd=uri-miss; collapsed"

					return part, nil
				}
				if fl.w.rec.status == http.StatusPartialContent {
					// Another slice of the response, the first one to be
					// stored: the marker it came with tells under which ID
					// the one wanted here is to be fetched.
					break wait
				}
				if !sr.lead {
					timer.Stop()
					return nil, errSliceLost
				}
				if x.serveTail(fl.w) {
					timer.Stop()
					return nil, sr.answered(nil)
				}
			case <-timer.C:
				if !sr.lead {
					return nil, errSliceLost
				}

				return nil, sr.answered(x.pass("LOCK-TIMEOUT"))
			case <-sr.ctx.Done():
				timer.Stop()
				return nil, sr.ctx.Err()
			}
		}
		timer.Stop()
	}
}
