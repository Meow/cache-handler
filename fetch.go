package httpcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// clientGoneGrace is how long a fetch whose client left may go without
// receiving anything from the upstream before it is given up.
var clientGoneGrace = 30 * time.Second

var (
	errFetchAborted = errors.New("cache: fetch aborted")
	errClientGone   = errors.New("cache: client disconnected")
)

// fetchMode is what becomes of the response the upstream is sending.
type fetchMode int

const (
	// modeUndecided: the upstream has not answered yet.
	modeUndecided fetchMode = iota
	// modePass: relayed to the client, not stored.
	modePass
	// modeRelay: stored, while the client is sent the response, or the one
	// range of it that it asked for, from what is stored so far.
	modeRelay
	// modeSilent: stored only. The client sent a precondition or asked for
	// something else than one plain range, which is answered from the stored
	// response afterwards.
	modeSilent
	// modeRevalidated: the upstream confirmed the stale response.
	modeRevalidated
	// modeStaleError: the upstream failed and the stale response is served
	// in place of its error.
	modeStaleError
	// modeAbort: the response is of no use to the cache and cannot be
	// relayed as is, so the client's own request is sent to the upstream.
	modeAbort
)

// fetchWriter is the ResponseWriter given to the upstream handlers when the
// cache fetches a response. It decides what to do with the response when its
// headers arrive, then streams the body accordingly without ever holding it.
//
// A response that is stored goes from the upstream to the file only. The
// client is served from the file by a separate goroutine, the pump, like the
// other requests reading the response while it downloads: a client that
// reads slowly holds up neither the download nor the others.
type fetchWriter struct {
	x  *exchange
	id ID
	rw http.ResponseWriter
	// base is the header map of the client response and hdr the copy the
	// upstream handlers work on. Their difference is what gets stored.
	base http.Header
	hdr  http.Header
	// stale is the expired response the cache holds, if any.
	stale *Hit
	// plain tells that the client can be sent the response as it comes.
	plain bool
	// rangeHeader is the range the client asked for, when that is all that
	// keeps the response from being relayed as it comes.
	rangeHeader string
	// revalidating tells that the request carries the validators of stale.
	revalidating bool
	// reqHeader holds the request headers as the client sent them. They are
	// what selects the variant a response is stored as, like they are when
	// it is looked up: the upstream handlers may change the request since.
	reqHeader http.Header
	// forward is the Cache-Status reason the request was forwarded.
	forward string
	cancel  context.CancelFunc

	// mu guards what follows: the client leaving is noticed from another
	// goroutine.
	mu       sync.Mutex
	mode     fetchMode
	status   int
	reason   string
	w        *Writer
	declared int64
	// pos is how much of the body went by. first and last bound what the
	// client is sent of it in modeRelay.
	pos, first, last int64
	// pump is closed when the goroutine serving the client from the file is
	// done, and pumpErr is why it stopped early, if it did. While the pump
	// runs, nothing else writes to the client.
	pump    chan struct{}
	pumpErr error
	tail    *Tail
	// selfAborted tells that the cache itself cut the fetch short, which is
	// not a failure of the upstream.
	selfAborted bool
	// satisfied tells that the fetch was cut short because the client had
	// been sent all of the range it asked for.
	satisfied bool
	gone      bool
	finished  bool
	idle      *time.Timer
}

func newFetchWriter(x *exchange, id ID, stale *Hit) *fetchWriter {
	fw := &fetchWriter{
		x:        x,
		id:       id,
		rw:       x.w,
		base:     x.w.Header(),
		hdr:      x.w.Header().Clone(),
		stale:    stale,
		plain:    plainRequest(x.r),
		forward:  "fwd=uri-miss",
		reason:   "UPSTREAM-ERROR",
		declared: -1,
	}
	if stale != nil {
		fw.forward = "fwd=stale"
	}
	if x.r.Method == http.MethodGet && !conditional(x.r) && x.r.Header.Get("If-Range") == "" {
		fw.rangeHeader = x.r.Header.Get("Range")
	}

	return fw
}

// plainRequest tells whether the request asks for a whole response without
// condition, which is what the upstream is asked for.
func plainRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && r.Header.Get("Range") == "" && !conditional(r)
}

// prepareRequest turns the client request into one for the whole response:
// a stored response must not depend on the range or the preconditions of the
// client that happened to trigger the fetch. The validators of the stale
// response are sent instead, so the upstream can answer that it is still
// current.
//
// The returned function puts the request back as it was received, undoing
// as well what the upstream handlers changed in it, a rewrite for instance:
// the request may have to be handled a second time.
func (fw *fetchWriter) prepareRequest(r *http.Request) (restore func()) {
	header := r.Header.Clone()
	url := *r.URL
	fw.reqHeader = header

	for _, name := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since"} {
		delete(r.Header, name)
	}

	if fw.stale != nil && fw.stale.rec.status == http.StatusOK {
		if etag := fw.stale.rec.header.Get("Etag"); etag != "" {
			r.Header.Set("If-None-Match", etag)
			fw.revalidating = true
		}
		if modified := fw.stale.rec.header.Get("Last-Modified"); modified != "" {
			r.Header.Set("If-Modified-Since", modified)
			fw.revalidating = true
		}
	}

	return func() {
		clear(r.Header)
		maps.Copy(r.Header, header)
		*r.URL = url
	}
}

// Header implements http.ResponseWriter.
func (fw *fetchWriter) Header() http.Header {
	return fw.hdr
}

// Unwrap lets http.ResponseController reach the client connection.
func (fw *fetchWriter) Unwrap() http.ResponseWriter {
	return fw.rw
}

// WriteHeader implements http.ResponseWriter. It is where the fate of the
// response is decided.
func (fw *fetchWriter) WriteHeader(code int) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.writeHeaderLocked(code)
}

func (fw *fetchWriter) writeHeaderLocked(code int) {
	// Informational responses are not relayed.
	if fw.mode != modeUndecided || code < 200 {
		return
	}
	fw.status = code

	x := fw.x
	c := x.c
	s := x.store
	now := time.Now()

	if code == http.StatusNotModified && fw.revalidating {
		fw.mode = modeRevalidated
		return
	}
	if code >= 500 && fw.stale != nil && x.staleUsable(fw.stale.rec, now, fw.stale.rec.sie) {
		fw.mode = modeStaleError
		return
	}

	// The decision is taken on what the upstream handlers answered, not on
	// what the handlers in front of the cache add to every response.
	own := ownHeaders(fw.base, fw.hdr)
	if c.defaultCC != "" && own.Get("Cache-Control") == "" {
		own.Set("Cache-Control", c.defaultCC)
		fw.hdr.Add("Cache-Control", c.defaultCC)
	}

	v := c.evaluate(x.r, code, own, now)
	// uncacheable tells that the response itself cannot be stored, as
	// opposed to this one attempt at storing it.
	uncacheable := !v.store

	// The size the response announces is known not to fit before any of it
	// is written.
	limit := s.maxObject()
	if c.maxBody > 0 && c.maxBody < limit {
		limit = c.maxBody
	}
	if n, err := strconv.ParseInt(fw.hdr.Get("Content-Length"), 10, 64); err == nil && n >= 0 {
		fw.declared = n
		if v.store && n > limit {
			v, uncacheable = reject("TOO-LARGE"), true
		}
	}

	// Nobody is waiting for the response anymore. It is only worth storing
	// for later if it is known to end.
	if v.store && fw.gone && fw.declared < 0 {
		v = reject("CLIENT-GONE")
	}

	if v.store {
		stored := headerDiff(fw.base, fw.hdr)
		if stored.Get("Date") == "" {
			stored.Set("Date", now.UTC().Format(http.TimeFormat))
		}

		rec := &record{
			stored: now.UnixMilli(),
			fresh:  now.Add(v.lifetime).UnixMilli(),
			swr:    seconds(v.swr),
			sie:    seconds(v.sie),
			age:    seconds(v.age),
			status: code,
			header: stored,
		}
		if v.mustRevalidate {
			rec.flags |= flagMustRevalidate
		}

		w, err := s.Create(x.key, v.vary, fw.reqHeader, rec, limit, fw.declared, c.minUses)
		if err != nil {
			s.warn("storing a response failed", err)
			v = reject("STORAGE-ERROR")
		} else {
			fw.w = w
			// From now on the requests waiting for this one are served from
			// the response as it arrives. That is only offered for a
			// response of known length: one that may turn out too large to
			// store would be cut short for them when it does.
			if x.flight != nil && fw.declared >= 0 {
				s.ShareFlight(x.flightID, x.flight, w)
			}
		}
	}

	if !v.store {
		fw.reason = v.reason
		if uncacheable {
			s.SetUncacheable(fw.id, true)
			// The upstream no longer gives a response to keep, so the one
			// it gave before is not to be served anymore either.
			if fw.stale != nil && code < 500 {
				fw.stale.Discard()
			}
		}
		// Nothing will come of this fetch for the requests waiting on it.
		x.endFlight(false)
	}

	switch {
	case v.store && fw.plain:
		fw.mode = modeRelay
		fw.first, fw.last = 0, math.MaxInt64-1
		fw.mergeHeaderLocked()
		fw.base.Add("Cache-Status", x.status(fw.forward+"; stored"))
		fw.rw.WriteHeader(code)
		fw.startPumpLocked(-1)
	case v.store && fw.relayRangeLocked():
		fw.mode = modeRelay
		fw.startPumpLocked(fw.last - fw.first + 1)
	case v.store:
		fw.mode = modeSilent
	case fw.plain:
		fw.mode = modePass
		fw.mergeHeaderLocked()
		fw.base.Add("Cache-Status", x.status(fw.forward+"; detail="+v.reason))
		fw.rw.WriteHeader(code)
		if fw.gone {
			fw.cancel()
		}
	default:
		fw.mode = modeAbort
		fw.selfAborted = true
		fw.cancel()
	}
}

// relayRangeLocked starts a partial response if the client asked for one
// range of a response whose length is known. The client then gets its bytes
// as they arrive from the upstream instead of once everything is stored.
func (fw *fetchWriter) relayRangeLocked() bool {
	if fw.status != http.StatusOK {
		return false
	}

	first, last, ok := singleRange(fw.rangeHeader, fw.declared)
	if !ok {
		return false
	}
	fw.first, fw.last = first, last

	fw.mergeHeaderLocked()
	fw.base.Set("Accept-Ranges", "bytes")
	fw.base.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, fw.declared))
	fw.base.Set("Content-Length", strconv.FormatInt(last-first+1, 10))
	fw.base.Add("Cache-Status", fw.x.status(fw.forward+"; stored"))
	fw.rw.WriteHeader(http.StatusPartialContent)

	return true
}

// singleRange parses a Range header asking for one range of a body of the
// given size and returns its first and last byte. Anything else, valid or
// not, is reported as not ok and left to the standard library.
func singleRange(header string, size int64) (first, last int64, ok bool) {
	spec, isBytes := strings.CutPrefix(header, "bytes=")
	if !isBytes || size <= 0 || strings.Contains(spec, ",") {
		return 0, 0, false
	}

	from, to, dash := strings.Cut(spec, "-")
	if !dash {
		return 0, 0, false
	}
	from, to = strings.TrimSpace(from), strings.TrimSpace(to)

	if from == "" {
		// The last n bytes.
		n, err := strconv.ParseInt(to, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}

		return size - min(n, size), size - 1, true
	}

	first, err := strconv.ParseInt(from, 10, 64)
	if err != nil || first < 0 || first >= size {
		return 0, 0, false
	}
	last = size - 1
	if to != "" {
		n, err := strconv.ParseInt(to, 10, 64)
		if err != nil || n < first {
			return 0, 0, false
		}
		last = min(n, last)
	}

	return first, last, true
}

// mergeHeaderLocked gives the client response the headers the upstream
// handlers produced.
func (fw *fetchWriter) mergeHeaderLocked() {
	for name := range fw.base {
		if _, ok := fw.hdr[name]; !ok {
			delete(fw.base, name)
		}
	}
	for name, values := range fw.hdr {
		fw.base[name] = slices.Clone(values)
	}
}

// startPumpLocked starts serving the client from the file the response is
// being written to: count bytes from fw.first, or everything if negative.
func (fw *fetchWriter) startPumpLocked(count int64) {
	tail, err := fw.w.Tail(fw.x.r.Context())
	if err == nil && fw.first > 0 {
		_, err = tail.Seek(fw.first, io.SeekStart)
	}
	if err != nil {
		// What is written cannot be read back: relay without storing.
		if tail != nil {
			tail.Close()
		}
		fw.dropStoreLocked(err)
		return
	}

	// When the client has caught up with the download it is given what it
	// has so far.
	tail.flush = func() { _ = http.NewResponseController(fw.rw).Flush() }

	done := make(chan struct{})
	fw.tail, fw.pump = tail, done

	go func() {
		defer close(done)
		defer tail.Close()

		buf := make([]byte, 64<<10)
		for count != 0 {
			want := int64(len(buf))
			if count > 0 && count < want {
				want = count
			}

			n, rerr := tail.Read(buf[:want])
			if n > 0 {
				if _, werr := fw.rw.Write(buf[:n]); werr != nil {
					fw.pumpErr = werr
					return
				}
				if count > 0 {
					count -= int64(n)
				}
			}
			if rerr != nil {
				if rerr != io.EOF {
					fw.pumpErr = rerr
				}
				return
			}
		}
	}()
}

// Write implements http.ResponseWriter.
func (fw *fetchWriter) Write(p []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.writeHeaderLocked(http.StatusOK)

	switch fw.mode {
	case modePass:
		return fw.rw.Write(p)

	case modeRelay:
		size := len(p)
		start := fw.pos
		fw.pos += int64(size)

		if fw.w != nil {
			_, err := fw.w.Write(p)
			if err == nil {
				if fw.gone {
					fw.armIdleLocked()
				}
				return size, nil
			}

			// The response cannot be stored after all. The client was sent
			// what the file holds; the rest goes to it directly.
			stored := fw.w.n - start
			fw.dropStoreLocked(err)
			p, start = p[stored:], start+stored
		}

		if fw.gone {
			fw.cancel()
			return 0, errClientGone
		}
		if from, to := max(start, fw.first), min(fw.pos, fw.last+1); from < to {
			if _, err := fw.rw.Write(p[from-start : to-start]); err != nil {
				fw.gone = true
				fw.cancel()
				return 0, err
			}
		}
		if fw.pos > fw.last {
			// The client has the range it asked for and nothing is stored:
			// the rest is of no use.
			fw.satisfied = true
			fw.cancel()
			return 0, errClientGone
		}

		return size, nil

	case modeSilent:
		if _, err := fw.w.Write(p); err != nil {
			fw.dropStoreLocked(err)
			fw.mode = modeAbort
			fw.selfAborted = true
			fw.cancel()

			return 0, errFetchAborted
		}
		if fw.gone {
			fw.armIdleLocked()
		}

		return len(p), nil

	case modeAbort:
		return 0, errFetchAborted

	default:
		// The body of the upstream response is not wanted.
		return len(p), nil
	}
}

// Flush implements http.Flusher.
func (fw *fetchWriter) Flush() {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// The pump flushes for itself.
	if fw.pump == nil && (fw.mode == modePass || fw.mode == modeRelay) && !fw.gone {
		_ = http.NewResponseController(fw.rw).Flush()
	}
}

// dropStoreLocked gives up storing the response. If the client was being
// served from the file, it returns once the client has received all that
// the file holds, after which the client can be written to directly.
func (fw *fetchWriter) dropStoreLocked(err error) {
	s := fw.x.store

	if fw.tail != nil {
		// Let the pump finish with what was written before the file goes.
		fw.tail.finishAt(fw.w.n)
	}
	fw.w.Abort()
	fw.w = nil

	if fw.pump != nil {
		<-fw.pump
		fw.pump, fw.tail = nil, nil
		if fw.pumpErr != nil {
			fw.gone = true
		}
	}

	if errors.Is(err, errTooLarge) {
		fw.reason = "TOO-LARGE"
		s.SetUncacheable(fw.id, true)
	} else {
		fw.reason = "STORAGE-ERROR"
		s.warn("storing a response failed", err)
	}
	fw.x.endFlight(false)
}

// clientGone is called when the client request is canceled.
func (fw *fetchWriter) clientGone() {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.gone = true
	if fw.finished {
		return
	}

	switch fw.mode {
	case modePass, modeAbort:
		fw.cancel()
	case modeRelay, modeSilent:
		// The download goes on for the requests to come, provided it is
		// known to end.
		if fw.w == nil || fw.declared < 0 {
			fw.cancel()
			return
		}
		fw.armIdleLocked()
	case modeUndecided:
		// The response may still be worth storing: give the upstream a
		// chance to send it.
		fw.armIdleLocked()
	}
}

// armIdleLocked (re)starts the countdown after which a fetch nobody is
// waiting for is given up if the upstream sends nothing.
func (fw *fetchWriter) armIdleLocked() {
	if fw.idle == nil {
		fw.idle = time.AfterFunc(clientGoneGrace, fw.cancel)
	} else {
		fw.idle.Reset(clientGoneGrace)
	}
}

// finish is called when the upstream handlers returned. ok tells that they
// ran to their end without error, in which case a response they never
// started is the empty 200 the server would send for them.
func (fw *fetchWriter) finish(ok bool) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if ok {
		fw.writeHeaderLocked(http.StatusOK)
	}
	fw.finished = true
	if fw.idle != nil {
		fw.idle.Stop()
	}
}

// copyTrailers gives the client response the trailers the upstream handlers
// set once the body was written. Nothing else may be writing to the client.
func (fw *fetchWriter) copyTrailers() {
	var announced []string
	for _, line := range fw.base.Values("Trailer") {
		for name := range strings.SplitSeq(line, ",") {
			announced = append(announced, http.CanonicalHeaderKey(strings.TrimSpace(name)))
		}
	}

	for name, values := range fw.hdr {
		if strings.HasPrefix(name, http.TrailerPrefix) || slices.Contains(announced, name) {
			fw.base[name] = values
		}
	}
}

// commit makes the stored response available. It reports whether it did.
func (fw *fetchWriter) commit() bool {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.w == nil {
		return false
	}
	w := fw.w
	fw.w = nil

	if fw.declared >= 0 && w.n != fw.declared {
		w.Abort()
		fw.x.h.logger.Warn("incomplete response not cached",
			zap.String("key", fw.x.key), zap.Int64("expected", fw.declared), zap.Int64("received", w.n))
		return false
	}

	// The body is whole: the client gets all of it even if it cannot be
	// kept.
	if fw.tail != nil {
		fw.tail.finishAt(w.n)
	}

	return w.Commit() == nil
}

// close releases what the fetch still holds and waits for the pump, so that
// nothing writes to the client once the handler has returned. It returns
// the reason the client did not get all it was to be sent, if so.
func (fw *fetchWriter) close() error {
	fw.mu.Lock()
	fw.finished = true
	if fw.idle != nil {
		fw.idle.Stop()
	}
	if fw.w != nil {
		fw.w.Abort()
		fw.w = nil
	}
	pump := fw.pump
	fw.mu.Unlock()

	if pump == nil {
		return nil
	}
	<-pump

	return fw.pumpErr
}
