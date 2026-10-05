package httpcache

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// clientGoneGrace is how long a fetch whose client left may go without
// receiving anything from the upstream before it is given up.
const clientGoneGrace = 30 * time.Second

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
	// modeStream: relayed to the client and stored at the same time.
	modeStream
	// modeSilent: stored only. The client asked for a range or sent a
	// precondition, which is answered from the stored response afterwards.
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
	// revalidating tells that the request carries the validators of stale.
	revalidating bool
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
	gone     bool
	finished bool
	idle     *time.Timer
}

func newFetchWriter(x *exchange, id ID, stale *Hit) *fetchWriter {
	fw := &fetchWriter{
		x:        x,
		id:       id,
		rw:       x.w,
		base:     x.w.Header(),
		hdr:      x.w.Header().Clone(),
		stale:    stale,
		plain:    x.r.Method == http.MethodGet && x.r.Header.Get("Range") == "" && !conditional(x.r),
		forward:  "fwd=uri-miss",
		reason:   "UPSTREAM-ERROR",
		declared: -1,
	}
	if stale != nil {
		fw.forward = "fwd=stale"
	}

	return fw
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
	s := x.h.store
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

	// The size the response announces is known not to fit before any of it
	// is written.
	limit := s.maxObject()
	if c.maxBody > 0 && c.maxBody < limit {
		limit = c.maxBody
	}
	if n, err := strconv.ParseInt(fw.hdr.Get("Content-Length"), 10, 64); err == nil && n >= 0 {
		fw.declared = n
		if v.store && n > limit {
			v = reject("TOO-LARGE")
		}
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

		w, err := s.Create(x.key, v.vary, x.r.Header, rec, limit)
		if err != nil {
			s.warn("storing a response failed", err)
			v = reject("STORAGE-ERROR")
		} else {
			fw.w = w
		}
	}

	if !v.store {
		fw.reason = v.reason
		if v.reason != "STORAGE-ERROR" {
			s.SetUncacheable(fw.id, true)
		}
		// The upstream no longer gives a response to keep, so the one it
		// gave before is not to be served anymore either.
		if fw.stale != nil && code < 500 {
			fw.stale.Discard()
		}
		// Nothing will come of this fetch for the requests waiting on it.
		x.endFlight(false)
	}

	switch {
	case v.store && fw.plain:
		fw.mode = modeStream
		fw.sendHeaderLocked(fw.forward + "; stored")
	case v.store:
		fw.mode = modeSilent
	case fw.plain:
		fw.mode = modePass
		fw.sendHeaderLocked(fw.forward + "; detail=" + v.reason)
		if fw.gone {
			fw.cancel()
		}
	default:
		fw.mode = modeAbort
		fw.cancel()
	}
}

// sendHeaderLocked gives the client response the headers the upstream
// handlers produced and starts the response.
func (fw *fetchWriter) sendHeaderLocked(params string) {
	for name := range fw.base {
		if _, ok := fw.hdr[name]; !ok {
			delete(fw.base, name)
		}
	}
	for name, values := range fw.hdr {
		fw.base[name] = values
	}
	fw.base.Add("Cache-Status", fw.x.status(params))

	fw.rw.WriteHeader(fw.status)
}

// Write implements http.ResponseWriter.
func (fw *fetchWriter) Write(p []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.writeHeaderLocked(http.StatusOK)

	switch fw.mode {
	case modePass:
		return fw.rw.Write(p)

	case modeStream:
		if fw.w != nil {
			if _, err := fw.w.Write(p); err != nil {
				fw.dropStoreLocked(err)
			}
		}
		if !fw.gone {
			if _, err := fw.rw.Write(p); err != nil {
				fw.gone = true
			}
		}
		if fw.gone {
			// Only the cache is still interested in the response.
			if fw.w == nil {
				fw.cancel()
				return 0, errClientGone
			}
			fw.armIdleLocked()
		}

		return len(p), nil

	case modeSilent:
		if _, err := fw.w.Write(p); err != nil {
			fw.dropStoreLocked(err)
			fw.mode = modeAbort
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

	if (fw.mode == modePass || fw.mode == modeStream) && !fw.gone {
		_ = http.NewResponseController(fw.rw).Flush()
	}
}

// dropStoreLocked gives up storing the response.
func (fw *fetchWriter) dropStoreLocked(err error) {
	s := fw.x.h.store

	fw.w.Abort()
	fw.w = nil

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
	case modeStream, modeSilent:
		if fw.w == nil {
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
// did so without error, in which case a response they never started is the
// empty 200 the server would send for them.
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

	return w.Commit() == nil
}

// close releases what the fetch still holds.
func (fw *fetchWriter) close() {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	fw.finished = true
	if fw.idle != nil {
		fw.idle.Stop()
	}
	if fw.w != nil {
		fw.w.Abort()
		fw.w = nil
	}
}
