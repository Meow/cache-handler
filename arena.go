package httpcache

import (
	"bufio"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// arenaSegmentSize is how much address space the arena reserves at a time.
// Pages are only backed by memory once a block is written to.
const arenaSegmentSize = 16 << 20

// arena hands out fixed-size blocks of memory that live outside the Go heap,
// so the bodies kept in memory neither cost garbage collection time nor
// inflate the heap target. It never has more than limit blocks in use and
// returns the pages of the blocks above the limit to the operating system.
type arena struct {
	mu        sync.Mutex
	blockSize int
	segBlocks int
	segs      [][]byte
	// carved counts the blocks taken from segments at least once.
	carved int
	// free holds unused blocks whose pages are still resident, cold those
	// whose pages were given back.
	free    []uint32
	cold    []uint32
	limit   int
	inUse   atomic.Int64
	retired bool
}

func newArena() *arena {
	return newArenaFor(os.Getpagesize())
}

// newArenaFor returns an arena for a system whose pages have the given size.
func newArenaFor(pageSize int) *arena {
	bs := 4096
	// Blocks are released page by page, so they must be page aligned.
	if pageSize > bs {
		bs = pageSize
	}

	return &arena{blockSize: bs, segBlocks: arenaSegmentSize / bs}
}

// blocksFor returns how many blocks a body of size bytes takes.
func (a *arena) blocksFor(size int64) int {
	return int((size + int64(a.blockSize) - 1) / int64(a.blockSize))
}

// alloc returns a blob able to hold size bytes, or nil when that would exceed
// the limit or memory cannot be obtained.
func (a *arena) alloc(size int64) *blob {
	b := a.newBlob()
	b.size = size
	if !a.grow(b, a.blocksFor(size)) {
		return nil
	}

	return b
}

// newBlob returns a blob that holds nothing yet, to be grown as its body
// arrives.
func (a *arena) newBlob() *blob {
	b := &blob{a: a, blockSize: a.blockSize}
	b.refs.Store(1)

	return b
}

// grow gives b n more blocks and reports whether it could: not when that
// would exceed the limit or memory cannot be obtained. The caller ensures
// nobody reads the block list of b meanwhile.
func (a *arena) grow(b *blob, n int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.retired || int(a.inUse.Load())+n > a.limit {
		return false
	}

	for len(a.free)+len(a.cold)+len(a.segs)*a.segBlocks-a.carved < n {
		seg, err := mapSegment(a.segBlocks * a.blockSize)
		if err != nil {
			return false
		}
		a.segs = append(a.segs, seg)
	}

	for range n {
		var id uint32
		switch {
		case len(a.free) > 0:
			id = a.free[len(a.free)-1]
			a.free = a.free[:len(a.free)-1]
		case len(a.cold) > 0:
			id = a.cold[len(a.cold)-1]
			a.cold = a.cold[:len(a.cold)-1]
		default:
			id = uint32(a.carved)
			a.carved++
		}
		b.ids = append(b.ids, id)
		b.blocks = append(b.blocks, a.block(id))
	}
	a.inUse.Add(int64(n))

	return true
}

func (a *arena) block(id uint32) []byte {
	seg := a.segs[int(id)/a.segBlocks]
	off := (int(id) % a.segBlocks) * a.blockSize

	return seg[off : off+a.blockSize : off+a.blockSize]
}

func (a *arena) put(b *blob) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.free = append(a.free, b.ids...)
	a.inUse.Add(-int64(len(b.ids)))
	b.ids, b.blocks = nil, nil

	if a.retired && a.inUse.Load() == 0 {
		a.unmapLocked()
	}
}

// setLimit changes how many blocks may be in use. Blocks already handed out
// stay valid; the caller is expected to release blobs until inUse fits.
func (a *arena) setLimit(blocks int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.limit = blocks
	// Enough for the limit drifting as the index grows. trim catches up
	// with a limit lowered by a lot.
	a.trimLocked(64)
}

// trimLocked gives the pages of free blocks back to the operating system
// until no more memory is resident than the limit allows, budget blocks at
// most. It reports whether there is more to give back.
func (a *arena) trimLocked(budget int) bool {
	target := max(a.limit, int(a.inUse.Load()))
	for a.carved-len(a.cold) > target && len(a.free) > 0 {
		if budget == 0 {
			return true
		}
		budget--

		id := a.free[len(a.free)-1]
		a.free = a.free[:len(a.free)-1]
		releasePages(a.block(id))
		a.cold = append(a.cold, id)
	}

	return false
}

// excess tells whether memory is left to give back to the system.
func (a *arena) excess() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.carved-len(a.cold) > max(a.limit, int(a.inUse.Load())) && len(a.free) > 0
}

// trim gives back all the memory the limit no longer allows, in steps short
// enough not to hold up the allocations meanwhile.
func (a *arena) trim() {
	for {
		a.mu.Lock()
		more := a.trimLocked(1024)
		a.mu.Unlock()

		if !more {
			return
		}
	}
}

// retire makes the arena refuse new allocations and unmaps its memory once
// the last blob is released.
func (a *arena) retire() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.retired = true
	if a.inUse.Load() == 0 {
		a.unmapLocked()
	}
}

func (a *arena) unmapLocked() {
	for _, seg := range a.segs {
		unmapSegment(seg)
	}
	a.segs, a.free, a.cold, a.carved = nil, nil, nil, 0
}

// residentBytes reports how much memory the arena holds from the system.
func (a *arena) residentBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	return int64(a.carved-len(a.cold)) * int64(a.blockSize)
}

// blob is a body held in arena blocks. It is reference counted: the index
// owns one reference while the blob is attached to an entry, the Writer
// receiving the body owns one until it is done, and every reader owns one
// while it serves from it. References are only taken under the store lock
// from an attached blob, or under the lock of the Writer from the blob it
// holds, so the count can never climb back from zero.
type blob struct {
	a         *arena
	ids       []uint32
	blocks    [][]byte
	size      int64
	blockSize int
	refs      atomic.Int32
}

func (b *blob) acquire() {
	b.refs.Add(1)
}

func (b *blob) release() {
	if b.refs.Add(-1) == 0 {
		b.a.put(b)
	}
}

// fill reads the whole body from r, which holds it at offset off.
func (b *blob) fill(r io.ReaderAt, off int64) error {
	for i, blk := range b.blocks {
		start := int64(i) * int64(b.blockSize)
		n := min(int64(b.blockSize), b.size-start)
		if _, err := r.ReadAt(blk[:n], off+start); err != nil {
			return err
		}
	}

	return nil
}

// write copies p into the blob at offset off, which its blocks must cover.
func (b *blob) write(p []byte, off int64) {
	bs := int64(b.blockSize)
	for len(p) > 0 {
		n := copy(b.blocks[off/bs][off%bs:], p)
		p = p[n:]
		off += int64(n)
	}
}

// readBlocks copies to p what the blocks, of bs bytes each, hold from offset
// off to end. It returns the number of bytes copied.
func readBlocks(p []byte, blocks [][]byte, bs, off, end int64) int {
	n := 0
	for n < len(p) && off < end {
		i := off / bs
		c := copy(p[n:], blocks[i][off%bs:min(bs, end-i*bs)])
		n += c
		off += int64(c)
	}

	return n
}

// writeTo writes the first size bytes of the blob to w, a few blocks at a
// time. The caller ensures the blob is not grown meanwhile.
func (b *blob) writeTo(w io.Writer, size int64) error {
	bw := bufio.NewWriterSize(w, 64<<10)

	bs := int64(b.blockSize)
	for i := int64(0); i*bs < size; i++ {
		if _, err := bw.Write(b.blocks[i][:min(bs, size-i*bs)]); err != nil {
			return err
		}
	}

	return bw.Flush()
}

// copyBufSize is how much of a body held in memory is given to a client in
// one Write. Caddy does not let a Write to a client exceed 64KiB.
const copyBufSize = 64 << 10

var copyBufs = sync.Pool{New: func() any {
	buf := make([]byte, copyBufSize)

	return &buf
}}

// copyBuffered copies r to w through a buffer of copyBufs, one Write for
// each Read. It is for bodies that are not files, which a ResponseWriter's
// ReadFrom handles poorly: it tries sendfile, and failing that sends the
// first bytes by themselves and the rest through a buffer it allocates for
// every response.
func copyBuffered(w io.Writer, r io.Reader) (int64, error) {
	bufp := copyBufs.Get().(*[]byte)
	defer copyBufs.Put(bufp)

	var written int64
	for {
		n, err := r.Read(*bufp)
		if n > 0 {
			m, werr := w.Write((*bufp)[:n])
			written += int64(m)
			if werr != nil {
				return written, werr
			}
		}
		if err == io.EOF {
			return written, nil
		}
		if err != nil {
			return written, err
		}
	}
}

// blobReader reads a blob as a stream. The caller must hold a reference on
// the blob for as long as it uses the reader.
type blobReader struct {
	b   *blob
	off int64
}

func (r *blobReader) Read(p []byte) (int, error) {
	if r.off >= r.b.size {
		return 0, io.EOF
	}

	n := readBlocks(p, r.b.blocks, int64(r.b.blockSize), r.off, r.b.size)
	r.off += int64(n)

	return n, nil
}

// WriteTo writes what is left of the blob to w. It is what io.Copy uses, in
// preference to the ReadFrom of w. A Read gathers as many blocks as the
// buffer holds, so that most bodies reach the connection in one Write.
func (r *blobReader) WriteTo(w io.Writer) (int64, error) {
	return copyBuffered(w, r)
}

func (r *blobReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.b.size
	}
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	r.off = offset

	return offset, nil
}
