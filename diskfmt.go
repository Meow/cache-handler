package httpcache

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"net/http"
)

// A cache file is self-describing and never modified once it is in place:
//
//	[64-byte fixed header][key][meta][body]
//
// The fixed header is little endian:
//
//	 0  magic "CCH1"
//	 4  flags       uint32
//	 8  stored      int64   unix milliseconds the response was received or last validated
//	16  fresh       int64   unix milliseconds until which the response is fresh
//	24  swr         uint32  seconds it may be served stale while it is being updated
//	28  sie         uint32  seconds it may be served stale when the upstream fails
//	32  age         uint32  Age of the response when it was received
//	36  keyLen      uint32
//	40  metaLen     uint32
//	44  reserved
//	48  bodyLen     int64
//	56  crc         uint32  CRC-32C of header[0:56] + key + meta
//	60  reserved
//
// meta holds the status code, the vary specification and the response headers.
const (
	fileMagic  = "CCH1"
	headerSize = 64

	maxKeyLen  = 16 << 10
	maxMetaLen = 1 << 20

	// flagMarker marks a file that holds no response, only the list of
	// request headers the responses of its key vary on.
	flagMarker uint32 = 1 << 0
	// flagMustRevalidate forbids serving the response once it is stale.
	flagMustRevalidate uint32 = 1 << 1
)

var (
	errCorrupt = errors.New("corrupt cache file")
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
)

// ID names a cache file. It is derived from the full key of the entry.
type ID [16]byte

func makeID(key string) ID {
	sum := sha256.Sum256([]byte(key))

	var id ID
	copy(id[:], sum[:])

	return id
}

func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

func parseID(name string) (ID, bool) {
	var id ID
	if len(name) != 2*len(id) {
		return id, false
	}
	if _, err := hex.Decode(id[:], []byte(name)); err != nil {
		return id, false
	}

	return id, true
}

// record is everything a cache file says about its response except the body.
// It is immutable once built.
type record struct {
	key    string
	flags  uint32
	stored int64
	fresh  int64
	swr    uint32
	sie    uint32
	age    uint32
	status int
	// vary is only set on markers: a salt, a NUL, then the comma separated
	// lowercase names of the request headers the key varies on.
	vary string
	// header holds the response headers to set when the response is served.
	// A name with no value is a header to remove.
	header  http.Header
	bodyLen int64
	bodyOff int64
}

func (r *record) marker() bool {
	return r.flags&flagMarker != 0
}

// memCost estimates the heap a record keeps alive.
func (r *record) memCost() int64 {
	n := int64(160 + len(r.key) + len(r.vary))
	for k, vv := range r.header {
		n += int64(len(k)) + 64
		for _, v := range vv {
			n += int64(len(v)) + 16
		}
	}

	return n
}

// encodeHead returns the fixed header, key and meta of r. The body length and
// checksum are filled in later by finalizeHead, once the body is known.
func encodeHead(r *record) ([]byte, error) {
	if len(r.key) == 0 || len(r.key) > maxKeyLen {
		return nil, errors.New("cache key too long")
	}

	meta := make([]byte, 0, 512)
	meta = binary.AppendUvarint(meta, uint64(r.status))
	meta = appendString(meta, r.vary)
	meta = binary.AppendUvarint(meta, uint64(len(r.header)))
	for k, vv := range r.header {
		meta = appendString(meta, k)
		meta = binary.AppendUvarint(meta, uint64(len(vv)))
		for _, v := range vv {
			meta = appendString(meta, v)
		}
	}
	if len(meta) > maxMetaLen {
		return nil, errors.New("response headers too large")
	}

	buf := make([]byte, headerSize, headerSize+len(r.key)+len(meta))
	copy(buf, fileMagic)
	binary.LittleEndian.PutUint32(buf[4:], r.flags)
	binary.LittleEndian.PutUint64(buf[8:], uint64(r.stored))
	binary.LittleEndian.PutUint64(buf[16:], uint64(r.fresh))
	binary.LittleEndian.PutUint32(buf[24:], r.swr)
	binary.LittleEndian.PutUint32(buf[28:], r.sie)
	binary.LittleEndian.PutUint32(buf[32:], r.age)
	binary.LittleEndian.PutUint32(buf[36:], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(buf[40:], uint32(len(meta)))
	buf = append(buf, r.key...)
	buf = append(buf, meta...)

	return buf, nil
}

// finalizeHead records the body length in a head built by encodeHead and
// seals it with its checksum.
func finalizeHead(head []byte, bodyLen int64) {
	binary.LittleEndian.PutUint64(head[48:], uint64(bodyLen))
	binary.LittleEndian.PutUint32(head[56:], headChecksum(head))
}

func headChecksum(head []byte) uint32 {
	crc := crc32.Update(0, crcTable, head[:56])

	return crc32.Update(crc, crcTable, head[headerSize:])
}

// readRecord reads and validates the head of a cache file of the given size.
func readRecord(f io.ReaderAt, size int64) (*record, error) {
	if size < headerSize {
		return nil, errCorrupt
	}

	// One read covers the head of nearly every file.
	buf := make([]byte, min(size, 4096))
	if _, err := f.ReadAt(buf, 0); err != nil {
		return nil, err
	}
	if string(buf[:4]) != fileMagic {
		return nil, errCorrupt
	}

	keyLen := int64(binary.LittleEndian.Uint32(buf[36:]))
	metaLen := int64(binary.LittleEndian.Uint32(buf[40:]))
	bodyLen := int64(binary.LittleEndian.Uint64(buf[48:]))
	headLen := headerSize + keyLen + metaLen
	if keyLen == 0 || keyLen > maxKeyLen || metaLen > maxMetaLen || bodyLen < 0 || headLen+bodyLen != size {
		return nil, errCorrupt
	}

	if int64(len(buf)) < headLen {
		full := make([]byte, headLen)
		n := copy(full, buf)
		if _, err := f.ReadAt(full[n:], int64(n)); err != nil {
			return nil, err
		}
		buf = full
	}
	buf = buf[:headLen]

	if binary.LittleEndian.Uint32(buf[56:]) != headChecksum(buf) {
		return nil, errCorrupt
	}

	r := &record{
		key:     string(buf[headerSize : headerSize+keyLen]),
		flags:   binary.LittleEndian.Uint32(buf[4:]),
		stored:  int64(binary.LittleEndian.Uint64(buf[8:])),
		fresh:   int64(binary.LittleEndian.Uint64(buf[16:])),
		swr:     binary.LittleEndian.Uint32(buf[24:]),
		sie:     binary.LittleEndian.Uint32(buf[28:]),
		age:     binary.LittleEndian.Uint32(buf[32:]),
		bodyLen: bodyLen,
		bodyOff: headLen,
	}

	d := decoder{buf: buf[headerSize+keyLen:]}
	r.status = int(d.uvarint())
	r.vary = d.string()
	if n := d.uvarint(); n > 0 && !d.failed {
		r.header = make(http.Header, min(n, 64))
		for ; n > 0 && !d.failed; n-- {
			name := d.string()
			values := make([]string, 0, 1)
			for m := d.uvarint(); m > 0 && !d.failed; m-- {
				values = append(values, d.string())
			}
			r.header[name] = values
		}
	}
	if d.failed || len(d.buf) != 0 {
		return nil, errCorrupt
	}

	return r, nil
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))

	return append(b, s...)
}

// decoder reads the meta block. Any malformed input sets failed and makes
// every further read return a zero value.
type decoder struct {
	buf    []byte
	failed bool
}

func (d *decoder) uvarint() uint64 {
	if d.failed {
		return 0
	}

	v, n := binary.Uvarint(d.buf)
	if n <= 0 {
		d.failed = true
		return 0
	}
	d.buf = d.buf[n:]

	return v
}

func (d *decoder) string() string {
	n := d.uvarint()
	if d.failed || n > uint64(len(d.buf)) {
		d.failed = true
		return ""
	}

	s := string(d.buf[:n])
	d.buf = d.buf[n:]

	return s
}
