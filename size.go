package httpcache

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Size is a number of bytes. In configuration it is either a plain number of
// bytes or a string with a unit: "512Mi", "25GiB", "10g" (binary, as nginx
// does) or "500MB" (decimal).
type Size int64

// SizeOff disables the limit or feature the size configures.
const SizeOff Size = -1

var sizeUnits = map[string]float64{
	"": 1, "b": 1,
	"k": 1 << 10, "ki": 1 << 10, "kib": 1 << 10, "kb": 1e3,
	"m": 1 << 20, "mi": 1 << 20, "mib": 1 << 20, "mb": 1e6,
	"g": 1 << 30, "gi": 1 << 30, "gib": 1 << 30, "gb": 1e9,
	"t": 1 << 40, "ti": 1 << 40, "tib": 1 << 40, "tb": 1e12,
}

// ParseSize parses a byte count such as "8Gi", "25000m", "1.5GB" or "4096".
func ParseSize(s string) (Size, error) {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "off") {
		return SizeOff, nil
	}

	i := 0
	for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}

	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}

	unit, ok := sizeUnits[strings.ToLower(strings.TrimSpace(s[i:]))]
	if !ok {
		return 0, fmt.Errorf("invalid size %q: unknown unit %q", s, strings.TrimSpace(s[i:]))
	}

	v := n * unit
	if v >= math.MaxInt64 {
		return 0, fmt.Errorf("invalid size %q: too large", s)
	}

	return Size(v), nil
}

// String formats the size with the largest binary unit that keeps it exact
// enough to read.
func (s Size) String() string {
	if s < 0 {
		return "off"
	}

	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v := float64(s)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return strconv.FormatInt(int64(s), 10) + "B"
	}

	return strconv.FormatFloat(v, 'f', -1, 64) + units[i]
}

// UnmarshalJSON accepts a number of bytes or a string with a unit.
func (s *Size) UnmarshalJSON(b []byte) error {
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		*s = Size(n)
		return nil
	}

	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return fmt.Errorf("size must be a number of bytes or a string: %s", b)
	}

	v, err := ParseSize(str)
	if err != nil {
		return err
	}
	*s = v

	return nil
}
