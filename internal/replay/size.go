package replay

import (
	"fmt"
	"strconv"
	"strings"
)

// Size is a cache size that may be absolute or expressed as a fraction of the
// trace's working set.
type Size struct {
	// Spec is the original text, kept so results record what was asked for.
	Spec string
	// Bytes is the absolute size, valid when Fraction is 0.
	Bytes int64
	// Fraction, when non-zero, is a multiple of the trace working set.
	Fraction float64
}

// IsFraction reports whether this size must be resolved against a trace.
func (s Size) IsFraction() bool { return s.Fraction != 0 }

var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	// Longest suffixes first so "kib" is not matched as "b".
	{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"tib", 1 << 40},
	{"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30}, {"tb", 1 << 40},
	{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}, {"t", 1 << 40},
	{"b", 1},
}

// ParseSize accepts:
//
//	1gb, 100mb, 512kb, 1024b   absolute, binary units (kb = 1024 bytes)
//	1048576                    absolute, raw bytes
//	0.1x                       fraction of the trace working set
//	0.1                        same, libCacheSim's bare-fraction spelling
//
// Under --ignore-obj-size every object counts as 1 byte, so an absolute size is
// a count of objects and a fraction is a fraction of the unique object count.
func ParseSize(spec string) (Size, error) {
	s := strings.ToLower(strings.TrimSpace(spec))
	if s == "" {
		return Size{}, fmt.Errorf("size: empty")
	}

	// Explicit fraction: "0.1x".
	if strings.HasSuffix(s, "x") {
		f, err := strconv.ParseFloat(strings.TrimSuffix(s, "x"), 64)
		if err != nil || f <= 0 {
			return Size{}, fmt.Errorf("size: %q must be a positive fraction like 0.1x", spec)
		}
		return Size{Spec: spec, Fraction: f}, nil
	}

	// Bare integer: raw byte count.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n <= 0 {
			return Size{}, fmt.Errorf("size: %q must be positive", spec)
		}
		return Size{Spec: spec, Bytes: n}, nil
	}

	// Bare decimal below 1: libCacheSim's fractional working-set spelling.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f > 0 && f < 1 {
			return Size{Spec: spec, Fraction: f}, nil
		}
		return Size{}, fmt.Errorf(
			"size: %q is ambiguous; write %sx for a fraction of the working set, or an integer byte count", spec, s)
	}

	for _, u := range sizeUnits {
		if !strings.HasSuffix(s, u.suffix) {
			continue
		}
		num := strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
		if num == "" {
			continue
		}
		f, err := strconv.ParseFloat(num, 64)
		if err != nil || f <= 0 {
			continue
		}
		b := int64(f * float64(u.mult))
		if b <= 0 {
			return Size{}, fmt.Errorf("size: %q rounds to zero bytes", spec)
		}
		return Size{Spec: spec, Bytes: b}, nil
	}

	return Size{}, fmt.Errorf("size: cannot parse %q (try 1gb, 100mb, 1048576, or 0.1x)", spec)
}

// Resolve turns a size into a byte capacity, using workingSet for fractions.
func (s Size) Resolve(workingSet int64) (int64, error) {
	if !s.IsFraction() {
		return s.Bytes, nil
	}
	if workingSet <= 0 {
		return 0, fmt.Errorf("size: cannot resolve fraction %q against an empty working set", s.Spec)
	}
	b := int64(s.Fraction * float64(workingSet))
	if b <= 0 {
		return 0, fmt.Errorf("size: %q of a %d-byte working set rounds to zero", s.Spec, workingSet)
	}
	return b, nil
}
