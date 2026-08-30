package trace

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// CSVParams mirrors libCacheSim's --trace-format-params for CSV traces.
// Column numbers are 1-INDEXED, as they are in libCacheSim; 0 means the column
// is absent.
type CSVParams struct {
	TimeCol    int
	ObjIDCol   int
	ObjSizeCol int
	Delimiter  byte

	// HasHeader nil means auto-detect: if the first line's numeric columns
	// do not parse as numbers, it is treated as a header.
	HasHeader *bool

	// ObjIDIsNum is accepted for libCacheSim command-line compatibility.
	// This harness keys on the raw id text either way, so it affects nothing
	// but is recorded in output for provenance.
	ObjIDIsNum bool
}

// DefaultCSVParams matches the common cacheMon layout: time, obj-id, obj-size.
func DefaultCSVParams() CSVParams {
	return CSVParams{TimeCol: 1, ObjIDCol: 2, ObjSizeCol: 3, Delimiter: ','}
}

// ParseCSVParams parses a libCacheSim-style parameter string, for example
//
//	"time-col=2, obj-id-col=5, obj-size-col=4, obj-id-is-num=true, delimiter=,, has-header=true"
//
// Note the delimiter quirk: because parameters are themselves comma-separated,
// a comma delimiter is written "delimiter=," and arrives here as an empty
// value. An empty delimiter value therefore means comma, which is what
// libCacheSim's own examples rely on.
func ParseCSVParams(s string) (CSVParams, error) {
	p := DefaultCSVParams()
	if strings.TrimSpace(s) == "" {
		return p, nil
	}

	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		key, val, ok := strings.Cut(tok, "=")
		if !ok {
			return p, fmt.Errorf("trace-format-params: %q is not key=value", tok)
		}
		key = strings.TrimSpace(strings.ToLower(key))
		val = strings.TrimSpace(val)

		intVal := func() (int, error) {
			n, err := strconv.Atoi(val)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("trace-format-params: %s=%q must be a non-negative integer", key, val)
			}
			return n, nil
		}
		boolVal := func() (bool, error) {
			switch strings.ToLower(val) {
			case "1", "true", "yes":
				return true, nil
			case "0", "false", "no", "":
				return false, nil
			}
			return false, fmt.Errorf("trace-format-params: %s=%q must be a boolean", key, val)
		}

		var err error
		switch key {
		case "time-col":
			p.TimeCol, err = intVal()
		case "obj-id-col":
			p.ObjIDCol, err = intVal()
		case "obj-size-col":
			p.ObjSizeCol, err = intVal()
		case "obj-id-is-num":
			p.ObjIDIsNum, err = boolVal()
		case "has-header":
			var b bool
			if b, err = boolVal(); err == nil {
				p.HasHeader = &b
			}
		case "delimiter":
			p.Delimiter, err = parseDelimiter(val)
		default:
			err = fmt.Errorf("trace-format-params: unknown key %q", key)
		}
		if err != nil {
			return p, err
		}
	}

	if p.ObjIDCol == 0 {
		return p, fmt.Errorf("trace-format-params: obj-id-col is required")
	}
	return p, nil
}

func parseDelimiter(val string) (byte, error) {
	switch strings.ToLower(val) {
	case "":
		// Consumed by the outer comma split: "delimiter=," lands here.
		return ',', nil
	case "tab", "\\t":
		return '\t', nil
	case "space":
		return ' ', nil
	}
	if len(val) != 1 {
		return 0, fmt.Errorf("trace-format-params: delimiter=%q must be a single character", val)
	}
	return val[0], nil
}

// LoadOptions control normalization applied while reading.
type LoadOptions struct {
	// IgnoreObjSize forces every object to size 1, matching libCacheSim's
	// --ignore-obj-size. Under it a cache size is a count of objects and the
	// byte miss ratio equals the request miss ratio.
	IgnoreObjSize bool

	// MaxRequests truncates the trace after this many records. 0 reads all.
	MaxRequests int64
}

// ReadStats records what the reader saw, for provenance in results.
type ReadStats struct {
	LinesRead        int64 `json:"lines_read"`
	HeaderSkipped    bool  `json:"header_skipped"`
	MalformedSkipped int64 `json:"malformed_skipped"`
	ZeroSizeRecords  int64 `json:"zero_size_records"`
	Truncated        bool  `json:"truncated"`
}

// ReadCSV reads an entire CSV trace into memory.
//
// The whole trace is materialized because every interesting use needs more than
// one pass over it: fractional cache sizes need the working set, Belady needs
// next-access times, and a bench sweep replays one trace under many
// policy/size combinations. Loading once also keeps trace parsing out of the
// timed region, so reported ns/op measures the policy rather than the reader.
func ReadCSV(r io.Reader, p CSVParams, opts LoadOptions) ([]Request, ReadStats, error) {
	var st ReadStats

	if p.ObjIDCol == 0 {
		return nil, st, fmt.Errorf("trace: obj-id-col is required")
	}
	if p.Delimiter == 0 {
		p.Delimiter = ','
	}
	maxCol := p.ObjIDCol
	if p.TimeCol > maxCol {
		maxCol = p.TimeCol
	}
	if p.ObjSizeCol > maxCol {
		maxCol = p.ObjSizeCol
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 256*1024), 4*1024*1024)

	var (
		reqs   []Request
		fields [][]byte
		// intern maps each distinct object id to one shared string, so a
		// trace with millions of requests over thousands of objects does not
		// allocate a string per request.
		intern = make(map[string]string, 4096)
		idx    int64
		first  = true
	)

	for sc.Scan() {
		line := sc.Bytes()
		st.LinesRead++
		if len(line) == 0 {
			continue
		}
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}

		fields = splitFields(fields[:0], line, p.Delimiter)
		if len(fields) < maxCol {
			if first {
				first = false
			}
			st.MalformedSkipped++
			continue
		}

		if first {
			first = false
			if isHeader(fields, p) {
				st.HeaderSkipped = true
				continue
			}
		}

		var req Request

		if p.TimeCol > 0 {
			t, ok := parseInt(fields[p.TimeCol-1])
			if !ok {
				st.MalformedSkipped++
				continue
			}
			req.Time = t
		} else {
			req.Time = idx
		}

		key := fields[p.ObjIDCol-1]
		if len(key) == 0 {
			st.MalformedSkipped++
			continue
		}
		// m[string(b)] on a []byte does not allocate; only new keys do.
		s, ok := intern[string(key)]
		if !ok {
			s = string(key)
			intern[s] = s
		}
		req.Key = s

		switch {
		case opts.IgnoreObjSize, p.ObjSizeCol == 0:
			req.Size = 1
		default:
			sz, ok := parseInt(fields[p.ObjSizeCol-1])
			if !ok {
				st.MalformedSkipped++
				continue
			}
			if sz < 0 {
				sz = 0
			}
			if sz == 0 {
				st.ZeroSizeRecords++
			}
			req.Size = int(sz)
		}

		reqs = append(reqs, req)
		idx++

		if opts.MaxRequests > 0 && idx >= opts.MaxRequests {
			st.Truncated = true
			break
		}
	}
	if err := sc.Err(); err != nil {
		return nil, st, fmt.Errorf("trace: read: %w", err)
	}
	if len(reqs) == 0 {
		return nil, st, fmt.Errorf("trace: no usable records (read %d lines, %d malformed)", st.LinesRead, st.MalformedSkipped)
	}
	return reqs, st, nil
}

// splitFields splits line on delim into dst, reusing dst's backing array. The
// returned slices alias line and are only valid until the next scan.
func splitFields(dst [][]byte, line []byte, delim byte) [][]byte {
	start := 0
	for i := 0; i < len(line); i++ {
		if line[i] == delim {
			dst = append(dst, line[start:i])
			start = i + 1
		}
	}
	return append(dst, line[start:])
}

// isHeader guesses whether the first line is a header by checking that the
// columns which must be numeric actually are. libCacheSim does the same kind of
// sniffing, and getting it wrong silently costs exactly one request, so the
// guess is recorded in ReadStats rather than hidden.
func isHeader(fields [][]byte, p CSVParams) bool {
	if p.HasHeader != nil {
		return *p.HasHeader
	}
	if p.TimeCol > 0 {
		if _, ok := parseInt(fields[p.TimeCol-1]); !ok {
			return true
		}
	}
	if p.ObjSizeCol > 0 {
		if _, ok := parseInt(fields[p.ObjSizeCol-1]); !ok {
			return true
		}
	}
	return false
}

// parseInt accepts integers and, for timestamp columns that carry fractional
// seconds, floats truncated toward zero.
func parseInt(b []byte) (int64, bool) {
	s := string(b)
	if s == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f), true
	}
	return 0, false
}
