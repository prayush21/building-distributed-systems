package trace

import (
	"strings"
	"testing"
	"unsafe"
)

func TestParseCSVParamsCommaDelimiterQuirk(t *testing.T) {
	// libCacheSim's own example writes a comma delimiter as "delimiter=,",
	// which the outer comma split turns into an empty value.
	in := "time-col=2, obj-id-col=5, obj-size-col=4, obj-id-is-num=true, delimiter=,, has-header=true"
	p, err := ParseCSVParams(in)
	if err != nil {
		t.Fatalf("ParseCSVParams: %v", err)
	}
	if p.TimeCol != 2 || p.ObjIDCol != 5 || p.ObjSizeCol != 4 {
		t.Errorf("columns = %d/%d/%d, want 2/5/4", p.TimeCol, p.ObjIDCol, p.ObjSizeCol)
	}
	if p.Delimiter != ',' {
		t.Errorf("delimiter = %q, want ','", p.Delimiter)
	}
	if !p.ObjIDIsNum {
		t.Error("obj-id-is-num should be true")
	}
	if p.HasHeader == nil || !*p.HasHeader {
		t.Error("has-header should be explicitly true")
	}
}

func TestParseCSVParams(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
		check   func(CSVParams) string
	}{
		{name: "empty uses defaults", in: "", check: func(p CSVParams) string {
			if p.TimeCol != 1 || p.ObjIDCol != 2 || p.ObjSizeCol != 3 || p.Delimiter != ',' {
				return "defaults not time=1,id=2,size=3,delim=,"
			}
			if p.HasHeader != nil {
				return "has-header should default to auto (nil)"
			}
			return ""
		}},
		{name: "tab delimiter", in: "obj-id-col=1,delimiter=tab", check: func(p CSVParams) string {
			if p.Delimiter != '\t' {
				return "delimiter is not tab"
			}
			return ""
		}},
		{name: "escaped tab", in: "obj-id-col=1,delimiter=\\t", check: func(p CSVParams) string {
			if p.Delimiter != '\t' {
				return "delimiter is not tab"
			}
			return ""
		}},
		{name: "no obj-id-col", in: "time-col=1,obj-id-col=0", wantErr: true},
		{name: "unknown key", in: "obj-id-col=1,nonsense=4", wantErr: true},
		{name: "not key=value", in: "obj-id-col", wantErr: true},
		{name: "negative column", in: "obj-id-col=-1", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseCSVParams(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseCSVParams(%q) succeeded, want error", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCSVParams(%q): %v", tc.in, err)
			}
			if tc.check != nil {
				if msg := tc.check(p); msg != "" {
					t.Error(msg)
				}
			}
		})
	}
}

const sample = `time,key,size
100,a,10
101,b,20
102,a,10
103,c,30
`

func TestReadCSVWithHeader(t *testing.T) {
	p := DefaultCSVParams()
	reqs, st, err := ReadCSV(strings.NewReader(sample), p, LoadOptions{})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if !st.HeaderSkipped {
		t.Error("header should have been auto-detected and skipped")
	}
	if len(reqs) != 4 {
		t.Fatalf("got %d requests, want 4", len(reqs))
	}
	want := []Request{
		{Time: 100, Key: "a", Size: 10},
		{Time: 101, Key: "b", Size: 20},
		{Time: 102, Key: "a", Size: 10},
		{Time: 103, Key: "c", Size: 30},
	}
	for i := range want {
		if reqs[i] != want[i] {
			t.Errorf("req[%d] = %+v, want %+v", i, reqs[i], want[i])
		}
	}
}

func TestReadCSVInternsKeys(t *testing.T) {
	reqs, _, err := ReadCSV(strings.NewReader(sample), DefaultCSVParams(), LoadOptions{})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	// Requests 0 and 2 are both "a" and must share one backing string, so a
	// long trace over few objects does not allocate a key per request.
	if unsafe.StringData(reqs[0].Key) != unsafe.StringData(reqs[2].Key) {
		t.Error("repeated key was not interned: keys have distinct backing arrays")
	}
	if unsafe.StringData(reqs[0].Key) == unsafe.StringData(reqs[1].Key) {
		t.Error("distinct keys unexpectedly share a backing array")
	}
}

func TestIgnoreObjSizeForcesSizeOne(t *testing.T) {
	reqs, _, err := ReadCSV(strings.NewReader(sample), DefaultCSVParams(), LoadOptions{IgnoreObjSize: true})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	for i, r := range reqs {
		if r.Size != 1 {
			t.Fatalf("req[%d].Size = %d, want 1 under --ignore-obj-size", i, r.Size)
		}
	}
	// Working set becomes a count of objects, which is what makes an
	// absolute --size a count of objects too.
	st := Summarize(reqs)
	if st.WorkingSetBytes != 3 || st.UniqueObjects != 3 {
		t.Errorf("working set = %d over %d objects, want 3/3", st.WorkingSetBytes, st.UniqueObjects)
	}
}

func TestColumnMapping(t *testing.T) {
	// Deliberately reordered columns, tab separated, no header.
	in := "x\t30\tk1\t900\ny\t40\tk2\t901\n"
	p, err := ParseCSVParams("time-col=4,obj-id-col=3,obj-size-col=2,delimiter=tab")
	if err != nil {
		t.Fatalf("ParseCSVParams: %v", err)
	}
	reqs, st, err := ReadCSV(strings.NewReader(in), p, LoadOptions{})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if st.HeaderSkipped {
		t.Error("no header present, none should have been skipped")
	}
	want := []Request{{Time: 900, Key: "k1", Size: 30}, {Time: 901, Key: "k2", Size: 40}}
	for i := range want {
		if reqs[i] != want[i] {
			t.Errorf("req[%d] = %+v, want %+v", i, reqs[i], want[i])
		}
	}
}

func TestNoTimeColumnUsesIndex(t *testing.T) {
	p, err := ParseCSVParams("time-col=0,obj-id-col=1,obj-size-col=2")
	if err != nil {
		t.Fatalf("ParseCSVParams: %v", err)
	}
	reqs, _, err := ReadCSV(strings.NewReader("a,10\nb,20\na,10\n"), p, LoadOptions{})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	for i, r := range reqs {
		if r.Time != int64(i) {
			t.Errorf("req[%d].Time = %d, want %d", i, r.Time, i)
		}
	}
}

func TestMalformedLinesAreSkippedNotFatal(t *testing.T) {
	in := "100,a,10\ngarbage\n101,b,notanumber\n102,c,30\n"
	p, err := ParseCSVParams("time-col=1,obj-id-col=2,obj-size-col=3")
	if err != nil {
		t.Fatalf("ParseCSVParams: %v", err)
	}
	reqs, st, err := ReadCSV(strings.NewReader(in), p, LoadOptions{})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if len(reqs) != 2 {
		t.Errorf("got %d usable requests, want 2", len(reqs))
	}
	if st.MalformedSkipped != 2 {
		t.Errorf("MalformedSkipped = %d, want 2", st.MalformedSkipped)
	}
}

func TestMaxRequestsTruncates(t *testing.T) {
	reqs, st, err := ReadCSV(strings.NewReader(sample), DefaultCSVParams(), LoadOptions{MaxRequests: 2})
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if len(reqs) != 2 || !st.Truncated {
		t.Errorf("got %d requests truncated=%v, want 2/true", len(reqs), st.Truncated)
	}
}

func TestSummarizeUsesLargestSizePerObject(t *testing.T) {
	reqs := []Request{
		{Key: "a", Size: 10},
		{Key: "b", Size: 5},
		{Key: "a", Size: 40}, // same object, larger
	}
	st := Summarize(reqs)
	if st.Requests != 3 || st.UniqueObjects != 2 {
		t.Errorf("requests/unique = %d/%d, want 3/2", st.Requests, st.UniqueObjects)
	}
	if st.WorkingSetBytes != 45 {
		t.Errorf("WorkingSetBytes = %d, want 45 (40+5)", st.WorkingSetBytes)
	}
}

func TestEmptyTraceIsAnError(t *testing.T) {
	if _, _, err := ReadCSV(strings.NewReader(""), DefaultCSVParams(), LoadOptions{}); err == nil {
		t.Error("empty input should be an error, not a zero-request run")
	}
}
