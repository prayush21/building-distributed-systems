package bench

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prayush21/building-distributed-systems/internal/replay"
	"github.com/prayush21/building-distributed-systems/internal/trace"
)

func TestParseSpecs(t *testing.T) {
	specs, err := ParseSpecs("fifo, lru ,s3fifo:small-size-ratio=0.2")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	want := []Spec{
		{Name: "fifo", Label: "fifo"},
		{Name: "lru", Label: "lru"},
		{Name: "s3fifo", Params: "small-size-ratio=0.2", Label: "s3fifo(small-size-ratio=0.2)"},
	}
	if len(specs) != len(want) {
		t.Fatalf("got %d specs, want %d", len(specs), len(want))
	}
	for i := range want {
		if specs[i] != want[i] {
			t.Errorf("spec[%d] = %+v, want %+v", i, specs[i], want[i])
		}
	}
}

// TestParseSpecsAllowsParameterSweep: the same policy at two settings is the
// point of the syntax, and the labels must distinguish them.
func TestParseSpecsAllowsParameterSweep(t *testing.T) {
	specs, err := ParseSpecs("s3fifo,s3fifo:small-size-ratio=0.2,s3fifo:small-size-ratio=0.3")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	seen := map[string]bool{}
	for _, s := range specs {
		if seen[s.Label] {
			t.Fatalf("duplicate label %q", s.Label)
		}
		seen[s.Label] = true
	}
	if len(specs) != 3 {
		t.Errorf("got %d specs, want 3", len(specs))
	}
}

func TestParseSpecsRejects(t *testing.T) {
	for _, in := range []string{"", "   ", "lru,lru", ",,"} {
		if _, err := ParseSpecs(in); err == nil {
			t.Errorf("ParseSpecs(%q) should fail", in)
		}
	}
}

func approx(got *float64, want float64) bool {
	return got != nil && math.Abs(*got-want) < 1e-9
}

func show(v *float64) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%v", *v)
}

// TestDeriveGroup pins both derived metrics, including the cases that are easy
// to get quietly wrong: a policy worse than LRU, and one better than Belady.
func TestDeriveGroup(t *testing.T) {
	group := []Cell{
		{Policy: "fifo", RequestMissRatio: 0.50},
		{Policy: "lru", RequestMissRatio: 0.40},
		{Policy: "belady", RequestMissRatio: 0.20},
		{Policy: "sieve", RequestMissRatio: 0.35},
		{Policy: "worse-than-lru", RequestMissRatio: 0.45},
		{Policy: "better-than-opt", RequestMissRatio: 0.10},
	}
	deriveGroup(group)

	tests := []struct {
		policy        string
		wantReduction float64
		wantGap       float64
	}{
		// FIFO is the denominator, so its own reduction is zero. Its gap is
		// negative: FIFO is worse than LRU.
		{"fifo", 0.0, -0.5},
		// LRU is the gap's origin.
		{"lru", 0.2, 0.0},
		// Belady defines the far end of the gap.
		{"belady", 0.6, 1.0},
		{"sieve", 0.3, 0.25},
		{"worse-than-lru", 0.1, -0.25},
		// Above 1.0 is legitimate: Belady is only optimal for uniform sizes.
		{"better-than-opt", 0.8, 1.5},
	}
	for _, tc := range tests {
		var c *Cell
		for i := range group {
			if group[i].Policy == tc.policy {
				c = &group[i]
			}
		}
		if c == nil {
			t.Fatalf("no cell for %q", tc.policy)
		}
		if !approx(c.MRReductionVsFIFO, tc.wantReduction) {
			t.Errorf("%s reduction = %s, want %v", tc.policy, show(c.MRReductionVsFIFO), tc.wantReduction)
		}
		if !approx(c.GapClosedVsBelady, tc.wantGap) {
			t.Errorf("%s gap closed = %s, want %v", tc.policy, show(c.GapClosedVsBelady), tc.wantGap)
		}
	}
}

// TestDeriveGroupUndefinedCases: an undefined ratio must stay nil rather than
// become zero, because zero means "as good as the baseline" and would be read
// as a result.
func TestDeriveGroupUndefinedCases(t *testing.T) {
	t.Run("no belady means no gap column", func(t *testing.T) {
		group := []Cell{
			{Policy: "fifo", RequestMissRatio: 0.5},
			{Policy: "lru", RequestMissRatio: 0.4},
			{Policy: "sieve", RequestMissRatio: 0.3},
		}
		deriveGroup(group)
		for _, c := range group {
			if c.GapClosedVsBelady != nil {
				t.Errorf("%s: gap closed = %s, want nil", c.Policy, show(c.GapClosedVsBelady))
			}
			if c.MRReductionVsFIFO == nil {
				t.Errorf("%s: reduction should still be defined", c.Policy)
			}
		}
	})

	t.Run("no fifo means no reduction column", func(t *testing.T) {
		group := []Cell{
			{Policy: "lru", RequestMissRatio: 0.4},
			{Policy: "belady", RequestMissRatio: 0.2},
		}
		deriveGroup(group)
		for _, c := range group {
			if c.MRReductionVsFIFO != nil {
				t.Errorf("%s: reduction = %s, want nil", c.Policy, show(c.MRReductionVsFIFO))
			}
		}
	})

	t.Run("gap too small to divide by", func(t *testing.T) {
		// LRU is already essentially optimal here, so "share of the gap
		// closed" would be a huge number derived from noise.
		group := []Cell{
			{Policy: "fifo", RequestMissRatio: 0.5},
			{Policy: "lru", RequestMissRatio: 0.4},
			{Policy: "belady", RequestMissRatio: 0.4 - minGapForRatio/2},
			{Policy: "sieve", RequestMissRatio: 0.39},
		}
		deriveGroup(group)
		for _, c := range group {
			if c.GapClosedVsBelady != nil {
				t.Errorf("%s: gap closed = %s, want nil for a negligible gap",
					c.Policy, show(c.GapClosedVsBelady))
			}
		}
	})

	t.Run("failed cells get no derived values", func(t *testing.T) {
		group := []Cell{
			{Policy: "fifo", RequestMissRatio: 0.5},
			{Policy: "lru", RequestMissRatio: 0.4},
			{Policy: "belady", RequestMissRatio: 0.2},
			{Policy: "s3fifo", Error: "cache too small to split"},
		}
		deriveGroup(group)
		last := group[3]
		if last.MRReductionVsFIFO != nil || last.GapClosedVsBelady != nil {
			t.Error("a failed cell must not carry derived values")
		}
	})

	t.Run("a failed baseline disables its column", func(t *testing.T) {
		group := []Cell{
			{Policy: "fifo", Error: "boom"},
			{Policy: "lru", RequestMissRatio: 0.4},
			{Policy: "belady", RequestMissRatio: 0.2},
		}
		deriveGroup(group)
		for _, c := range group {
			if c.MRReductionVsFIFO != nil {
				t.Errorf("%s: reduction should be nil when FIFO failed", c.Policy)
			}
		}
	})
}

// TestBaselineIgnoresParameterizedVariants: "s3fifo(...)" is a policy under
// test, never a reference point.
func TestBaselineIgnoresParameterizedVariants(t *testing.T) {
	group := []Cell{
		{Policy: "lru(x=1)", RequestMissRatio: 0.9},
		{Policy: "lru", RequestMissRatio: 0.4},
	}
	got, ok := baseline(group, "lru")
	if !ok || got != 0.4 {
		t.Errorf("baseline(lru) = %v, %v; want 0.4, true", got, ok)
	}
}

func TestMedian(t *testing.T) {
	if v := medianPtr([]float64{3, 1, 2}); !approx(v, 2) {
		t.Errorf("odd median = %s, want 2", show(v))
	}
	if v := medianPtr([]float64{4, 1, 3, 2}); !approx(v, 2.5) {
		t.Errorf("even median = %s, want 2.5", show(v))
	}
	if medianPtr(nil) != nil {
		t.Error("median of nothing should be nil")
	}
}

// --- end to end ---

func writeTrace(t *testing.T, dir, name string, requests, objects int, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	var b strings.Builder
	b.WriteString("timestamp,obj_id,obj_size\n")
	for i := 0; i < requests; i++ {
		u := rng.Float64()
		id := int(u * u * float64(objects))
		fmt.Fprintf(&b, "%d,k%d,%d\n", 1000+i, id, 64+(id*37)%2048)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write trace: %v", err)
	}
}

func testSweep(t *testing.T) (*Report, Options) {
	t.Helper()
	dir := t.TempDir()
	writeTrace(t, dir, "a.csv", 8000, 600, 1)
	writeTrace(t, dir, "b.csv", 8000, 400, 2)

	paths, err := FindTraces(dir)
	if err != nil {
		t.Fatalf("FindTraces: %v", err)
	}
	specs, err := ParseSpecs("fifo,lru,lfu,sieve,belady")
	if err != nil {
		t.Fatalf("ParseSpecs: %v", err)
	}
	var sizes []replay.Size
	for _, s := range []string{"0.05x", "0.2x"} {
		size, err := replay.ParseSize(s)
		if err != nil {
			t.Fatalf("ParseSize: %v", err)
		}
		sizes = append(sizes, size)
	}
	opts := Options{
		TracePaths: paths, Specs: specs, Sizes: sizes,
		Seed: 1, Oracle: true, CSVParams: trace.DefaultCSVParams(),
	}
	rep, err := Run(opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return rep, opts
}

func TestSweepShape(t *testing.T) {
	rep, _ := testSweep(t)

	if got, want := len(rep.Results), 2*2*5; got != want {
		t.Errorf("got %d cells, want %d", got, want)
	}
	if got := len(rep.Traces); got != 2 {
		t.Errorf("got %d traces, want 2", got)
	}
	if got, want := len(rep.Summary), 5*2; got != want {
		t.Errorf("got %d summary rows, want %d", got, want)
	}
	for _, c := range rep.Results {
		if c.Error != "" {
			t.Errorf("unexpected error in %s/%s/%s: %s", c.Trace, c.SizeSpec, c.Policy, c.Error)
		}
	}
	if inv := rep.Invalid(); len(inv) != 0 {
		t.Errorf("got %d invalid cells, want 0", len(inv))
	}
}

// TestSweepBaselineIdentities are the invariants a reader will assume when
// scanning the table, so they are worth asserting rather than trusting.
func TestSweepBaselineIdentities(t *testing.T) {
	rep, _ := testSweep(t)
	for _, c := range rep.Results {
		switch c.Policy {
		case "fifo":
			if !approx(c.MRReductionVsFIFO, 0) {
				t.Errorf("fifo reduction = %s, want 0", show(c.MRReductionVsFIFO))
			}
		case "lru":
			if !approx(c.GapClosedVsBelady, 0) {
				t.Errorf("lru gap closed = %s, want 0", show(c.GapClosedVsBelady))
			}
		case "belady":
			if !approx(c.GapClosedVsBelady, 1) {
				t.Errorf("belady gap closed = %s, want 1", show(c.GapClosedVsBelady))
			}
		}
	}
}

// TestOracleDisabledDropsGapColumn: without Belady the gap cannot be computed,
// and must be reported as undefined rather than silently omitted or zeroed.
func TestOracleDisabledDropsGapColumn(t *testing.T) {
	dir := t.TempDir()
	writeTrace(t, dir, "a.csv", 4000, 300, 7)
	paths, _ := FindTraces(dir)
	specs, _ := ParseSpecs("fifo,lru,belady")
	size, _ := replay.ParseSize("0.1x")

	rep, err := Run(Options{
		TracePaths: paths, Specs: specs, Sizes: []replay.Size{size},
		Seed: 1, Oracle: false, CSVParams: trace.DefaultCSVParams(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, c := range rep.Results {
		if c.Policy == "belady" {
			if c.Error == "" {
				t.Error("belady should be skipped when --oracle is off")
			}
			continue
		}
		if c.GapClosedVsBelady != nil {
			t.Errorf("%s: gap closed = %s, want nil without a Belady baseline",
				c.Policy, show(c.GapClosedVsBelady))
		}
	}
}

// TestUnrunnableCellDoesNotAbortSweep: S3-FIFO cannot split a tiny cache, and
// that must degrade to one empty cell rather than losing the whole run.
func TestUnrunnableCellDoesNotAbortSweep(t *testing.T) {
	dir := t.TempDir()
	writeTrace(t, dir, "a.csv", 4000, 300, 3)
	paths, _ := FindTraces(dir)
	specs, _ := ParseSpecs("lru,s3fifo")
	size, _ := replay.ParseSize("4") // 4 bytes: far too small for S3-FIFO's queues

	rep, err := Run(Options{
		TracePaths: paths, Specs: specs, Sizes: []replay.Size{size},
		Seed: 1, Oracle: false, CSVParams: trace.DefaultCSVParams(),
	})
	if err != nil {
		t.Fatalf("Run should not fail because one cell could not be configured: %v", err)
	}
	var s3, lru *Cell
	for i := range rep.Results {
		switch rep.Results[i].Policy {
		case "s3fifo":
			s3 = &rep.Results[i]
		case "lru":
			lru = &rep.Results[i]
		}
	}
	if s3 == nil || s3.Error == "" {
		t.Error("s3fifo should record why it could not run")
	}
	if s3 != nil && s3.Invalid {
		t.Error("an unconfigurable cell is not an invalid result")
	}
	if lru == nil || lru.Error != "" {
		t.Error("lru should still have run")
	}
	for _, row := range rep.Summary {
		if row.Policy == "s3fifo" && row.Failures != 1 {
			t.Errorf("summary should record the failure, got %d", row.Failures)
		}
	}
}

// TestSweepIsDeterministic: the sweep is the fitness function, so identical
// inputs must give identical numbers.
func TestSweepIsDeterministic(t *testing.T) {
	first, _ := testSweep(t)
	for run := 1; run < 3; run++ {
		got, _ := testSweep(t)
		if len(got.Results) != len(first.Results) {
			t.Fatalf("run %d: cell count changed", run)
		}
		for i := range got.Results {
			a, b := first.Results[i], got.Results[i]
			if a.Trace != b.Trace || a.Policy != b.Policy || a.SizeSpec != b.SizeSpec {
				t.Fatalf("run %d: cell %d ordering changed", run, i)
			}
			if a.RequestMissRatio != b.RequestMissRatio || a.ByteMissRatio != b.ByteMissRatio {
				t.Fatalf("run %d: %s/%s/%s miss ratio changed: %v vs %v",
					run, a.Trace, a.SizeSpec, a.Policy, a.RequestMissRatio, b.RequestMissRatio)
			}
			if a.Evictions != b.Evictions {
				t.Fatalf("run %d: %s/%s/%s evictions changed", run, a.Trace, a.SizeSpec, a.Policy)
			}
		}
	}
}

func TestMarkdownRendersUndefinedAsDash(t *testing.T) {
	rep, _ := testSweep(t)
	md := Markdown(rep)

	for _, want := range []string{
		"# Cache policy benchmark",
		"## Summary across traces",
		"reduction vs FIFO",
		"gap closed vs OPT",
		"## Reading these numbers",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown is missing %q", want)
		}
	}

	// An undefined ratio must never be rendered as a number.
	if got := pct(nil); got != "—" {
		t.Errorf("pct(nil) = %q, want an em dash", got)
	}
	v := 0.0
	if got := pct(&v); got != "+0.0%" {
		t.Errorf("pct(0) = %q, want +0.0%%", got)
	}
}

func TestBytesHumanAndCommas(t *testing.T) {
	if got := bytesHuman(1536); got != "1.5 KiB" {
		t.Errorf("bytesHuman(1536) = %q", got)
	}
	if got := bytesHuman(900); got != "900 B" {
		t.Errorf("bytesHuman(900) = %q", got)
	}
	if got := commas(1234567); got != "1,234,567" {
		t.Errorf("commas(1234567) = %q", got)
	}
	if got := commas(12); got != "12" {
		t.Errorf("commas(12) = %q", got)
	}
}

// TestFindTracesIgnoresBareCompressedFiles: a bare .zst says nothing about the
// contents. The cacheMon dataset ships binary oracleGeneral traces under
// exactly that name, and matching them made a directory scan try to parse
// binary as CSV.
func TestFindTracesIgnoresBareCompressedFiles(t *testing.T) {
	dir := t.TempDir()
	writeTrace(t, dir, "good.csv", 100, 20, 1)
	for _, name := range []string{"binary.oracleGeneral.zst", "binary.oracleGeneral", "archive.gz", ".hidden.csv"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("\x00\x01binary"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	paths, err := FindTraces(dir)
	if err != nil {
		t.Fatalf("FindTraces: %v", err)
	}
	if len(paths) != 1 || filepath.Base(paths[0]) != "good.csv" {
		t.Errorf("FindTraces returned %v, want only good.csv", paths)
	}

	// The glob is the escape hatch for anything the extension list misses.
	globbed, err := MatchTraces(filepath.Join(dir, "*.oracleGeneral"))
	if err != nil || len(globbed) != 1 {
		t.Errorf("MatchTraces = %v, %v; want one match", globbed, err)
	}
}

// TestUnreadableTraceIsSkippedNotFatal: one bad file must not cost the other
// traces their runs.
func TestUnreadableTraceIsSkippedNotFatal(t *testing.T) {
	dir := t.TempDir()
	writeTrace(t, dir, "good.csv", 4000, 300, 5)
	if err := os.WriteFile(filepath.Join(dir, "bad.csv"), []byte("\x00\x01\x02not a trace\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths, _ := FindTraces(dir)
	specs, _ := ParseSpecs("fifo,lru")
	size, _ := replay.ParseSize("0.1x")

	rep, err := Run(Options{
		TracePaths: paths, Specs: specs, Sizes: []replay.Size{size},
		Seed: 1, CSVParams: trace.DefaultCSVParams(),
	})
	if err != nil {
		t.Fatalf("one unreadable trace should not fail the sweep: %v", err)
	}
	if len(rep.Traces) != 1 {
		t.Errorf("got %d usable traces, want 1", len(rep.Traces))
	}
	if len(rep.Skipped) != 1 {
		t.Fatalf("got %d skipped, want 1", len(rep.Skipped))
	}
	if !strings.Contains(Markdown(rep), "could not be read") {
		t.Error("the report must say a trace was skipped rather than hiding it")
	}
	if len(rep.Results) != 2 {
		t.Errorf("got %d cells, want 2 from the readable trace", len(rep.Results))
	}
}

// TestAllTracesUnreadableIsFatal: losing every trace usually means the column
// mapping is wrong, which should fail loudly rather than print an empty table.
func TestAllTracesUnreadableIsFatal(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.csv"), []byte("\x00\x01nope\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths, _ := FindTraces(dir)
	specs, _ := ParseSpecs("lru")
	size, _ := replay.ParseSize("0.1x")

	_, err := Run(Options{
		TracePaths: paths, Specs: specs, Sizes: []replay.Size{size},
		Seed: 1, CSVParams: trace.DefaultCSVParams(),
	})
	if err == nil {
		t.Fatal("a sweep with no readable trace should fail")
	}
	if !strings.Contains(err.Error(), "trace-format-params") {
		t.Errorf("the error should point at the likely cause, got: %v", err)
	}
}
