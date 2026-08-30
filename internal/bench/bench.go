// Package bench sweeps a set of policies across a directory of traces at a
// range of cache sizes and reports comparable results.
//
// Raw miss ratios are not comparable across traces: one workload's 0.30 may be
// another's 0.05, so averaging them or ranking policies by them says more about
// the traces than the policies. Two derived numbers do the real work:
//
//   - Miss ratio reduction against FIFO, the convention the SIEVE authors use,
//     which normalizes each trace against its own weakest baseline.
//   - Gap closed against Belady, (LRU - policy) / (LRU - OPT), which states how
//     much of the headroom a workload actually had was captured. "Closed 40% of
//     the LRU-to-OPT gap" survives scrutiny in a way "8% better than LRU" does
//     not, because it is scaled by how much was available to win.
package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/prayush21/building-distributed-systems/internal/replay"
	"github.com/prayush21/building-distributed-systems/internal/trace"
)

// Baseline policy names. FIFO is the denominator for reduction, LRU and Belady
// the endpoints of the gap.
const (
	BaselineFIFO   = "fifo"
	BaselineLRU    = "lru"
	BaselineBelady = "belady"
)

// minGapForRatio is the smallest LRU-to-OPT gap for which "gap closed" is
// reported. Below it the denominator is comparable to the noise in the
// numerator and the ratio would be a large number with no meaning, so it is
// reported as undefined rather than as a striking result.
const minGapForRatio = 1e-6

// Spec is one policy configuration in the sweep. The same policy may appear
// more than once with different parameters, which is how a parameter sweep is
// expressed.
type Spec struct {
	Name   string // registered policy name
	Params string // policy-specific parameters
	Label  string // unique display label
}

// ParseSpecs parses "fifo,lru,s3fifo:small-size-ratio=0.2".
//
// A policy repeated with different parameters gets a label carrying those
// parameters, so the two rows can be told apart in the report.
func ParseSpecs(s string) ([]Spec, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("no policies given")
	}
	var specs []Spec
	seen := map[string]bool{}
	for _, tok := range strings.Split(s, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		name, params, hasParams := strings.Cut(tok, ":")
		name = strings.ToLower(strings.TrimSpace(name))
		params = strings.TrimSpace(params)

		label := name
		if hasParams && params != "" {
			label = name + "(" + params + ")"
		}
		if seen[label] {
			return nil, fmt.Errorf("policy %q listed twice", label)
		}
		seen[label] = true
		specs = append(specs, Spec{Name: name, Params: params, Label: label})
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("no policies given")
	}
	return specs, nil
}

// Options configure a sweep.
type Options struct {
	TracePaths []string
	Specs      []Spec
	Sizes      []replay.Size

	IgnoreObjSize bool
	WarmupFrac    float64
	Seed          int64
	MaxRequests   int64
	CSVParams     trace.CSVParams

	// Oracle enables the offline Belady baseline. Without it the gap-closed
	// column cannot be computed and is reported as undefined.
	Oracle bool
}

// traceExtensions are the suffixes FindTraces accepts.
//
// Deliberately narrow. A bare ".zst" or ".gz" is not enough to conclude a file
// is CSV: the cacheMon dataset ships binary oracleGeneral traces under exactly
// that name, and matching them produced a directory scan that picked up
// unreadable files and reported them as malformed CSV. Use --trace-glob when
// the naming does not fit.
var traceExtensions = []string{
	".csv", ".csv.gz", ".csv.zst",
	".tsv", ".tsv.gz", ".tsv.zst",
	".txt", ".txt.gz", ".txt.zst",
}

// FindTraces lists trace files in a directory, sorted, so a sweep over the same
// directory always visits them in the same order.
func FindTraces(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") {
			continue
		}
		for _, ext := range traceExtensions {
			if strings.HasSuffix(strings.ToLower(name), ext) {
				out = append(out, filepath.Join(dir, name))
				break
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf(
			"no trace files in %s (looked for %s; use --trace-glob for other names)",
			dir, strings.Join(traceExtensions, ", "))
	}
	sort.Strings(out)
	return out, nil
}

// MatchTraces lists files matching a glob, sorted, for layouts FindTraces does
// not cover.
func MatchTraces(pattern string) ([]string, error) {
	out, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no files match %s", pattern)
	}
	sort.Strings(out)
	return out, nil
}

// Cell is one (trace, size, policy) result.
type Cell struct {
	Trace     string `json:"trace"`
	Policy    string `json:"policy"`
	SizeSpec  string `json:"size_spec"`
	SizeBytes int64  `json:"size_bytes"`

	RequestMissRatio float64 `json:"request_miss_ratio"`
	ByteMissRatio    float64 `json:"byte_miss_ratio"`
	Evictions        int64   `json:"evictions"`

	PeakMetadataBytes              int     `json:"peak_metadata_bytes"`
	MetadataBytesPerResidentObject float64 `json:"metadata_bytes_per_resident_object"`
	NanosecondsPerOp               float64 `json:"nanoseconds_per_op"`

	// MRReductionVsFIFO is (fifo - policy) / fifo. Positive is better.
	MRReductionVsFIFO *float64 `json:"mr_reduction_vs_fifo"`

	// GapClosedVsBelady is (lru - policy) / (lru - opt). 0 means "as good as
	// LRU", 1 means "as good as Belady". It can exceed 1: Belady is only
	// provably optimal for uniform object sizes, so with variable sizes a
	// policy can legitimately beat it.
	GapClosedVsBelady *float64 `json:"gap_closed_vs_belady"`

	// Error is set when this configuration could not run at all, for example
	// S3-FIFO at a cache size too small to split into its queues. The sweep
	// continues; the cell reports why it is empty.
	Error string `json:"error,omitempty"`

	// Invalid marks a cell whose numbers cannot be trusted because the policy
	// violated its contract, as distinct from one that simply could not be
	// configured. A sweep with an unconfigurable cell is a normal result; a
	// sweep with an invalid one is a bug, and cmd/bench exits non-zero.
	Invalid bool `json:"invalid,omitempty"`
}

// TraceInfo describes one input.
type TraceInfo struct {
	Path            string `json:"path"`
	Name            string `json:"name"`
	Requests        int64  `json:"requests"`
	UniqueObjects   int64  `json:"unique_objects"`
	WorkingSetBytes int64  `json:"working_set_bytes"`
}

// Config records the sweep that produced a report.
type Config struct {
	Policies      []string `json:"policies"`
	Sizes         []string `json:"sizes"`
	IgnoreObjSize bool     `json:"ignore_obj_size"`
	WarmupFrac    float64  `json:"warmup_frac"`
	Seed          int64    `json:"seed"`
	Oracle        bool     `json:"oracle"`
	MaxRequests   int64    `json:"max_requests,omitempty"`
}

// SummaryRow aggregates one (policy, size) across every trace, which is the
// level at which a claim about a policy should be made.
type SummaryRow struct {
	Policy   string `json:"policy"`
	SizeSpec string `json:"size_spec"`
	Traces   int    `json:"traces"`

	MeanMissRatio float64 `json:"mean_miss_ratio"`

	MeanReductionVsFIFO   *float64 `json:"mean_reduction_vs_fifo"`
	MedianReductionVsFIFO *float64 `json:"median_reduction_vs_fifo"`

	MeanGapClosed   *float64 `json:"mean_gap_closed_vs_belady"`
	MedianGapClosed *float64 `json:"median_gap_closed_vs_belady"`

	// Failures counts cells that could not run, so a favorable mean over a
	// subset of traces cannot be mistaken for a result over all of them.
	Failures int `json:"failures"`
}

// SkippedTrace records a trace that could not be read.
type SkippedTrace struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// Report is the full result of a sweep.
type Report struct {
	Config  Config         `json:"config"`
	Traces  []TraceInfo    `json:"traces"`
	Skipped []SkippedTrace `json:"skipped_traces,omitempty"`
	Results []Cell         `json:"results"`
	Summary []SummaryRow   `json:"summary"`
}

// Invalid lists cells whose numbers cannot be trusted because a policy violated
// its contract. Any non-empty result invalidates the sweep.
func (r *Report) Invalid() []Cell {
	var out []Cell
	for _, c := range r.Results {
		if c.Invalid {
			out = append(out, c)
		}
	}
	return out
}

// Run executes the sweep. Traces are loaded once each and replayed for every
// size and policy, since loading dominates the cost of a small replay.
func Run(opts Options) (*Report, error) {
	if len(opts.TracePaths) == 0 {
		return nil, fmt.Errorf("no traces given")
	}
	if len(opts.Specs) == 0 {
		return nil, fmt.Errorf("no policies given")
	}
	if len(opts.Sizes) == 0 {
		return nil, fmt.Errorf("no cache sizes given")
	}

	rep := &Report{Config: Config{
		Sizes:         make([]string, 0, len(opts.Sizes)),
		IgnoreObjSize: opts.IgnoreObjSize,
		WarmupFrac:    opts.WarmupFrac,
		Seed:          opts.Seed,
		Oracle:        opts.Oracle,
		MaxRequests:   opts.MaxRequests,
	}}
	for _, s := range opts.Specs {
		rep.Config.Policies = append(rep.Config.Policies, s.Label)
	}
	for _, s := range opts.Sizes {
		rep.Config.Sizes = append(rep.Config.Sizes, s.Spec)
	}

	for _, path := range opts.TracePaths {
		reqs, ts, _, err := replay.Load(replay.Options{
			TracePath:     path,
			IgnoreObjSize: opts.IgnoreObjSize,
			MaxRequests:   opts.MaxRequests,
			CSVParams:     opts.CSVParams,
		})
		if err != nil {
			// One unreadable file must not cost the other traces their runs.
			// It is recorded and reported; only losing every trace is fatal,
			// since that usually means the column mapping is wrong.
			rep.Skipped = append(rep.Skipped, SkippedTrace{Path: path, Reason: err.Error()})
			continue
		}
		name := traceName(path)
		rep.Traces = append(rep.Traces, TraceInfo{
			Path: path, Name: name,
			Requests: ts.Requests, UniqueObjects: ts.UniqueObjects,
			WorkingSetBytes: ts.WorkingSetBytes,
		})

		for _, size := range opts.Sizes {
			group := make([]Cell, 0, len(opts.Specs))
			for _, spec := range opts.Specs {
				group = append(group, runOne(reqs, ts, opts, spec, size, name))
			}
			deriveGroup(group)
			rep.Results = append(rep.Results, group...)
		}

		// The loaded trace can be large; drop it before loading the next.
		reqs = nil
	}

	if len(rep.Traces) == 0 {
		return nil, fmt.Errorf("no trace could be read (%d skipped); check --trace-format-params: %s",
			len(rep.Skipped), rep.Skipped[0].Reason)
	}

	rep.Summary = summarize(rep, opts)
	return rep, nil
}

func runOne(reqs []trace.Request, ts trace.Stats, opts Options, spec Spec, size replay.Size, traceName string) Cell {
	cell := Cell{Trace: traceName, Policy: spec.Label, SizeSpec: size.Spec}

	isOracle := spec.Name == BaselineBelady || spec.Name == "opt"
	if isOracle && !opts.Oracle {
		cell.Error = "offline policy skipped: --oracle not set"
		return cell
	}

	res, err := replay.Replay(reqs, ts, replay.Options{
		Policy:        spec.Name,
		PolicyParams:  spec.Params,
		Size:          size,
		IgnoreObjSize: opts.IgnoreObjSize,
		WarmupFrac:    opts.WarmupFrac,
		Seed:          opts.Seed,
		Oracle:        isOracle,
		CSVParams:     opts.CSVParams,
	})
	if err != nil {
		cell.Error = err.Error()
		return cell
	}
	if res.Metrics.PolicyErrors > 0 {
		// A policy that named victims it did not hold produced numbers that
		// mean nothing, so the cell reports the violation instead of them.
		cell.Error = fmt.Sprintf("%d policy contract violations", res.Metrics.PolicyErrors)
		cell.Invalid = true
		return cell
	}

	cell.SizeBytes = res.Run.CacheSizeBytes
	cell.RequestMissRatio = res.Metrics.RequestMissRatio
	cell.ByteMissRatio = res.Metrics.ByteMissRatio
	cell.Evictions = res.Metrics.Evictions
	cell.PeakMetadataBytes = res.Metrics.PeakMetadataBytes
	cell.MetadataBytesPerResidentObject = res.Metrics.MetadataBytesPerResidentObject
	cell.NanosecondsPerOp = res.Timing.NanosecondsPerOp
	return cell
}

// deriveGroup fills the comparative columns for one (trace, size) group, where
// all cells share the baselines.
func deriveGroup(group []Cell) {
	fifo, okFIFO := baseline(group, BaselineFIFO)
	lru, okLRU := baseline(group, BaselineLRU)
	opt, okOPT := baseline(group, BaselineBelady)

	gap := lru - opt
	gapUsable := okLRU && okOPT && gap > minGapForRatio

	for i := range group {
		if group[i].Error != "" {
			continue
		}
		if okFIFO && fifo > 0 {
			v := (fifo - group[i].RequestMissRatio) / fifo
			group[i].MRReductionVsFIFO = &v
		}
		if gapUsable {
			v := (lru - group[i].RequestMissRatio) / gap
			group[i].GapClosedVsBelady = &v
		}
	}
}

// baseline finds a baseline policy's miss ratio in a group. It matches the
// unparameterized label only: a parameterized variant is a policy under test,
// not the reference point.
func baseline(group []Cell, name string) (float64, bool) {
	for _, c := range group {
		if c.Policy == name && c.Error == "" {
			return c.RequestMissRatio, true
		}
	}
	return 0, false
}

func summarize(rep *Report, opts Options) []SummaryRow {
	var rows []SummaryRow
	for _, spec := range opts.Specs {
		for _, size := range opts.Sizes {
			row := SummaryRow{Policy: spec.Label, SizeSpec: size.Spec}
			var mrs, reds, gaps []float64
			for _, c := range rep.Results {
				if c.Policy != spec.Label || c.SizeSpec != size.Spec {
					continue
				}
				if c.Error != "" {
					row.Failures++
					continue
				}
				mrs = append(mrs, c.RequestMissRatio)
				if c.MRReductionVsFIFO != nil {
					reds = append(reds, *c.MRReductionVsFIFO)
				}
				if c.GapClosedVsBelady != nil {
					gaps = append(gaps, *c.GapClosedVsBelady)
				}
			}
			row.Traces = len(mrs)
			if row.Traces == 0 && row.Failures == 0 {
				continue
			}
			row.MeanMissRatio = mean(mrs)
			row.MeanReductionVsFIFO = meanPtr(reds)
			row.MedianReductionVsFIFO = medianPtr(reds)
			row.MeanGapClosed = meanPtr(gaps)
			row.MedianGapClosed = medianPtr(gaps)
			rows = append(rows, row)
		}
	}
	return rows
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func meanPtr(xs []float64) *float64 {
	if len(xs) == 0 {
		return nil
	}
	v := mean(xs)
	return &v
}

// medianPtr reports the median, which is the more honest centre for these
// ratios: one trace where the LRU-to-OPT gap is narrow can throw a mean a long
// way without saying much about the policy.
func medianPtr(xs []float64) *float64 {
	if len(xs) == 0 {
		return nil
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	var v float64
	if n := len(s); n%2 == 1 {
		v = s[n/2]
	} else {
		v = (s[n/2-1] + s[n/2]) / 2
	}
	return &v
}

func traceName(path string) string {
	n := filepath.Base(path)
	for _, ext := range []string{".zst", ".gz", ".csv", ".txt"} {
		n = strings.TrimSuffix(n, ext)
	}
	return n
}
