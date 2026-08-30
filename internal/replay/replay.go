// Package replay drives a cache and policy over a trace and reports metrics.
//
// The engine is single-threaded and fully deterministic: the same trace, policy,
// size, and seed always produce the same decisions and therefore the same
// metrics. Only the timing fields vary between runs.
package replay

import (
	"fmt"
	"time"

	"github.com/prayush21/building-distributed-systems/internal/cache"
	"github.com/prayush21/building-distributed-systems/internal/policy"
	"github.com/prayush21/building-distributed-systems/internal/trace"
)

// Options configure a single replay.
type Options struct {
	TracePath     string
	Policy        string
	PolicyParams  string
	Size          Size
	IgnoreObjSize bool
	WarmupFrac    float64
	Seed          int64
	Oracle        bool
	MaxRequests   int64
	CSVParams     trace.CSVParams
}

// Result is the JSON document written to stdout.
//
// Everything that varies between two identical runs lives under Timing, so a
// determinism check can compare the rest byte for byte.
type Result struct {
	Trace   TraceInfo `json:"trace"`
	Run     RunInfo   `json:"run"`
	Metrics Metrics   `json:"metrics"`
	Timing  Timing    `json:"timing"`
}

// TraceInfo records what was read, so a result file is self-describing.
type TraceInfo struct {
	Path             string `json:"path"`
	Format           string `json:"format"`
	FormatParams     string `json:"format_params"`
	Requests         int64  `json:"requests"`
	UniqueObjects    int64  `json:"unique_objects"`
	WorkingSetBytes  int64  `json:"working_set_bytes"`
	HeaderSkipped    bool   `json:"header_skipped"`
	MalformedSkipped int64  `json:"malformed_skipped"`
	ZeroSizeRecords  int64  `json:"zero_size_records"`
	Truncated        bool   `json:"truncated"`
}

// RunInfo records the configuration that produced the metrics.
type RunInfo struct {
	Policy           string  `json:"policy"`
	PolicyParams     string  `json:"policy_params"`
	Oracle           bool    `json:"oracle"`
	CacheSizeSpec    string  `json:"cache_size_spec"`
	CacheSizeBytes   int64   `json:"cache_size_bytes"`
	IgnoreObjSize    bool    `json:"ignore_obj_size"`
	Seed             int64   `json:"seed"`
	WarmupFrac       float64 `json:"warmup_frac"`
	WarmupRequests   int64   `json:"warmup_requests"`
	MeasuredRequests int64   `json:"measured_requests"`
}

// Metrics are the scored results over the measured window.
type Metrics struct {
	RequestMissRatio float64 `json:"request_miss_ratio"`
	ByteMissRatio    float64 `json:"byte_miss_ratio"`

	Requests int64 `json:"requests"`
	Misses   int64 `json:"misses"`
	Hits     int64 `json:"hits"`

	BytesRequested int64 `json:"bytes_requested"`
	BytesMissed    int64 `json:"bytes_missed"`

	Evictions  int64 `json:"evictions"`
	Admissions int64 `json:"admissions"`
	Rejections int64 `json:"rejections"`

	PeakMetadataBytes              int     `json:"peak_metadata_bytes"`
	MetadataBytesPerResidentObject float64 `json:"metadata_bytes_per_resident_object"`

	FinalResidentObjects int   `json:"final_resident_objects"`
	FinalResidentBytes   int64 `json:"final_resident_bytes"`

	// PolicyErrors counts contract violations by the policy. Any non-zero
	// value invalidates the run; the CLI exits non-zero when it happens.
	PolicyErrors int64 `json:"policy_errors"`
}

// Timing is excluded from determinism comparisons.
type Timing struct {
	// NanosecondsPerOp is measured, not modelled: the measured window is
	// replayed from an in-memory request slice with a wall clock around the
	// loop, so trace parsing is not counted against the policy.
	NanosecondsPerOp float64 `json:"nanoseconds_per_op"`
	MeasuredNanos    int64   `json:"measured_nanos"`
	LoadNanos        int64   `json:"load_nanos"`
}

// Load reads and summarizes a trace. It is separate from Replay so that a
// sweep over many policies and sizes reads the file only once.
func Load(opts Options) ([]trace.Request, trace.Stats, trace.ReadStats, error) {
	reqs, rs, err := trace.LoadFile(opts.TracePath, opts.CSVParams, trace.LoadOptions{
		IgnoreObjSize: opts.IgnoreObjSize,
		MaxRequests:   opts.MaxRequests,
	})
	if err != nil {
		return nil, trace.Stats{}, rs, err
	}
	return reqs, trace.Summarize(reqs), rs, nil
}

// Run loads a trace and replays it once.
func Run(opts Options) (*Result, error) {
	start := time.Now()
	reqs, ts, rs, err := Load(opts)
	if err != nil {
		return nil, err
	}
	loadNanos := time.Since(start).Nanoseconds()

	res, err := Replay(reqs, ts, opts)
	if err != nil {
		return nil, err
	}
	res.Timing.LoadNanos = loadNanos
	res.Trace.HeaderSkipped = rs.HeaderSkipped
	res.Trace.MalformedSkipped = rs.MalformedSkipped
	res.Trace.ZeroSizeRecords = rs.ZeroSizeRecords
	res.Trace.Truncated = rs.Truncated
	return res, nil
}

// Replay drives one policy over an already-loaded trace.
func Replay(reqs []trace.Request, ts trace.Stats, opts Options) (*Result, error) {
	if len(reqs) == 0 {
		return nil, fmt.Errorf("replay: empty trace")
	}
	if opts.WarmupFrac < 0 || opts.WarmupFrac >= 1 {
		return nil, fmt.Errorf("replay: --warmup-frac must be in [0, 1), got %v", opts.WarmupFrac)
	}

	capacity, err := opts.Size.Resolve(ts.WorkingSetBytes)
	if err != nil {
		return nil, err
	}

	cfg := policy.Config{CacheSize: capacity, Seed: opts.Seed, Params: opts.PolicyParams}
	params, err := policy.ParseParams(opts.PolicyParams)
	if err != nil {
		return nil, err
	}

	// The oracle path is entered only on an explicit request, and an online
	// policy asked for with --oracle is an error rather than a silent
	// downgrade. Both directions are enforced by the registry.
	var (
		c       *cache.Cache
		polName string
	)
	if opts.Oracle {
		p, err := policy.NewOracle(opts.Policy, cfg)
		if err != nil {
			return nil, err
		}
		c, polName = cache.NewOracle(capacity, p), p.Name()
	} else {
		p, err := policy.New(opts.Policy, cfg)
		if err != nil {
			return nil, err
		}
		c, polName = cache.New(capacity, p), p.Name()
	}

	warmup := int(float64(len(reqs)) * opts.WarmupFrac)
	if warmup >= len(reqs) {
		warmup = len(reqs) - 1
	}

	// Next-access times are computed only for the oracle path, so an online
	// run never even materializes the future.
	var next []int64
	if opts.Oracle {
		next = NextAccessTimes(reqs)
	}

	// Warmup populates the cache but is excluded from reported metrics.
	if opts.Oracle {
		for i := 0; i < warmup; i++ {
			c.AccessOracle(reqs[i].Key, reqs[i].Size, next[i])
		}
	} else {
		for i := 0; i < warmup; i++ {
			c.Access(reqs[i].Key, reqs[i].Size)
		}
	}
	c.ResetStats()

	measuredStart := time.Now()
	if opts.Oracle {
		for i := warmup; i < len(reqs); i++ {
			c.AccessOracle(reqs[i].Key, reqs[i].Size, next[i])
		}
	} else {
		for i := warmup; i < len(reqs); i++ {
			c.Access(reqs[i].Key, reqs[i].Size)
		}
	}
	measuredNanos := time.Since(measuredStart).Nanoseconds()

	st := c.Stats()
	nsPerOp := 0.0
	if st.Requests > 0 {
		nsPerOp = float64(measuredNanos) / float64(st.Requests)
	}

	return &Result{
		Trace: TraceInfo{
			Path:            opts.TracePath,
			Format:          "csv",
			FormatParams:    formatParamsString(opts.CSVParams),
			Requests:        ts.Requests,
			UniqueObjects:   ts.UniqueObjects,
			WorkingSetBytes: ts.WorkingSetBytes,
		},
		Run: RunInfo{
			Policy:           polName,
			PolicyParams:     params.Sorted(),
			Oracle:           opts.Oracle,
			CacheSizeSpec:    opts.Size.Spec,
			CacheSizeBytes:   capacity,
			IgnoreObjSize:    opts.IgnoreObjSize,
			Seed:             opts.Seed,
			WarmupFrac:       opts.WarmupFrac,
			WarmupRequests:   int64(warmup),
			MeasuredRequests: int64(len(reqs) - warmup),
		},
		Metrics: Metrics{
			RequestMissRatio:               st.MissRatio(),
			ByteMissRatio:                  st.ByteMissRatio(),
			Requests:                       st.Requests,
			Misses:                         st.Misses,
			Hits:                           st.Hits,
			BytesRequested:                 st.BytesRequested,
			BytesMissed:                    st.BytesMissed,
			Evictions:                      st.Evictions,
			Admissions:                     st.Admissions,
			Rejections:                     st.Rejections,
			PeakMetadataBytes:              c.PeakMetadataBytes(),
			MetadataBytesPerResidentObject: c.MetadataBytesPerResidentObject(),
			FinalResidentObjects:           c.Len(),
			FinalResidentBytes:             c.Used(),
			PolicyErrors:                   st.PolicyErrors,
		},
		Timing: Timing{
			NanosecondsPerOp: nsPerOp,
			MeasuredNanos:    measuredNanos,
		},
	}, nil
}

// NextAccessTimes returns, for each request, the index of the next request for
// the same key, or policy.NeverAgain.
//
// Indices are used rather than trace timestamps because only the ordering
// matters and indices are strictly increasing, while real trace timestamps
// repeat. This matches libCacheSim's next_access_vtime.
func NextAccessTimes(reqs []trace.Request) []int64 {
	next := make([]int64, len(reqs))
	last := make(map[string]int64, 4096)
	for i := len(reqs) - 1; i >= 0; i-- {
		if n, ok := last[reqs[i].Key]; ok {
			next[i] = n
		} else {
			next[i] = policy.NeverAgain
		}
		last[reqs[i].Key] = int64(i)
	}
	return next
}

func formatParamsString(p trace.CSVParams) string {
	header := "auto"
	if p.HasHeader != nil {
		if *p.HasHeader {
			header = "true"
		} else {
			header = "false"
		}
	}
	delim := string(rune(p.Delimiter))
	if p.Delimiter == '\t' {
		delim = "tab"
	}
	return fmt.Sprintf("time-col=%d,obj-id-col=%d,obj-size-col=%d,delimiter=%s,has-header=%s,obj-id-is-num=%t",
		p.TimeCol, p.ObjIDCol, p.ObjSizeCol, delim, header, p.ObjIDIsNum)
}
