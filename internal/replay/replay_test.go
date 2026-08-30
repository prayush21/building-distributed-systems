package replay

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prayush21/building-distributed-systems/internal/trace"
)

// writeSyntheticTrace produces a skewed synthetic trace for unit tests only.
// Real evaluation runs against real traces; this exists so determinism and
// plumbing can be tested without a network fetch.
func writeSyntheticTrace(t *testing.T, requests, objects int) string {
	t.Helper()
	rng := rand.New(rand.NewSource(12345))

	var b strings.Builder
	b.WriteString("timestamp,obj_id,obj_size\n")
	for i := 0; i < requests; i++ {
		// Square the uniform draw to concentrate references on low ids.
		u := rng.Float64()
		id := int(u * u * float64(objects))
		size := 64 + (id*37)%4096 // stable per object, widely spread
		fmt.Fprintf(&b, "%d,k%d,%d\n", 1000+i, id, size)
	}

	path := filepath.Join(t.TempDir(), "synthetic.csv")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write trace: %v", err)
	}
	return path
}

func baseOptions(path string) Options {
	size, _ := ParseSize("0.1x")
	return Options{
		TracePath: path,
		Policy:    "lru",
		Size:      size,
		Seed:      1,
		CSVParams: trace.DefaultCSVParams(),
	}
}

// stableJSON marshals a result with the timing fields cleared, which are the
// only fields allowed to differ between two identical runs.
func stableJSON(t *testing.T, r *Result) []byte {
	t.Helper()
	c := *r
	c.Timing = Timing{}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestByteIdenticalAcrossRuns is the determinism guarantee: identical inputs
// must produce identical JSON apart from timing. Go randomizes map iteration
// order on every range, so repeated runs in one process exercise any accidental
// dependence on it.
func TestByteIdenticalAcrossRuns(t *testing.T) {
	path := writeSyntheticTrace(t, 20000, 2000)
	opts := baseOptions(path)

	first, err := Run(opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := stableJSON(t, first)

	for i := 1; i < 8; i++ {
		got, err := Run(opts)
		if err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
		if g := stableJSON(t, got); string(g) != string(want) {
			t.Fatalf("run %d diverged from run 0\n--- run 0 ---\n%s\n--- run %d ---\n%s", i, want, i, g)
		}
	}
}

// TestTimingFieldsAreTheOnlyVariation confirms the determinism check above is
// meaningful: timing really is populated, so clearing it is not vacuous.
func TestTimingFieldsAreTheOnlyVariation(t *testing.T) {
	path := writeSyntheticTrace(t, 5000, 500)
	res, err := Run(baseOptions(path))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Timing.MeasuredNanos <= 0 {
		t.Error("MeasuredNanos not populated; the determinism test would be vacuous")
	}
	if res.Timing.NanosecondsPerOp <= 0 {
		t.Error("NanosecondsPerOp not populated")
	}
}

func TestWarmupExcludedFromMetrics(t *testing.T) {
	path := writeSyntheticTrace(t, 10000, 1000)

	cold := baseOptions(path)
	warm := baseOptions(path)
	warm.WarmupFrac = 0.2

	coldRes, err := Run(cold)
	if err != nil {
		t.Fatalf("Run cold: %v", err)
	}
	warmRes, err := Run(warm)
	if err != nil {
		t.Fatalf("Run warm: %v", err)
	}

	if warmRes.Run.WarmupRequests != 2000 {
		t.Errorf("WarmupRequests = %d, want 2000", warmRes.Run.WarmupRequests)
	}
	if warmRes.Metrics.Requests != 8000 {
		t.Errorf("measured requests = %d, want 8000", warmRes.Metrics.Requests)
	}
	if warmRes.Metrics.Requests+warmRes.Run.WarmupRequests != coldRes.Metrics.Requests {
		t.Error("warmup plus measured should account for the whole trace")
	}
	// Warmup fills the cache, so the measured window must not be penalized by
	// compulsory misses the cold run pays.
	if warmRes.Metrics.RequestMissRatio >= coldRes.Metrics.RequestMissRatio {
		t.Errorf("warm miss ratio %v should be below cold %v",
			warmRes.Metrics.RequestMissRatio, coldRes.Metrics.RequestMissRatio)
	}
}

func TestWarmupFracValidated(t *testing.T) {
	path := writeSyntheticTrace(t, 1000, 100)
	for _, f := range []float64{-0.1, 1.0, 1.5} {
		opts := baseOptions(path)
		opts.WarmupFrac = f
		if _, err := Run(opts); err == nil {
			t.Errorf("--warmup-frac %v should be rejected", f)
		}
	}
}

// TestIgnoreObjSizeMatchesLibCacheSimSemantics: every object counts as 1, so
// the cache size is a count of objects and the two miss ratios coincide.
func TestIgnoreObjSizeMatchesLibCacheSimSemantics(t *testing.T) {
	path := writeSyntheticTrace(t, 10000, 1000)

	size, _ := ParseSize("100")
	opts := baseOptions(path)
	opts.Size = size
	opts.IgnoreObjSize = true

	res, err := Run(opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Metrics.RequestMissRatio != res.Metrics.ByteMissRatio {
		t.Errorf("under --ignore-obj-size the miss ratios must coincide, got %v and %v",
			res.Metrics.RequestMissRatio, res.Metrics.ByteMissRatio)
	}
	if res.Metrics.FinalResidentObjects > 100 {
		t.Errorf("resident objects = %d, want at most the 100-object capacity",
			res.Metrics.FinalResidentObjects)
	}
	if res.Trace.WorkingSetBytes != res.Trace.UniqueObjects {
		t.Errorf("working set %d should equal unique objects %d under --ignore-obj-size",
			res.Trace.WorkingSetBytes, res.Trace.UniqueObjects)
	}
}

func TestLargerCacheMissesLess(t *testing.T) {
	path := writeSyntheticTrace(t, 20000, 2000)

	var prev float64 = 1.1
	for _, spec := range []string{"0.01x", "0.05x", "0.1x", "0.5x"} {
		size, err := ParseSize(spec)
		if err != nil {
			t.Fatal(err)
		}
		opts := baseOptions(path)
		opts.Size = size
		res, err := Run(opts)
		if err != nil {
			t.Fatalf("Run %s: %v", spec, err)
		}
		if res.Metrics.RequestMissRatio > prev {
			t.Errorf("miss ratio at %s (%v) exceeds the smaller cache's (%v): LRU is a stack policy and must not",
				spec, res.Metrics.RequestMissRatio, prev)
		}
		prev = res.Metrics.RequestMissRatio
	}
}

func TestOracleRefusedWithoutFlag(t *testing.T) {
	path := writeSyntheticTrace(t, 100, 20)
	opts := baseOptions(path)
	opts.Policy = "lru"
	opts.Oracle = true
	// LRU is online; asking for it through the oracle path is an error rather
	// than a silent downgrade.
	if _, err := Run(opts); err == nil {
		t.Error("running an online policy with --oracle should fail")
	} else if !strings.Contains(err.Error(), "online policy") {
		t.Errorf("unhelpful error: %v", err)
	}
}

func TestUnknownPolicy(t *testing.T) {
	path := writeSyntheticTrace(t, 100, 20)
	opts := baseOptions(path)
	opts.Policy = "not-a-policy"
	if _, err := Run(opts); err == nil {
		t.Error("unknown policy should fail")
	}
}

// TestNextAccessTimes covers the pre-pass Belady will consume.
func TestNextAccessTimes(t *testing.T) {
	reqs := []trace.Request{
		{Key: "a"}, {Key: "b"}, {Key: "a"}, {Key: "c"}, {Key: "b"},
	}
	got := NextAccessTimes(reqs)
	const never = int64(9223372036854775807)
	want := []int64{2, 4, never, never, never}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("next[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}
