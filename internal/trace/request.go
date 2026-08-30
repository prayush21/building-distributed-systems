// Package trace reads cache workload traces into a normalized request stream.
//
// The reader is deliberately compatible with libCacheSim's CSV handling
// (1-indexed column numbers, the same --trace-format-params keys) so that
// results from this harness can be cross-checked against cachesim on the same
// file with the same arguments.
package trace

// Request is the normalized form of one trace record. Everything downstream of
// the reader sees only this: policies and the cache never learn what format the
// trace was in.
type Request struct {
	// Time is the trace timestamp, or the 0-based request index when the
	// trace has no time column. Only its ordering is used.
	Time int64

	// Key is the object id. Keys are interned by the reader, so repeated
	// references to one object share a single backing string.
	Key string

	// Size is the object size in bytes, or 1 when sizes are unavailable or
	// have been suppressed by IgnoreObjSize.
	Size int
}

// Stats summarizes a loaded trace.
type Stats struct {
	Requests      int64 `json:"requests"`
	UniqueObjects int64 `json:"unique_objects"`

	// WorkingSetBytes is the sum of each unique object's size: the bytes
	// needed to hold the entire trace at once. Fractional cache sizes
	// ("0.1x") are taken against this, matching libCacheSim's fractional
	// working-set-size sizing.
	WorkingSetBytes int64 `json:"working_set_bytes"`
}

// Summarize computes trace statistics. When an object appears at several sizes,
// its largest size is used for the working set, so a fractional cache size can
// always hold the fraction of the trace it claims to.
func Summarize(reqs []Request) Stats {
	sizes := make(map[string]int, 1024)
	for i := range reqs {
		if cur, ok := sizes[reqs[i].Key]; !ok || reqs[i].Size > cur {
			sizes[reqs[i].Key] = reqs[i].Size
		}
	}

	st := Stats{Requests: int64(len(reqs)), UniqueObjects: int64(len(sizes))}
	// Sum over the request slice rather than ranging the map: map iteration
	// order is unspecified, and float-free integer summation over a stable
	// order keeps this bit-for-bit reproducible.
	counted := make(map[string]bool, len(sizes))
	for i := range reqs {
		k := reqs[i].Key
		if counted[k] {
			continue
		}
		counted[k] = true
		st.WorkingSetBytes += int64(sizes[k])
	}
	return st
}
