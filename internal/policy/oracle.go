package policy

import "math"

// NeverAgain is the next-access time reported for a key that the trace never
// requests again.
const NeverAgain int64 = math.MaxInt64

// OraclePolicy is an offline policy that is allowed to see the future.
//
// This path is deliberately separate from Policy. The next-access hint is
// delivered only through OnNextAccess, which Policy does not declare, and only
// by a cache explicitly constructed in oracle mode. A normal online policy
// therefore cannot reach this information: it is a compile-time separation,
// not a convention.
//
// The only implementation is the Belady optimal baseline, which exists to give
// an upper bound to measure against ("closed X% of the LRU-to-OPT gap"), never
// as a deployable policy.
type OraclePolicy interface {
	Policy

	// OnNextAccess is called by the cache for every request, immediately
	// before the OnHit or CanAdmit/OnAdmit sequence for that same key. next
	// is the trace time at which key is requested again, or NeverAgain if it
	// never is.
	OnNextAccess(key string, next int64)
}
