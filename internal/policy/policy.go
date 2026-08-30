// Package policy defines the cache replacement policy interface and the
// baseline policy implementations.
//
// Policies are deliberately kept blind: they see keys and sizes, never values,
// never the trace, and never the future. Capacity accounting is owned by the
// cache (see internal/cache), not by the policy. A policy's only job is to
// maintain enough bookkeeping to name a victim when asked.
//
// Determinism is a hard requirement of this package: no implementation may
// depend on Go map iteration order, and any randomness must be driven by an
// explicitly seeded source. Two runs over identical inputs must make identical
// eviction decisions.
package policy

// Policy is an online cache replacement policy.
//
// The cache calls these methods in a fixed order. For a request on a resident
// key, only OnHit fires. For a request on a non-resident key, the cache calls
// CanAdmit first; if it returns true the cache evicts until the object fits
// (each eviction calling Evict then OnRemove) and then calls OnAdmit.
//
// Implementations must not retain the key strings' backing arrays in a way
// that assumes the cache owns them, and must tolerate OnRemove for a key they
// are not tracking.
type Policy interface {
	// OnHit is called on every request for a key already resident.
	OnHit(key string)

	// CanAdmit is the admission decision, and is a pure predicate: it is
	// called before any eviction happens and must not mutate policy state in
	// a way that assumes the key will be admitted. Returning false rejects
	// the admission without the cache evicting anything.
	CanAdmit(key string, size int) bool

	// OnAdmit registers a key the cache has decided to admit. It is called
	// after eviction has made room, and cannot fail: the admission decision
	// was already made by CanAdmit.
	OnAdmit(key string, size int)

	// Evict chooses a victim. It must return a key that is currently
	// resident. Returning ok=false means the policy has nothing to evict,
	// and the cache will stop trying to make room.
	Evict() (key string, ok bool)

	// OnRemove is called when the cache removes a key for any reason
	// (eviction, TTL, explicit delete) so the policy can drop its
	// bookkeeping. It is called after Evict for evictions, so a policy must
	// not assume OnRemove's key is the one it just returned.
	OnRemove(key string)

	// Name identifies the policy in reported metrics.
	Name() string

	// MetadataBytes is a best-effort estimate of the policy-owned metadata
	// currently held, in bytes. It should be O(1) to compute: the replayer
	// samples it on every request.
	MetadataBytes() int
}
