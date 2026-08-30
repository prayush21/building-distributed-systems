// Package cache implements a byte-denominated cache whose replacement
// decisions are delegated to a policy.Policy.
//
// Capacity is measured in BYTES, never entry counts. Value sizes in real
// key-value workloads are heavily skewed, and an entry-count capacity produces
// miss ratios that do not correspond to any real deployment.
//
// The cache owns all capacity accounting. The policy is told only which keys
// exist and how large they are, and is asked only to name victims.
package cache

import (
	"github.com/prayush21/building-distributed-systems/internal/policy"
)

// entry is the cache's per-object record. Only the size is kept: values never
// enter this layer, so neither the cache nor any policy can observe them.
type entry struct {
	size int
}

// Stats are the raw counters behind every reported metric. All counts cover
// the current measurement window, which ResetStats restarts (used to exclude
// trace warmup).
type Stats struct {
	Requests int64
	Hits     int64
	Misses   int64

	BytesRequested int64
	BytesHit       int64
	BytesMissed    int64

	Evictions  int64
	Admissions int64

	// Rejections counts misses that ended without the object resident:
	// the object was larger than the whole cache, the policy's CanAdmit
	// declined it, or eviction could not free enough room.
	Rejections int64

	// PolicyErrors counts policy contract violations observed at runtime,
	// specifically Evict returning a key that is not resident. A correct
	// policy never produces one; a nonzero count invalidates the run.
	PolicyErrors int64
}

// MissRatio is the request miss ratio over the measurement window.
func (s Stats) MissRatio() float64 {
	if s.Requests == 0 {
		return 0
	}
	return float64(s.Misses) / float64(s.Requests)
}

// ByteMissRatio is the byte miss ratio over the measurement window.
func (s Stats) ByteMissRatio() float64 {
	if s.BytesRequested == 0 {
		return 0
	}
	return float64(s.BytesMissed) / float64(s.BytesRequested)
}

// Cache is a byte-bounded key/size store driven by a replacement policy.
//
// It is not safe for concurrent use. The replayer is deliberately
// single-threaded so that results are reproducible.
type Cache struct {
	data     map[string]entry
	capacity int64
	used     int64

	pol policy.Policy

	// oracle is non-nil only for a cache built by NewOracle. Its presence
	// selects the offline access path and is the only way a next-access hint
	// can reach a policy.
	oracle policy.OraclePolicy

	stats              Stats
	peakMeta           int
	residentAtPeakMeta int
}

// New builds an online cache of the given capacity in bytes.
//
// It panics if p is an OraclePolicy: running Belady through the online path
// would silently produce meaningless numbers, and a harness whose whole purpose
// is scoring policies must not have a silent-wrong-answer mode. Use NewOracle.
func New(capacity int64, p policy.Policy) *Cache {
	if capacity <= 0 {
		panic("cache: capacity must be positive")
	}
	if p == nil {
		panic("cache: policy must not be nil")
	}
	if _, isOracle := p.(policy.OraclePolicy); isOracle {
		panic("cache: " + p.Name() + " is an OraclePolicy and requires NewOracle")
	}
	return newCache(capacity, p)
}

// NewOracle builds an offline cache that feeds next-access times to p. This is
// the only constructor that enables the oracle path.
func NewOracle(capacity int64, p policy.OraclePolicy) *Cache {
	if capacity <= 0 {
		panic("cache: capacity must be positive")
	}
	if p == nil {
		panic("cache: oracle policy must not be nil")
	}
	c := newCache(capacity, p)
	c.oracle = p
	return c
}

func newCache(capacity int64, p policy.Policy) *Cache {
	return &Cache{
		data:     make(map[string]entry),
		capacity: capacity,
		pol:      p,
	}
}

// Access performs one request against an online cache and reports whether the
// key was resident (a hit). It panics on an oracle cache: see AccessOracle.
func (c *Cache) Access(key string, size int) bool {
	if c.oracle != nil {
		panic("cache: Access called on an oracle cache, use AccessOracle")
	}
	return c.access(key, size)
}

// AccessOracle performs one request against an oracle cache, handing the policy
// the trace time of the next access to key (policy.NeverAgain if there is
// none). It panics on a cache with no oracle policy, so the future can never
// leak into an online run.
func (c *Cache) AccessOracle(key string, size int, next int64) bool {
	if c.oracle == nil {
		panic("cache: AccessOracle called on a cache with no oracle policy")
	}
	c.oracle.OnNextAccess(key, next)
	return c.access(key, size)
}

func (c *Cache) access(key string, size int) bool {
	if size < 0 {
		panic("cache: negative object size")
	}

	c.stats.Requests++
	c.stats.BytesRequested += int64(size)

	if e, ok := c.data[key]; ok {
		c.stats.Hits++
		c.stats.BytesHit += int64(size)
		c.pol.OnHit(key)

		// Real traces re-request the same object id at a different size.
		// Track the newest size so byte accounting stays honest, shrinking
		// back under capacity if the object grew.
		if e.size != size {
			c.used += int64(size) - int64(e.size)
			c.data[key] = entry{size: size}
			if c.used > c.capacity {
				c.evictToFit(0)
			}
		}

		c.sampleMeta()
		return true
	}

	c.stats.Misses++
	c.stats.BytesMissed += int64(size)
	c.insert(key, size)
	c.sampleMeta()
	return false
}

// insert admits a missed object, evicting as needed. Reports whether the object
// ended up resident.
func (c *Cache) insert(key string, size int) bool {
	// An object larger than the whole cache can never be resident.
	if int64(size) > c.capacity {
		c.stats.Rejections++
		return false
	}

	// Admission decision first, before any eviction: a rejected object must
	// not cost the cache objects it already holds.
	if !c.pol.CanAdmit(key, size) {
		c.stats.Rejections++
		return false
	}

	c.evictToFit(int64(size))
	if c.used+int64(size) > c.capacity {
		c.stats.Rejections++
		return false
	}

	c.pol.OnAdmit(key, size)
	c.data[key] = entry{size: size}
	c.used += int64(size)
	c.stats.Admissions++
	return true
}

// evictToFit evicts until need additional bytes would fit. Reports whether it
// succeeded.
//
// The loop is bounded. A policy that keeps naming non-resident victims would
// otherwise spin forever, and this harness is meant to run machine-generated
// policies, so a buggy one must fail as a recorded error rather than a hang.
func (c *Cache) evictToFit(need int64) bool {
	budget := len(c.data) + 64
	for c.used+need > c.capacity {
		if budget <= 0 {
			c.stats.PolicyErrors++
			return false
		}
		budget--

		victim, ok := c.pol.Evict()
		if !ok {
			return false
		}
		if c.removeKey(victim) {
			c.stats.Evictions++
		} else {
			c.stats.PolicyErrors++
		}
	}
	return true
}

// removeKey drops a key and tells the policy. Reports whether it was resident.
// The policy is notified either way so it can shed stale bookkeeping.
func (c *Cache) removeKey(key string) bool {
	e, ok := c.data[key]
	if !ok {
		c.pol.OnRemove(key)
		return false
	}
	delete(c.data, key)
	c.used -= int64(e.size)
	c.pol.OnRemove(key)
	return true
}

// Delete removes a key explicitly, reporting whether it was resident.
func (c *Cache) Delete(key string) bool { return c.removeKey(key) }

func (c *Cache) sampleMeta() {
	if m := c.pol.MetadataBytes(); m > c.peakMeta {
		c.peakMeta = m
		c.residentAtPeakMeta = len(c.data)
	}
}

// Contains reports residency without recording a request or touching the
// policy. For assertions and tests only.
func (c *Cache) Contains(key string) bool {
	_, ok := c.data[key]
	return ok
}

// Len is the number of resident objects.
func (c *Cache) Len() int { return len(c.data) }

// Used is the resident bytes.
func (c *Cache) Used() int64 { return c.used }

// Capacity is the cache size in bytes.
func (c *Cache) Capacity() int64 { return c.capacity }

// Policy returns the underlying policy.
func (c *Cache) Policy() policy.Policy { return c.pol }

// Stats returns a copy of the counters for the current measurement window.
func (c *Cache) Stats() Stats { return c.stats }

// ResetStats restarts the measurement window, keeping cache contents. The
// replayer calls this after warmup so warmup requests are excluded from
// reported metrics.
func (c *Cache) ResetStats() {
	c.stats = Stats{}
	c.peakMeta = 0
	c.residentAtPeakMeta = 0
	c.sampleMeta()
}

// MetadataBytes is the policy's current metadata estimate.
func (c *Cache) MetadataBytes() int { return c.pol.MetadataBytes() }

// PeakMetadataBytes is the largest metadata estimate seen in the current
// measurement window.
func (c *Cache) PeakMetadataBytes() int { return c.peakMeta }

// MetadataBytesPerResidentObject is the peak metadata divided by the number of
// resident objects at the moment that peak was observed. It answers "what does
// this policy cost me per cached object", which is the form that matters when
// judging whether a policy's bookkeeping is affordable.
func (c *Cache) MetadataBytesPerResidentObject() float64 {
	if c.residentAtPeakMeta == 0 {
		return 0
	}
	return float64(c.peakMeta) / float64(c.residentAtPeakMeta)
}
