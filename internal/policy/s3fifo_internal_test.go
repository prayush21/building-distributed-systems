package policy

import "testing"

// These tests drive S3-FIFO's interface directly rather than through a Cache,
// because the behavior that matters is which of the three queues an object
// lands in, and that is not observable from outside the package.

func newTestS3FIFO(t *testing.T, cacheSize int64) *S3FIFO {
	t.Helper()
	s, err := NewS3FIFO(Config{CacheSize: cacheSize})
	if err != nil {
		t.Fatalf("NewS3FIFO: %v", err)
	}
	return s
}

// where reports which queue holds key.
func (s *S3FIFO) where(key string) string {
	switch {
	case s.small.items[key] != nil:
		return "small"
	case s.main.items[key] != nil:
		return "main"
	case s.ghost.items[key] != nil:
		return "ghost"
	}
	return ""
}

// admit performs the miss path the cache would drive: CanAdmit then OnAdmit.
func (s *S3FIFO) admit(t *testing.T, key string, size int) bool {
	t.Helper()
	if !s.CanAdmit(key, size) {
		return false
	}
	s.OnAdmit(key, size)
	return true
}

func TestS3FIFODefaultsMatchLibCacheSim(t *testing.T) {
	s := newTestS3FIFO(t, 1000)
	if s.smallRatio != 0.10 || s.ghostRatio != 0.90 || s.moveToMain != 2 {
		t.Errorf("defaults = %v/%v/%d, want 0.10/0.90/2 to match libCacheSim",
			s.smallRatio, s.ghostRatio, s.moveToMain)
	}
	if s.small.capacity != 100 || s.main.capacity != 900 || s.ghost.capacity != 900 {
		t.Errorf("queue capacities = %d/%d/%d, want 100/900/900",
			s.small.capacity, s.main.capacity, s.ghost.capacity)
	}
}

// TestS3FIFOFirstAdmissionsGoToSmall covers the split, including libCacheSim's
// rule that once the small queue is full but the cache is not, admissions go to
// main instead of pushing objects out of small early.
func TestS3FIFOFirstAdmissionsGoToSmall(t *testing.T) {
	s := newTestS3FIFO(t, 20) // small=2, main=18, ghost=18

	s.admit(t, "a", 1)
	s.admit(t, "b", 1) // small now holds 2 bytes, its whole capacity
	s.admit(t, "c", 1)

	if got := s.where("a"); got != "small" {
		t.Errorf("a is in %q, want small", got)
	}
	if got := s.where("b"); got != "small" {
		t.Errorf("b is in %q, want small", got)
	}
	if got := s.where("c"); got != "main" {
		t.Errorf("c is in %q, want main: with small full and the cache not yet "+
			"evicting, admissions go to main", got)
	}
}

// TestS3FIFOGhostPromotesToMain is the mechanism the ghost queue exists for. An
// object evicted from the small queue leaves its key behind; if it is requested
// again it skips the small queue entirely.
func TestS3FIFOGhostPromotesToMain(t *testing.T) {
	s := newTestS3FIFO(t, 20) // small=2, main=18, ghost=18

	s.admit(t, "a", 1)
	s.admit(t, "b", 1)
	for _, k := range []string{"c", "d", "e", "f", "g", "h"} {
		s.admit(t, k, 1)
	}

	// a was never re-accessed, so evicting from small demotes it to the ghost.
	victim, ok := s.Evict()
	if !ok || victim != "a" {
		t.Fatalf("Evict() = %q, %v; want a evicted from the head of small", victim, ok)
	}
	s.OnRemove(victim)

	if got := s.where("a"); got != "ghost" {
		t.Fatalf("after eviction a is in %q, want ghost", got)
	}

	// Requesting a again is still a miss, but the ghost hit routes it to main.
	if !s.admit(t, "a", 1) {
		t.Fatal("a should be admissible on its ghost hit")
	}
	if got := s.where("a"); got != "main" {
		t.Errorf("a is in %q, want main: a ghost hit skips the small queue", got)
	}
}

// TestS3FIFOPromotesReaccessedObject: an object accessed enough times before the
// small queue drains is promoted rather than discarded, and the sweep continues
// to find a real victim.
func TestS3FIFOPromotesReaccessedObject(t *testing.T) {
	s := newTestS3FIFO(t, 20)

	s.admit(t, "a", 1)
	s.admit(t, "b", 1)
	// a reaches the move-to-main threshold of 2.
	s.OnHit("a")
	s.OnHit("a")

	victim, ok := s.Evict()
	if !ok {
		t.Fatal("Evict reported nothing to evict")
	}
	if victim == "a" {
		t.Fatal("a met the promotion threshold and must not be the victim")
	}
	if victim != "b" {
		t.Errorf("victim = %q, want b", victim)
	}
	if got := s.where("a"); got != "main" {
		t.Errorf("a is in %q, want main after promotion", got)
	}
	if got := s.where("b"); got != "ghost" {
		t.Errorf("b is in %q, want ghost", got)
	}
}

// TestS3FIFOMainQueueClock covers the 2-bit clock: an object in main with a
// non-zero counter is reinserted at the tail with the counter decremented
// instead of being evicted.
func TestS3FIFOMainQueueClock(t *testing.T) {
	s := newTestS3FIFO(t, 20)

	// Put x and y straight into main via ghost hits, then make x hot.
	for _, k := range []string{"x", "y"} {
		s.ghostAdd(k, 1)
		s.admit(t, k, 1)
		if got := s.where(k); got != "main" {
			t.Fatalf("%s is in %q, want main", k, got)
		}
	}
	s.OnHit("x")

	// Force the main queue to be the eviction target by emptying small.
	victim, ok := s.evictMain()
	if !ok {
		t.Fatal("evictMain reported nothing to evict")
	}
	if victim != "y" {
		t.Errorf("victim = %q, want y: x had a non-zero counter and is reinserted", victim)
	}
	if got := s.where("x"); got != "main" {
		t.Errorf("x is in %q, want main", got)
	}
	if n := s.main.items["x"]; n == nil || n.freq != 0 {
		t.Errorf("x freq = %v, want 0 after one clock decrement", n)
	}
}

// TestS3FIFORejectsObjectAsLargeAsSmallQueue pins libCacheSim's refusal to admit
// an object that would fill the small queue outright.
//
// This has a practical consequence worth stating: under --ignore-obj-size every
// object has size 1, so a cache smaller than 20 objects gives a small queue of
// at most 1 and admits nothing through it.
func TestS3FIFORejectsObjectAsLargeAsSmallQueue(t *testing.T) {
	s := newTestS3FIFO(t, 100) // small=10

	if s.CanAdmit("big", 10) {
		t.Error("an object exactly the size of the small queue must be refused")
	}
	if !s.CanAdmit("ok", 9) {
		t.Error("an object smaller than the small queue must be admissible")
	}
	if s.CanAdmit("huge", 11) {
		t.Error("an object larger than the small queue must be refused")
	}

	// A ghost hit routes to main, which is large enough to take it.
	s.ghostAdd("big", 10)
	if !s.CanAdmit("big", 10) {
		t.Error("on a ghost hit the object goes to main and should be admissible")
	}
}

func TestS3FIFORejectsUnsplittableCacheAndBadParams(t *testing.T) {
	if _, err := NewS3FIFO(Config{CacheSize: 5}); err == nil {
		t.Error("a cache too small to split should be a clear error, not a silent misconfiguration")
	}
	for _, params := range []string{
		"small-size-ratio=0",
		"small-size-ratio=1",
		"small-size-ratio=1.5",
		"ghost-size-ratio=-1",
		"typo-ratio=0.1",
		"small-size-ratio=abc",
	} {
		if _, err := NewS3FIFO(Config{CacheSize: 1000, Params: params}); err == nil {
			t.Errorf("params %q should be rejected", params)
		}
	}
}

func TestS3FIFOParamsAreReported(t *testing.T) {
	s, err := NewS3FIFO(Config{CacheSize: 1000, Params: "small-size-ratio=0.2,move-to-main-threshold=3"})
	if err != nil {
		t.Fatalf("NewS3FIFO: %v", err)
	}
	const want = "small-size-ratio=0.2,ghost-size-ratio=0.9,move-to-main-threshold=3"
	if got := s.Params(); got != want {
		t.Errorf("Params() = %q, want %q", got, want)
	}
	if s.small.capacity != 200 || s.main.capacity != 800 {
		t.Errorf("capacities = %d/%d, want 200/800", s.small.capacity, s.main.capacity)
	}
}
