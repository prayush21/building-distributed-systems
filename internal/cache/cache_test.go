package cache

import (
	"testing"

	"github.com/prayush21/building-distributed-systems/internal/policy"
)

// fifoStub is a minimal deterministic policy used to exercise the cache. The
// real FIFO lives in internal/policy; this exists so cache tests do not depend
// on policy implementations.
type fifoStub struct {
	queue  []string
	reject map[string]bool
}

func newFIFOStub() *fifoStub { return &fifoStub{reject: map[string]bool{}} }

func (f *fifoStub) OnHit(string) {}

func (f *fifoStub) CanAdmit(key string, _ int) bool { return !f.reject[key] }

func (f *fifoStub) OnAdmit(key string, _ int) { f.queue = append(f.queue, key) }

func (f *fifoStub) Evict() (string, bool) {
	if len(f.queue) == 0 {
		return "", false
	}
	return f.queue[0], true
}

func (f *fifoStub) OnRemove(key string) {
	for i, k := range f.queue {
		if k == key {
			f.queue = append(f.queue[:i], f.queue[i+1:]...)
			return
		}
	}
}

func (f *fifoStub) Name() string       { return "fifo-stub" }
func (f *fifoStub) MetadataBytes() int { return len(f.queue) * 16 }

// TestEvictsUntilObjectFits checks the core contract: capacity is in bytes and
// insertion evicts in a loop until the new object fits, not once per insert.
func TestEvictsUntilObjectFits(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)

	for _, k := range []string{"a", "b", "c", "d"} {
		c.Access(k, 25) // fills to exactly 100
	}
	if c.Used() != 100 || c.Len() != 4 {
		t.Fatalf("setup: used=%d len=%d, want 100/4", c.Used(), c.Len())
	}

	// A 75-byte object needs three 25-byte victims evicted in one insert.
	c.Access("big", 75)

	if c.Used() != 100 {
		t.Errorf("used = %d, want 100", c.Used())
	}
	if !c.Contains("big") || !c.Contains("d") {
		t.Errorf("want big and d resident, got len=%d", c.Len())
	}
	for _, k := range []string{"a", "b", "c"} {
		if c.Contains(k) {
			t.Errorf("%q should have been evicted", k)
		}
	}
	if got := c.Stats().Evictions; got != 3 {
		t.Errorf("evictions = %d, want 3", got)
	}
}

// TestObjectLargerThanCapacityRejected: such an object can never be resident,
// so it must not empty the cache trying.
func TestObjectLargerThanCapacityRejected(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)
	c.Access("a", 50)

	if hit := c.Access("huge", 101); hit {
		t.Fatal("huge should be a miss")
	}
	if c.Contains("huge") {
		t.Error("huge must not be resident")
	}
	if !c.Contains("a") {
		t.Error("rejecting an oversized object must not evict anything")
	}
	if got := c.Stats().Evictions; got != 0 {
		t.Errorf("evictions = %d, want 0", got)
	}
	if got := c.Stats().Rejections; got != 1 {
		t.Errorf("rejections = %d, want 1", got)
	}
}

// TestCanAdmitFalseEvictsNothing is the reason CanAdmit is split from OnAdmit:
// a rejected object must not cost the cache objects it already holds.
func TestCanAdmitFalseEvictsNothing(t *testing.T) {
	p := newFIFOStub()
	p.reject["cold"] = true
	c := New(100, p)

	for _, k := range []string{"a", "b", "c", "d"} {
		c.Access(k, 25)
	}
	c.Access("cold", 25)

	if c.Contains("cold") {
		t.Error("cold was rejected by CanAdmit, must not be resident")
	}
	if c.Len() != 4 || c.Used() != 100 {
		t.Errorf("len=%d used=%d, want 4/100: rejection evicted something", c.Len(), c.Used())
	}
	if got := c.Stats().Evictions; got != 0 {
		t.Errorf("evictions = %d, want 0", got)
	}
}

// TestSizeChangeOnHit: real traces re-request an object id at a new size.
func TestSizeChangeOnHit(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)
	c.Access("a", 20)
	c.Access("b", 20)

	if hit := c.Access("a", 50); !hit {
		t.Fatal("a should hit regardless of size change")
	}
	if c.Used() != 70 {
		t.Errorf("used = %d, want 70 (50+20)", c.Used())
	}

	// Growing past capacity must shrink back under it.
	c.Access("a", 95)
	if c.Used() > 100 {
		t.Errorf("used = %d exceeds capacity 100", c.Used())
	}
}

// TestEvictNonResidentVictimIsRecordedNotHung guards the harness against
// machine-generated policies: a policy that names victims it does not hold must
// produce a recorded error, never an infinite loop.
func TestEvictNonResidentVictimIsRecordedNotHung(t *testing.T) {
	c := New(100, &ghostPolicy{})
	c.Access("a", 60)
	c.Access("b", 60) // needs eviction; ghostPolicy only names phantoms

	if got := c.Stats().PolicyErrors; got == 0 {
		t.Error("PolicyErrors = 0, want > 0 for a policy naming non-resident victims")
	}
	if c.Used() > 100 {
		t.Errorf("used = %d exceeds capacity", c.Used())
	}
}

// ghostPolicy always names a victim it does not actually hold.
type ghostPolicy struct{ n int }

func (g *ghostPolicy) OnHit(string)              {}
func (g *ghostPolicy) CanAdmit(string, int) bool { return true }
func (g *ghostPolicy) OnAdmit(string, int)       {}
func (g *ghostPolicy) Evict() (string, bool)     { g.n++; return "phantom", true }
func (g *ghostPolicy) OnRemove(string)           {}
func (g *ghostPolicy) Name() string              { return "ghost" }
func (g *ghostPolicy) MetadataBytes() int        { return 0 }

func TestResetStatsKeepsContents(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)
	c.Access("a", 25)
	c.Access("b", 25)

	c.ResetStats()

	if c.Len() != 2 {
		t.Errorf("len = %d, want 2: ResetStats must not clear contents", c.Len())
	}
	if got := c.Stats().Requests; got != 0 {
		t.Errorf("requests = %d, want 0", got)
	}
	if hit := c.Access("a", 25); !hit {
		t.Error("a should still hit after ResetStats")
	}
	s := c.Stats()
	if s.Requests != 1 || s.Hits != 1 || s.Misses != 0 {
		t.Errorf("stats = %+v, want 1 request / 1 hit", s)
	}
}

func TestMissRatios(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)
	c.Access("a", 10) // miss, 10 bytes
	c.Access("a", 10) // hit
	c.Access("b", 30) // miss, 30 bytes
	c.Access("a", 10) // hit

	s := c.Stats()
	if got, want := s.MissRatio(), 0.5; got != want {
		t.Errorf("MissRatio() = %v, want %v", got, want)
	}
	// 40 missed bytes of 60 requested.
	if got, want := s.ByteMissRatio(), 40.0/60.0; got != want {
		t.Errorf("ByteMissRatio() = %v, want %v", got, want)
	}
}

func TestMetadataAccounting(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)
	for _, k := range []string{"a", "b", "c", "d"} {
		c.Access(k, 25)
	}
	if got, want := c.PeakMetadataBytes(), 64; got != want {
		t.Errorf("PeakMetadataBytes() = %d, want %d", got, want)
	}
	if got, want := c.MetadataBytesPerResidentObject(), 16.0; got != want {
		t.Errorf("MetadataBytesPerResidentObject() = %v, want %v", got, want)
	}
}

func TestDeleteNotifiesPolicy(t *testing.T) {
	p := newFIFOStub()
	c := New(100, p)
	c.Access("a", 25)
	c.Access("b", 25)

	if !c.Delete("a") {
		t.Fatal("Delete(a) reported not resident")
	}
	if c.Contains("a") || c.Used() != 25 {
		t.Errorf("after delete: contains=%v used=%d", c.Contains("a"), c.Used())
	}
	if len(p.queue) != 1 || p.queue[0] != "b" {
		t.Errorf("policy queue = %v, want [b]: OnRemove was not honored", p.queue)
	}
	if c.Delete("missing") {
		t.Error("Delete of a non-resident key should report false")
	}
}

// --- oracle path separation ---

type oracleStub struct {
	fifoStub
	hints []int64
}

func (o *oracleStub) OnNextAccess(_ string, next int64) { o.hints = append(o.hints, next) }

func TestOraclePathIsSeparate(t *testing.T) {
	t.Run("New rejects an OraclePolicy", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("New with an OraclePolicy should panic")
			}
		}()
		New(100, &oracleStub{fifoStub: *newFIFOStub()})
	})

	t.Run("Access rejected on an oracle cache", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("Access on an oracle cache should panic")
			}
		}()
		c := NewOracle(100, &oracleStub{fifoStub: *newFIFOStub()})
		c.Access("a", 10)
	})

	t.Run("AccessOracle rejected on an online cache", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("AccessOracle on an online cache should panic")
			}
		}()
		c := New(100, newFIFOStub())
		c.AccessOracle("a", 10, 5)
	})

	t.Run("hints reach only the oracle policy", func(t *testing.T) {
		o := &oracleStub{fifoStub: *newFIFOStub()}
		c := NewOracle(100, o)
		c.AccessOracle("a", 10, 42)
		c.AccessOracle("a", 10, policy.NeverAgain)
		if len(o.hints) != 2 || o.hints[0] != 42 || o.hints[1] != policy.NeverAgain {
			t.Errorf("hints = %v, want [42 NeverAgain]", o.hints)
		}
	})
}

// TestDeterministicAcrossRuns is a smoke check that identical inputs produce
// identical cache state. The full byte-identical-output check lives with the
// replayer.
func TestDeterministicAcrossRuns(t *testing.T) {
	run := func() Stats {
		c := New(500, newFIFOStub())
		for i := 0; i < 2000; i++ {
			key := string(rune('a' + i%37))
			c.Access(key, 10+i%53)
		}
		return c.Stats()
	}
	if a, b := run(), run(); a != b {
		t.Errorf("runs diverged:\n a = %+v\n b = %+v", a, b)
	}
}
