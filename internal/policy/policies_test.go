// Package policy_test exercises the policies through the same Cache the
// replayer uses, so tests measure the behavior that actually gets scored rather
// than the policy in isolation.
package policy_test

import (
	"strings"
	"testing"

	"github.com/prayush21/building-distributed-systems/internal/cache"
	"github.com/prayush21/building-distributed-systems/internal/policy"
)

// sharedTrace is the reference workload, 20 requests over five objects.
//
//	pos: 1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20
//	key: A  B  C  A  B  D  A  B  E  A  B  C  A  B  D  A  B  E  A  B
//
// A and B are hot; C, D and E each appear twice, far apart. With room for three
// objects, a policy that reorders on access keeps A and B resident and pays only
// for the cold objects, while FIFO keeps flushing A and B out behind them.
var sharedTrace = strings.Fields("A B C A B D A B E A B C A B D A B E A B")

// replay drives keys of one byte each through a cache holding capacity objects,
// returning a per-request "H"/"M" string.
func replay(p policy.Policy, capacity int64, keys []string) string {
	c := cache.New(capacity, p)
	var sb strings.Builder
	for _, k := range keys {
		if c.Access(k, 1) {
			sb.WriteByte('H')
		} else {
			sb.WriteByte('M')
		}
	}
	return sb.String()
}

// replayOracle is the same for an offline policy, computing next-access times
// the way the replayer does.
func replayOracle(p policy.OraclePolicy, capacity int64, keys []string) string {
	next := make([]int64, len(keys))
	last := map[string]int64{}
	for i := len(keys) - 1; i >= 0; i-- {
		if n, ok := last[keys[i]]; ok {
			next[i] = n
		} else {
			next[i] = policy.NeverAgain
		}
		last[keys[i]] = int64(i)
	}

	c := cache.NewOracle(capacity, p)
	var sb strings.Builder
	for i, k := range keys {
		if c.AccessOracle(k, 1, next[i]) {
			sb.WriteByte('H')
		} else {
			sb.WriteByte('M')
		}
	}
	return sb.String()
}

// TestHandComputedSharedTrace pins the exact hit/miss sequence each policy
// produces on sharedTrace with room for three objects. Every expectation below
// was worked out by hand from the algorithm, not recorded from a run.
func TestHandComputedSharedTrace(t *testing.T) {
	// FIFO ignores hits entirely, so A and B are evicted on schedule behind
	// C, D and E and must be re-fetched every round. Only the two accesses
	// immediately after a fill survive:
	//
	//   1-3   A B C fill the cache
	//   4,5   A B hit, but FIFO does not reorder them
	//   6     D evicts A (oldest)
	//   7     A evicts B, 8 B evicts C, 9 E evicts D
	//   10,11 A B hit again, and the cycle repeats
	const fifoWant = "MMMHHMMMMHHMMMMHHMMM"

	// LRU, LFU, SIEVE and Belady all keep A and B resident for different
	// reasons: recency, frequency, the visited bit, and lookahead. Each pays
	// only the six cold misses at 3, 6, 9, 12, 15, 18 plus the two compulsory
	// ones at 1 and 2.
	const hotSetWant = "MMMHHMHHMHHMHHMHHMHH"

	tests := []struct {
		name string
		pol  policy.Policy
		want string
	}{
		{"fifo", policy.NewFIFO(), fifoWant},
		{"lru", policy.NewLRU(), hotSetWant},
		{"lfu", policy.NewLFU(), hotSetWant},
		{"sieve", policy.NewSIEVE(), hotSetWant},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := replay(tc.pol, 3, sharedTrace)
			if got != tc.want {
				t.Errorf("miss sequence mismatch\n got: %s\nwant: %s\n      %s",
					got, tc.want, caretDiff(got, tc.want))
			}
		})
	}

	t.Run("belady", func(t *testing.T) {
		got := replayOracle(policy.NewBelady(), 3, sharedTrace)
		if got != hotSetWant {
			t.Errorf("miss sequence mismatch\n got: %s\nwant: %s\n      %s",
				got, hotSetWant, caretDiff(got, hotSetWant))
		}
	})
}

// caretDiff marks the first differing position, so a failure points at the
// request that diverged rather than making you count characters.
func caretDiff(got, want string) string {
	n := len(got)
	if len(want) < n {
		n = len(want)
	}
	for i := 0; i < n; i++ {
		if got[i] != want[i] {
			return strings.Repeat(" ", i) + "^ first difference at request " + itoa(i+1)
		}
	}
	if len(got) != len(want) {
		return "(length differs)"
	}
	return ""
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestFIFOIgnoresAccesses is FIFO's defining property, stated directly.
func TestFIFOIgnoresAccesses(t *testing.T) {
	c := cache.New(3, policy.NewFIFO())
	for _, k := range []string{"a", "b", "c"} {
		c.Access(k, 1)
	}
	// Hammer the oldest object; FIFO must not care.
	for i := 0; i < 5; i++ {
		c.Access("a", 1)
	}
	c.Access("d", 1)

	if c.Contains("a") {
		t.Error("FIFO evicts in insertion order, so a must go despite being hot")
	}
	if !c.Contains("b") || !c.Contains("c") || !c.Contains("d") {
		t.Error("b, c and d should all be resident")
	}
}

// TestLRUEvictsLeastRecent contrasts with the FIFO case above on the same input.
func TestLRUEvictsLeastRecent(t *testing.T) {
	c := cache.New(3, policy.NewLRU())
	for _, k := range []string{"a", "b", "c"} {
		c.Access(k, 1)
	}
	c.Access("a", 1) // a is now the most recent, b the least
	c.Access("d", 1)

	if !c.Contains("a") {
		t.Error("a was just accessed and must survive")
	}
	if c.Contains("b") {
		t.Error("b was least recently used and must be evicted")
	}
}

// TestLFUKeepsFrequentObject is the case that separates LFU from LRU: an object
// with a long history survives even when it is the least recently used.
func TestLFUKeepsFrequentObject(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pol       policy.Policy
		keepsHotA bool
	}{
		{"lfu", policy.NewLFU(), true},
		{"lru", policy.NewLRU(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cache.New(2, tc.pol)
			c.Access("a", 1)
			c.Access("a", 1)
			c.Access("a", 1) // a reaches frequency 3
			c.Access("b", 1) // b is frequency 1 but more recent
			c.Access("c", 1) // forces one eviction

			if got := c.Contains("a"); got != tc.keepsHotA {
				t.Errorf("Contains(a) = %v, want %v", got, tc.keepsHotA)
			}
		})
	}
}

// TestLFUBreaksTiesFIFO pins the tie-break that libCacheSim uses. Frequency ties
// are the common case, so this rule matters as much as the frequency ordering.
func TestLFUBreaksTiesFIFO(t *testing.T) {
	c := cache.New(2, policy.NewLFU())
	c.Access("a", 1)
	c.Access("b", 1)
	c.Access("a", 1) // a reaches frequency 2 first
	c.Access("b", 1) // b reaches frequency 2 second
	c.Access("c", 1) // both are at frequency 2; the older arrival goes

	if c.Contains("a") {
		t.Error("a arrived at frequency 2 first, so it must be evicted first")
	}
	if !c.Contains("b") || !c.Contains("c") {
		t.Error("b and c should be resident")
	}
}

// TestSIEVEHandDoesNotReset is SIEVE's distinguishing mechanism. The hand keeps
// its position between evictions instead of restarting at the oldest object, and
// an object whose visited bit was set survives the sweep that passes it.
//
// Fill a b c d, touch c, then force three evictions:
//
//	evict 1: hand starts at the tail, a is unvisited -> evict a, hand rests on b
//	evict 2: hand is already at b, unvisited      -> evict b, hand rests on c
//	evict 3: hand is at c, visited -> clear it and step to d, which is
//	         unvisited -> evict d
//
// So c outlives d despite being older, and the order is a, b, d.
func TestSIEVEHandDoesNotReset(t *testing.T) {
	c := cache.New(4, policy.NewSIEVE())
	for _, k := range []string{"a", "b", "c", "d"} {
		c.Access(k, 1)
	}
	c.Access("c", 1) // sets c's visited bit

	var evicted []string
	for _, k := range []string{"e", "f", "g"} {
		before := residents(c, "a", "b", "c", "d")
		c.Access(k, 1)
		after := residents(c, "a", "b", "c", "d")
		evicted = append(evicted, difference(before, after)...)
	}

	got := strings.Join(evicted, ",")
	const want = "a,b,d"
	if got != want {
		t.Errorf("eviction order = %q, want %q: the hand must not restart at the tail", got, want)
	}
	if !c.Contains("c") {
		t.Error("c had its visited bit set and must survive the sweep that passed it")
	}
}

func residents(c *cache.Cache, keys ...string) []string {
	var out []string
	for _, k := range keys {
		if c.Contains(k) {
			out = append(out, k)
		}
	}
	return out
}

func difference(before, after []string) []string {
	in := map[string]bool{}
	for _, k := range after {
		in[k] = true
	}
	var out []string
	for _, k := range before {
		if !in[k] {
			out = append(out, k)
		}
	}
	return out
}

// TestBeladyBeatsLRUOnScan uses the workload LRU is worst on: a cyclic scan one
// object larger than the cache. LRU evicts exactly the object needed next and
// never registers a hit, while lookahead keeps most of the working set.
func TestBeladyBeatsLRUOnScan(t *testing.T) {
	var scan []string
	for i := 0; i < 5; i++ {
		scan = append(scan, "a", "b", "c", "d")
	}

	lru := replay(policy.NewLRU(), 3, scan)
	if strings.Contains(lru, "H") {
		t.Errorf("LRU on a cyclic scan should never hit, got %s", lru)
	}

	opt := replayOracle(policy.NewBelady(), 3, scan)
	hits := strings.Count(opt, "H")
	if hits < 8 {
		t.Errorf("Belady got %d hits on the scan (%s), expected at least 8", hits, opt)
	}
}

// TestBeladyEvictsFurthestFuture covers the eviction rule and the one place
// this baseline knowingly departs from true optimality.
func TestBeladyEvictsFurthestFuture(t *testing.T) {
	t.Run("evicts the furthest-future resident", func(t *testing.T) {
		// a is not needed until 10, b is needed at 4. Admitting c must
		// displace a, the object the trace needs last.
		c := cache.NewOracle(2, policy.NewBelady())
		c.AccessOracle("a", 1, 10)
		c.AccessOracle("b", 1, 4)
		c.AccessOracle("c", 1, 5)

		if c.Contains("a") {
			t.Error("a is needed last and must be evicted")
		}
		if !c.Contains("b") || !c.Contains("c") {
			t.Error("b and c are needed sooner and must be resident")
		}
	})

	t.Run("admits an object needed later than every resident", func(t *testing.T) {
		// a is needed at 3, b at 4, c not until 5. A strictly optimal policy
		// would decline c and keep both residents. This one admits it and
		// evicts b, because the object being admitted is not yet resident and
		// so cannot be chosen as its own victim.
		//
		// That is libCacheSim's behavior, and matching it is deliberate: the
		// cross-check in harness/VALIDATION.md has to compare the same
		// algorithm, not merely the same name. It also means the reported OPT
		// is a strong upper bound rather than a proven optimum, which is why
		// Belady is documented as a ceiling and not as a policy.
		c := cache.NewOracle(2, policy.NewBelady())
		c.AccessOracle("a", 1, 3)
		c.AccessOracle("b", 1, 4)
		c.AccessOracle("c", 1, 5)

		if !c.Contains("c") {
			t.Error("c is admitted even though it is needed last")
		}
		if c.Contains("b") {
			t.Error("b was the furthest-future resident and must be evicted")
		}
		if !c.Contains("a") {
			t.Error("a is needed soonest and must survive")
		}
	})
}

// TestPoliciesNeverViolateTheirContract runs every registered policy through a
// varied workload and fails on any contract violation the cache detects, such as
// naming a victim it does not hold.
func TestPoliciesNeverViolateTheirContract(t *testing.T) {
	for _, name := range policy.Names() {
		t.Run(name, func(t *testing.T) {
			p, err := policy.New(name, policy.Config{CacheSize: 4096, Seed: 1})
			if err != nil {
				t.Fatalf("New(%q): %v", name, err)
			}
			c := cache.New(4096, p)
			for i := 0; i < 20000; i++ {
				key := string(rune('a' + i%53))
				c.Access(key, 1+i%97)
			}
			if got := c.Stats().PolicyErrors; got != 0 {
				t.Errorf("PolicyErrors = %d, want 0", got)
			}
			if c.Used() > c.Capacity() {
				t.Errorf("used %d exceeds capacity %d", c.Used(), c.Capacity())
			}
		})
	}
}

// TestPoliciesAreDeterministic guards the property the whole harness rests on.
func TestPoliciesAreDeterministic(t *testing.T) {
	keys := make([]string, 5000)
	for i := range keys {
		keys[i] = string(rune('a' + (i*7)%61))
	}

	for _, name := range policy.Names() {
		t.Run(name, func(t *testing.T) {
			var first string
			for run := 0; run < 5; run++ {
				p, err := policy.New(name, policy.Config{CacheSize: 64, Seed: 1})
				if err != nil {
					t.Fatalf("New(%q): %v", name, err)
				}
				got := replay(p, 64, keys)
				if run == 0 {
					first = got
					continue
				}
				if got != first {
					t.Fatalf("run %d diverged from run 0", run)
				}
			}
		})
	}

	t.Run("belady", func(t *testing.T) {
		first := replayOracle(policy.NewBelady(), 64, keys)
		for run := 1; run < 5; run++ {
			if got := replayOracle(policy.NewBelady(), 64, keys); got != first {
				t.Fatalf("run %d diverged from run 0", run)
			}
		}
	})
}

// TestOraclePoliciesAreNotReachableOnline is the compile-time separation checked
// at runtime: Belady must be unreachable through the online registry.
func TestOraclePoliciesAreNotReachableOnline(t *testing.T) {
	if _, err := policy.New("belady", policy.Config{CacheSize: 100}); err == nil {
		t.Error("policy.New(\"belady\") should fail without the oracle path")
	}
	for _, name := range policy.Names() {
		if policy.IsOracle(name) {
			t.Errorf("%q appears in the online registry but reports as an oracle", name)
		}
	}
}
