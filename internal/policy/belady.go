package policy

import "container/heap"

// Belady evicts the object whose next access is furthest in the future.
//
// It is an offline baseline, not a policy: it reaches the future only through
// OraclePolicy.OnNextAccess, and only a cache built by cache.NewOracle will
// call it. Its value is as a ceiling -- reporting "closed X% of the gap between
// LRU and OPT" is a far more defensible claim than a raw percentage over LRU,
// because the gap states how much headroom the workload actually had.
//
// Two caveats worth stating plainly:
//
// First, Belady is provably optimal only when every object is the same size.
// With variable sizes, optimal offline caching is NP-hard, so this is a strong
// heuristic upper bound rather than a true optimum. Under --ignore-obj-size it
// is genuinely optimal.
//
// Second, it matches libCacheSim in admitting every object, including one that
// is never requested again. Evicting a useful object to admit a useless one is
// not what a strictly optimal policy would do, but diverging here would make
// the cross-check compare two different algorithms.
type Belady struct {
	items map[string]*beladyItem
	h     beladyHeap
	seq   uint64

	// pending holds the hint for the request being processed, delivered by
	// OnNextAccess just before OnHit or CanAdmit/OnAdmit for the same key.
	pendingKey  string
	pendingNext int64
	havePending bool

	keyBytes int
}

type beladyItem struct {
	key   string
	next  int64
	seq   uint64
	index int
}

func init() {
	RegisterOracle("belady", func(Config) (OraclePolicy, error) { return NewBelady(), nil })
	RegisterOracle("opt", func(Config) (OraclePolicy, error) { return NewBelady(), nil })
}

// NewBelady returns an empty Belady baseline.
func NewBelady() *Belady {
	return &Belady{items: make(map[string]*beladyItem)}
}

func (b *Belady) Name() string { return "belady" }

// OnNextAccess receives the trace time of the next access to key. It only
// records the hint; it is applied when the cache reports the hit or admission,
// so a rejected admission leaves no trace in the policy.
func (b *Belady) OnNextAccess(key string, next int64) {
	b.pendingKey, b.pendingNext, b.havePending = key, next, true
}

func (b *Belady) OnHit(key string) {
	if b.havePending && b.pendingKey == key {
		b.setNext(key, b.pendingNext)
	}
}

func (b *Belady) CanAdmit(string, int) bool { return true }

func (b *Belady) OnAdmit(key string, _ int) {
	next := NeverAgain
	if b.havePending && b.pendingKey == key {
		next = b.pendingNext
	}
	b.setNext(key, next)
}

// Evict peeks at the furthest-future object. It does not remove it; the cache
// does that through OnRemove.
func (b *Belady) Evict() (string, bool) {
	if len(b.h) == 0 {
		return "", false
	}
	return b.h[0].key, true
}

func (b *Belady) OnRemove(key string) {
	it, ok := b.items[key]
	if !ok {
		return
	}
	heap.Remove(&b.h, it.index)
	delete(b.items, key)
	b.keyBytes -= len(key)
}

func (b *Belady) MetadataBytes() int {
	// One heap slot and one map entry per resident object, holding a next
	// access time and a sequence number.
	const perItem = StringHeaderBytes + 8 + 8 + 8 // key, next, seq, index
	perEntry := StringHeaderBytes + PointerBytes + MapEntryBytes + perItem + PointerBytes
	return len(b.items)*perEntry + 2*b.keyBytes
}

// setNext updates an object's next-access time in place, keeping exactly one
// heap slot per object. Pushing a fresh entry per access and filtering stale
// ones on pop would be simpler, but the heap would then grow with the number of
// requests instead of the number of resident objects.
func (b *Belady) setNext(key string, next int64) {
	if it, ok := b.items[key]; ok {
		it.next = next
		heap.Fix(&b.h, it.index)
		return
	}
	it := &beladyItem{key: key, next: next, seq: b.seq}
	b.seq++
	b.items[key] = it
	b.keyBytes += len(key)
	heap.Push(&b.h, it)
}

// beladyHeap orders by next access descending, so the root is the object the
// trace needs last.
type beladyHeap []*beladyItem

func (h beladyHeap) Len() int { return len(h) }

func (h beladyHeap) Less(i, j int) bool {
	if h[i].next != h[j].next {
		return h[i].next > h[j].next
	}
	// Objects never requested again all share NeverAgain, so ties are the
	// common case and need a deterministic rule. The earliest admitted goes
	// first, which keeps eviction order independent of heap layout.
	return h[i].seq < h[j].seq
}

func (h beladyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *beladyHeap) Push(x any) {
	it := x.(*beladyItem)
	it.index = len(*h)
	*h = append(*h, it)
}

func (h *beladyHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}
