package policy

// LFU evicts the least frequently used object, breaking ties in FIFO order
// within a frequency.
//
// The tie-break is not incidental. Frequency ties are the common case, not the
// exception -- at any moment most objects share a handful of low frequencies --
// so the rule for choosing among them determines the miss ratio as much as the
// frequency ordering does. This implementation matches libCacheSim: every
// frequency owns a FIFO list, an object joins the tail of its new frequency's
// list whenever its count changes, and eviction takes the head of the lowest
// non-empty list. That is "oldest arrival at the minimum frequency", not
// "oldest overall".
type LFU struct {
	items    map[string]*lfuNode
	buckets  map[int64]*lfuBucket
	minFreq  int64
	maxFreq  int64
	keyBytes int
}

type lfuNode struct {
	key          string
	freq         int64
	older, newer *lfuNode // within its frequency bucket; older is the head
}

// lfuBucket is the FIFO list of every object sharing one frequency.
type lfuBucket struct {
	head, tail *lfuNode
	n          int
}

func init() {
	Register("lfu", func(Config) (Policy, error) { return NewLFU(), nil })
}

// NewLFU returns an empty LFU policy.
func NewLFU() *LFU {
	return &LFU{
		items:   make(map[string]*lfuNode),
		buckets: make(map[int64]*lfuBucket),
		minFreq: 1,
		maxFreq: 1,
	}
}

func (l *LFU) Name() string { return "lfu" }

func (l *LFU) OnHit(key string) {
	n, ok := l.items[key]
	if !ok {
		return
	}
	old := n.freq
	l.unlinkFrom(old, n)
	n.freq++
	l.linkInto(n.freq, n)

	if n.freq > l.maxFreq {
		l.maxFreq = n.freq
	}
	// Promoting the last object out of the minimum frequency raises the
	// minimum. Recomputed by scanning upward rather than by ranging the
	// bucket map, which would make eviction order depend on map order.
	if old == l.minFreq && l.bucketEmpty(old) {
		l.raiseMinFreq()
	}
}

func (l *LFU) CanAdmit(string, int) bool { return true }

func (l *LFU) OnAdmit(key string, _ int) {
	if _, ok := l.items[key]; ok {
		return
	}
	n := &lfuNode{key: key, freq: 1}
	l.items[key] = n
	l.keyBytes += len(key)
	l.linkInto(1, n)
	// A fresh object always sits at the bottom, so the minimum is 1 again.
	l.minFreq = 1
}

func (l *LFU) Evict() (string, bool) {
	if len(l.items) == 0 {
		return "", false
	}
	l.raiseMinFreq()
	b := l.buckets[l.minFreq]
	if b == nil || b.head == nil {
		return "", false
	}
	// Head of the lowest non-empty frequency: least frequent, and among
	// those, the one that has been at this frequency longest.
	return b.head.key, true
}

func (l *LFU) OnRemove(key string) {
	n, ok := l.items[key]
	if !ok {
		return
	}
	l.unlinkFrom(n.freq, n)
	delete(l.items, key)
	l.keyBytes -= len(key)
}

func (l *LFU) MetadataBytes() int {
	const perNode = StringHeaderBytes + 8 + 2*PointerBytes // key, freq, links
	perEntry := StringHeaderBytes + PointerBytes + MapEntryBytes + perNode
	const perBucket = 8 + 2*PointerBytes + 8 + MapEntryBytes
	return len(l.items)*perEntry + 2*l.keyBytes + len(l.buckets)*perBucket
}

func (l *LFU) bucketEmpty(freq int64) bool {
	b := l.buckets[freq]
	return b == nil || b.n == 0
}

// raiseMinFreq walks up from the current minimum to the first non-empty
// frequency. Scanning integers keeps eviction independent of map iteration
// order; the walk is bounded by maxFreq and amortizes away because minFreq
// resets to 1 on every admission.
func (l *LFU) raiseMinFreq() {
	if l.minFreq < 1 {
		l.minFreq = 1
	}
	for f := l.minFreq; f <= l.maxFreq; f++ {
		if !l.bucketEmpty(f) {
			l.minFreq = f
			return
		}
	}
	l.minFreq = 1
}

func (l *LFU) linkInto(freq int64, n *lfuNode) {
	b := l.buckets[freq]
	if b == nil {
		b = &lfuBucket{}
		l.buckets[freq] = b
	}
	// Append at the tail: within a frequency, order is arrival order.
	n.older, n.newer = b.tail, nil
	if b.tail != nil {
		b.tail.newer = n
	}
	b.tail = n
	if b.head == nil {
		b.head = n
	}
	b.n++
}

func (l *LFU) unlinkFrom(freq int64, n *lfuNode) {
	b := l.buckets[freq]
	if b == nil {
		return
	}
	if n.older != nil {
		n.older.newer = n.newer
	} else {
		b.head = n.newer
	}
	if n.newer != nil {
		n.newer.older = n.older
	} else {
		b.tail = n.older
	}
	n.older, n.newer = nil, nil
	b.n--
	// Frequency 1 is kept because admissions repopulate it constantly.
	if b.n == 0 && freq != 1 {
		delete(l.buckets, freq)
	}
}
