package policy

// FIFO evicts in insertion order, ignoring accesses entirely.
//
// Reinsertion is what separates it from LRU: a hit does nothing at all, so an
// object's position is fixed the moment it is admitted. It is the reference
// point the bench report measures other policies against, since absolute miss
// ratios vary too much across traces to compare directly.
type FIFO struct {
	items      map[string]*fifoNode
	head, tail *fifoNode // head is the oldest, and the eviction end
	keyBytes   int
}

type fifoNode struct {
	key          string
	older, newer *fifoNode
}

func init() {
	Register("fifo", func(Config) (Policy, error) { return NewFIFO(), nil })
}

// NewFIFO returns an empty FIFO policy.
func NewFIFO() *FIFO { return &FIFO{items: make(map[string]*fifoNode)} }

func (f *FIFO) Name() string { return "fifo" }

// OnHit is deliberately empty: FIFO does not reorder on access.
func (f *FIFO) OnHit(string) {}

func (f *FIFO) CanAdmit(string, int) bool { return true }

func (f *FIFO) OnAdmit(key string, _ int) {
	if _, ok := f.items[key]; ok {
		return
	}
	n := &fifoNode{key: key}
	f.items[key] = n
	f.keyBytes += len(key)

	// Append at the tail; the head is the oldest and leaves first.
	n.older = f.tail
	if f.tail != nil {
		f.tail.newer = n
	}
	f.tail = n
	if f.head == nil {
		f.head = n
	}
}

func (f *FIFO) Evict() (string, bool) {
	if f.head == nil {
		return "", false
	}
	return f.head.key, true
}

func (f *FIFO) OnRemove(key string) {
	n, ok := f.items[key]
	if !ok {
		return
	}
	if n.older != nil {
		n.older.newer = n.newer
	} else {
		f.head = n.newer
	}
	if n.newer != nil {
		n.newer.older = n.older
	} else {
		f.tail = n.older
	}
	delete(f.items, key)
	f.keyBytes -= len(key)
}

func (f *FIFO) MetadataBytes() int {
	const perNode = StringHeaderBytes + 2*PointerBytes
	perEntry := StringHeaderBytes + PointerBytes + MapEntryBytes + perNode
	return len(f.items)*perEntry + 2*f.keyBytes
}
