package policy

// LRU evicts the least recently used object.
//
// Implemented as an intrusive doubly-linked list keyed by a map, so OnHit,
// OnAdmit, Evict and OnRemove are all O(1) and none of them iterates a map.
type LRU struct {
	items map[string]*lruNode
	head  *lruNode // most recently used
	tail  *lruNode // least recently used, the eviction end

	keyBytes int
}

type lruNode struct {
	key        string
	prev, next *lruNode
}

func init() {
	Register("lru", func(Config) (Policy, error) { return NewLRU(), nil })
}

// NewLRU returns an empty LRU policy.
func NewLRU() *LRU {
	return &LRU{items: make(map[string]*lruNode)}
}

func (l *LRU) Name() string { return "lru" }

func (l *LRU) OnHit(key string) {
	if n, ok := l.items[key]; ok {
		l.moveToHead(n)
	}
}

func (l *LRU) CanAdmit(string, int) bool { return true }

func (l *LRU) OnAdmit(key string, _ int) {
	if n, ok := l.items[key]; ok {
		l.moveToHead(n)
		return
	}
	n := &lruNode{key: key}
	l.items[key] = n
	l.keyBytes += len(key)
	l.pushHead(n)
}

func (l *LRU) Evict() (string, bool) {
	if l.tail == nil {
		return "", false
	}
	return l.tail.key, true
}

func (l *LRU) OnRemove(key string) {
	n, ok := l.items[key]
	if !ok {
		return
	}
	l.unlink(n)
	delete(l.items, key)
	l.keyBytes -= len(key)
}

func (l *LRU) MetadataBytes() int {
	// Per object: the map entry (string header + key bytes + pointer value +
	// map overhead) and the node it points at (key header + two pointers).
	const perNode = StringHeaderBytes + 2*PointerBytes
	perEntry := StringHeaderBytes + PointerBytes + MapEntryBytes + perNode
	return len(l.items)*perEntry + 2*l.keyBytes
}

func (l *LRU) pushHead(n *lruNode) {
	n.prev, n.next = nil, l.head
	if l.head != nil {
		l.head.prev = n
	}
	l.head = n
	if l.tail == nil {
		l.tail = n
	}
}

func (l *LRU) unlink(n *lruNode) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

func (l *LRU) moveToHead(n *lruNode) {
	if l.head == n {
		return
	}
	l.unlink(n)
	l.pushHead(n)
}
