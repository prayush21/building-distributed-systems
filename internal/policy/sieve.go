package policy

// SIEVE (NSDI'24) is a single FIFO queue plus a moving hand.
//
// Each object carries one visited bit, set on a hit and never cleared on the
// access path. Eviction walks a hand from the old end toward the new end: it
// clears the visited bit of everything it passes and evicts the first object
// whose bit is already clear. The hand keeps its position between evictions,
// which is the whole trick -- newly admitted objects are inserted at the new
// end, *ahead* of the hand, so they must survive a full sweep before they can
// be considered. That gives new objects a quick eviction path without the
// separate queue S3-FIFO needs.
//
// Matches libCacheSim's Sieve.c: hits set the bit to 1 rather than incrementing
// it, the hand advances toward the head and wraps to the tail, and after an
// eviction the hand points at the object just newer than the victim.
type SIEVE struct {
	items      map[string]*sieveNode
	head, tail *sieveNode // head is newest, tail is oldest
	hand       *sieveNode // nil means "start the next sweep at the tail"
	keyBytes   int
}

type sieveNode struct {
	key          string
	visited      bool
	older, newer *sieveNode
}

func init() {
	Register("sieve", func(Config) (Policy, error) { return NewSIEVE(), nil })
}

// NewSIEVE returns an empty SIEVE policy.
func NewSIEVE() *SIEVE { return &SIEVE{items: make(map[string]*sieveNode)} }

func (s *SIEVE) Name() string { return "sieve" }

// OnHit sets the visited bit. It is set, not incremented: SIEVE keeps one bit
// per object, so a hot object gains nothing from being hit repeatedly between
// sweeps.
func (s *SIEVE) OnHit(key string) {
	if n, ok := s.items[key]; ok {
		n.visited = true
	}
}

func (s *SIEVE) CanAdmit(string, int) bool { return true }

func (s *SIEVE) OnAdmit(key string, _ int) {
	if _, ok := s.items[key]; ok {
		return
	}
	n := &sieveNode{key: key}
	s.items[key] = n
	s.keyBytes += len(key)

	// New objects go to the head, ahead of the hand.
	n.newer, n.older = nil, s.head
	if s.head != nil {
		s.head.newer = n
	}
	s.head = n
	if s.tail == nil {
		s.tail = n
	}
}

// Evict sweeps the hand toward the head, clearing visited bits, and returns the
// first object whose bit was already clear.
//
// The victim is left linked; the cache removes it through OnRemove. The hand is
// advanced here so that OnRemove sees a hand that has already moved off the
// victim and does not adjust it twice.
func (s *SIEVE) Evict() (string, bool) {
	if len(s.items) == 0 {
		return "", false
	}

	n := s.hand
	if n == nil {
		n = s.tail
	}
	// Terminates: every step either finds a clear bit or clears one, and
	// there are finitely many set bits.
	for n.visited {
		n.visited = false
		if n.newer == nil {
			n = s.tail // past the head, wrap to the oldest
		} else {
			n = n.newer
		}
	}

	s.hand = n.newer
	return n.key, true
}

func (s *SIEVE) OnRemove(key string) {
	n, ok := s.items[key]
	if !ok {
		return
	}
	// Removing the object the hand rests on would strand the hand, so move it
	// one step along its sweep direction first.
	if s.hand == n {
		s.hand = n.newer
	}
	if n.older != nil {
		n.older.newer = n.newer
	} else {
		s.tail = n.newer
	}
	if n.newer != nil {
		n.newer.older = n.older
	} else {
		s.head = n.older
	}
	delete(s.items, key)
	s.keyBytes -= len(key)
}

func (s *SIEVE) MetadataBytes() int {
	// One visited bit per object, but Go pads it to a byte inside the node.
	const perNode = StringHeaderBytes + 1 + 2*PointerBytes
	perEntry := StringHeaderBytes + PointerBytes + MapEntryBytes + perNode
	return len(s.items)*perEntry + 2*s.keyBytes
}
