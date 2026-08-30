package policy

import "fmt"

// Default S3-FIFO parameters, taken from libCacheSim's DEFAULT_CACHE_PARAMS so
// that a cross-check against cachesim compares the same algorithm rather than
// the same name. Reported verbatim in results.
const (
	DefaultS3FIFOSmallRatio    = 0.10
	DefaultS3FIFOGhostRatio    = 0.90
	DefaultS3FIFOMoveThreshold = 2
	s3fifoFreqCap              = 3 // 2-bit clock counter in the main queue
)

// S3FIFO (SOSP'23) is three FIFO queues: a small admission queue holding ~10%
// of the cache, a main queue holding the rest, and a ghost queue of keys only.
//
// The idea is that most objects in a real workload are requested exactly once.
// Those enter the small queue and leave it quickly, never polluting the main
// queue. An object that earns a second access before the small queue drains is
// promoted to main; one that is evicted from small leaves its key in the ghost
// queue, so if it comes back it is recognized as worth main-queue space
// immediately.
//
// This follows libCacheSim's S3FIFO.c, which differs from the paper's original
// in one respect worth knowing: when the small queue is full but the cache as a
// whole is not yet full, an admission goes to the main queue rather than the
// small one.
type S3FIFO struct {
	small, main, ghost *s3queue

	smallRatio, ghostRatio float64
	moveToMain             int

	// hitOnGhost is set while handling a miss whose key was found in the
	// ghost queue, and consumed by the following OnAdmit.
	hitOnGhost bool

	// hasEvicted stays false until the cache first evicts, which is how the
	// policy tells "still filling up" from "at steady state".
	hasEvicted bool
}

type s3node struct {
	key          string
	size         int
	freq         int
	older, newer *s3node
}

// s3queue is a byte-bounded FIFO. The head is the oldest and leaves first.
type s3queue struct {
	items      map[string]*s3node
	head, tail *s3node
	used       int64
	capacity   int64
	keyBytes   int
}

func init() {
	Register("s3fifo", func(c Config) (Policy, error) { return NewS3FIFO(c) })
	Register("s3-fifo", func(c Config) (Policy, error) { return NewS3FIFO(c) })
}

// NewS3FIFO builds an S3-FIFO policy. Accepted parameters mirror libCacheSim's:
// small-size-ratio, ghost-size-ratio, move-to-main-threshold.
func NewS3FIFO(cfg Config) (*S3FIFO, error) {
	p, err := ParseParams(cfg.Params)
	if err != nil {
		return nil, err
	}
	if err := p.Unknown("small-size-ratio", "ghost-size-ratio", "move-to-main-threshold"); err != nil {
		return nil, err
	}
	smallRatio, err := p.Float("small-size-ratio", DefaultS3FIFOSmallRatio)
	if err != nil {
		return nil, err
	}
	ghostRatio, err := p.Float("ghost-size-ratio", DefaultS3FIFOGhostRatio)
	if err != nil {
		return nil, err
	}
	moveToMain, err := p.Int("move-to-main-threshold", DefaultS3FIFOMoveThreshold)
	if err != nil {
		return nil, err
	}
	if smallRatio <= 0 || smallRatio >= 1 {
		return nil, fmt.Errorf("s3fifo: small-size-ratio must be in (0, 1), got %v", smallRatio)
	}
	if ghostRatio < 0 {
		return nil, fmt.Errorf("s3fifo: ghost-size-ratio must be non-negative, got %v", ghostRatio)
	}

	smallCap := int64(float64(cfg.CacheSize) * smallRatio)
	mainCap := cfg.CacheSize - smallCap
	ghostCap := int64(float64(cfg.CacheSize) * ghostRatio)
	if smallCap <= 0 || mainCap <= 0 {
		return nil, fmt.Errorf(
			"s3fifo: cache size %d with small-size-ratio %v gives small=%d main=%d; the cache is too small to split",
			cfg.CacheSize, smallRatio, smallCap, mainCap)
	}

	return &S3FIFO{
		small:      newS3Queue(smallCap),
		main:       newS3Queue(mainCap),
		ghost:      newS3Queue(ghostCap),
		smallRatio: smallRatio,
		ghostRatio: ghostRatio,
		moveToMain: moveToMain,
	}, nil
}

func (s *S3FIFO) Name() string { return "s3fifo" }

// Params renders the parameters actually in force, for the results file.
func (s *S3FIFO) Params() string {
	return fmt.Sprintf("small-size-ratio=%g,ghost-size-ratio=%g,move-to-main-threshold=%d",
		s.smallRatio, s.ghostRatio, s.moveToMain)
}

func (s *S3FIFO) OnHit(key string) {
	if n := s.small.items[key]; n != nil {
		n.freq++
		return
	}
	if n := s.main.items[key]; n != nil {
		n.freq++
	}
}

// CanAdmit also performs the ghost-queue lookup, because libCacheSim consults
// and clears the ghost inside find(), which runs before can_insert and before
// any eviction. Doing it here keeps the same ordering.
func (s *S3FIFO) CanAdmit(key string, size int) bool {
	s.hitOnGhost = false
	if s.ghost.capacity > 0 && s.ghost.remove(key) {
		s.hitOnGhost = true
	}

	if int64(size) > s.small.capacity {
		return false
	}
	if s.hitOnGhost {
		// Ghost hits go straight to the main queue, which is large enough.
		return true
	}
	// An object exactly the size of the small queue would fill it completely
	// and evict everything else in it, so libCacheSim refuses that case.
	return int64(size) < s.small.capacity
}

func (s *S3FIFO) OnAdmit(key string, size int) {
	if s.hitOnGhost {
		s.hitOnGhost = false
		s.main.push(key, size)
		return
	}
	// Small queue full but the cache is not yet: send it to main, where it
	// will be evicted sooner than the objects already queued in small.
	if !s.hasEvicted && s.small.used >= s.small.capacity {
		s.main.push(key, size)
		return
	}
	s.small.push(key, size)
}

// Evict returns one victim, having performed any promotions and demotions the
// sweep required. The victim is already unlinked from the policy's queues; the
// cache's following OnRemove is a no-op for it.
func (s *S3FIFO) Evict() (string, bool) {
	s.hasEvicted = true

	if s.main.used > s.main.capacity || s.small.used == 0 {
		return s.evictMain()
	}
	return s.evictSmall()
}

// evictSmall drains the small queue's head until it finds an object that was
// not re-accessed. Objects that were re-accessed are promoted to main and the
// sweep continues, so a single Evict call can promote several objects.
func (s *S3FIFO) evictSmall() (string, bool) {
	for s.small.head != nil {
		n := s.small.head
		s.small.unlink(n)

		if n.freq >= s.moveToMain {
			// Promotion resets the counter: the object must earn its keep
			// again under the main queue's clock.
			s.main.push(n.key, n.size)
			continue
		}
		s.ghostAdd(n.key, n.size)
		return n.key, true
	}
	// Nothing left in small; fall through to main rather than reporting that
	// there is nothing to evict while main still holds objects.
	return s.evictMain()
}

// evictMain runs a 2-bit clock over the main queue: an object with a non-zero
// counter is reinserted at the tail with its counter decremented, and the first
// object reaching zero is evicted.
func (s *S3FIFO) evictMain() (string, bool) {
	for s.main.head != nil {
		n := s.main.head
		freq := n.freq
		s.main.unlink(n)

		if freq >= 1 {
			m := s.main.push(n.key, n.size)
			if freq > s3fifoFreqCap {
				freq = s3fifoFreqCap
			}
			m.freq = freq - 1
			continue
		}
		return n.key, true
	}
	return "", false
}

// ghostAdd records an evicted key so a prompt re-reference can skip the small
// queue. The ghost holds keys and sizes only, never values.
func (s *S3FIFO) ghostAdd(key string, size int) {
	if s.ghost.capacity <= 0 || int64(size) > s.ghost.capacity {
		return
	}
	if _, exists := s.ghost.items[key]; exists {
		return
	}
	for s.ghost.used+int64(size) > s.ghost.capacity && s.ghost.head != nil {
		s.ghost.unlink(s.ghost.head)
	}
	s.ghost.push(key, size)
}

func (s *S3FIFO) OnRemove(key string) {
	if s.small.remove(key) {
		return
	}
	s.main.remove(key)
}

func (s *S3FIFO) MetadataBytes() int {
	// The ghost queue is the honest cost of S3-FIFO: it keeps per-object
	// bookkeeping for objects the cache is no longer holding, and that shows
	// up here rather than being quietly excluded.
	return s.small.metadataBytes() + s.main.metadataBytes() + s.ghost.metadataBytes()
}

// --- s3queue ---

func newS3Queue(capacity int64) *s3queue {
	return &s3queue{items: make(map[string]*s3node), capacity: capacity}
}

func (q *s3queue) push(key string, size int) *s3node {
	if n, ok := q.items[key]; ok {
		return n
	}
	n := &s3node{key: key, size: size}
	q.items[key] = n
	q.used += int64(size)
	q.keyBytes += len(key)

	n.older, n.newer = q.tail, nil
	if q.tail != nil {
		q.tail.newer = n
	}
	q.tail = n
	if q.head == nil {
		q.head = n
	}
	return n
}

func (q *s3queue) unlink(n *s3node) {
	if n.older != nil {
		n.older.newer = n.newer
	} else {
		q.head = n.newer
	}
	if n.newer != nil {
		n.newer.older = n.older
	} else {
		q.tail = n.older
	}
	n.older, n.newer = nil, nil
	delete(q.items, n.key)
	q.used -= int64(n.size)
	q.keyBytes -= len(n.key)
}

func (q *s3queue) remove(key string) bool {
	n, ok := q.items[key]
	if !ok {
		return false
	}
	q.unlink(n)
	return true
}

func (q *s3queue) metadataBytes() int {
	const perNode = StringHeaderBytes + 8 + 8 + 2*PointerBytes // key, size, freq, links
	perEntry := StringHeaderBytes + PointerBytes + MapEntryBytes + perNode
	return len(q.items)*perEntry + 2*q.keyBytes
}
