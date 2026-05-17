package memcache

import (
	"container/list"
	"hash/maphash"
)

// Policy selects the eviction algorithm for the in-memory adapter.
type Policy int

const (
	// LRU is the default: simple least-recently-used.
	LRU Policy = iota
	// AdaptiveWTinyLFU is a Caffeine-style Window-TinyLFU: a small LRU
	// admission window in front of a segmented-LRU main region, with a
	// Count-Min frequency sketch (with aging) gating admission. It resists
	// one-hit-wonder floods and scans far better than plain LRU.
	AdaptiveWTinyLFU
)

// evictPolicy tracks key ordering/frequency and chooses victims. All methods
// are called with the owning shard's lock held. drop removes a displaced key
// from the shard (map + byte accounting + EvictSize counter); a policy never
// touches shard state directly.
type evictPolicy interface {
	// add records a newly inserted key. It returns false if admission was
	// denied (the caller must then drop the just-inserted entry). Displaced
	// incumbents are removed via the drop callback.
	add(key string) bool
	// hit records access to an existing key.
	hit(key string)
	// remove forgets a key the caller already deleted.
	remove(key string)
	// evict forcibly removes and returns one victim (used for byte-cap
	// trimming, independent of the key-count cap).
	evict() (string, bool)
	// clear resets all bookkeeping (Flush).
	clear()
}

// ---------- LRU ----------

type lruPolicy struct {
	cap  int // 0 = unlimited (count cap disabled)
	ll   *list.List
	idx  map[string]*list.Element
	drop func(key string)
}

func newLRUPolicy(capKeys int, drop func(string)) *lruPolicy {
	return &lruPolicy{cap: capKeys, ll: list.New(), idx: map[string]*list.Element{}, drop: drop}
}

func (p *lruPolicy) add(key string) bool {
	p.idx[key] = p.ll.PushFront(key)
	if p.cap > 0 && p.ll.Len() > p.cap {
		if k, ok := p.popTail(); ok {
			p.drop(k)
		}
	}
	return true
}

func (p *lruPolicy) hit(key string) {
	if el, ok := p.idx[key]; ok {
		p.ll.MoveToFront(el)
	}
}

func (p *lruPolicy) remove(key string) {
	if el, ok := p.idx[key]; ok {
		p.ll.Remove(el)
		delete(p.idx, key)
	}
}

func (p *lruPolicy) popTail() (string, bool) {
	el := p.ll.Back()
	if el == nil {
		return "", false
	}
	k := el.Value.(string)
	p.ll.Remove(el)
	delete(p.idx, k)
	return k, true
}

func (p *lruPolicy) evict() (string, bool) { return p.popTail() }

func (p *lruPolicy) clear() {
	p.ll.Init()
	p.idx = map[string]*list.Element{}
}

// ---------- Count-Min frequency sketch (4-bit counters, with aging) ----------

// cmSketch is a Count-Min sketch of access frequency with 4-bit saturating
// counters and periodic aging. Count-Min only ever over-estimates (hash
// collisions add spurious counts), so estimate() takes the MINIMUM across
// rows — the tightest available bound. Four rows with independent seeds make
// a collision on all four simultaneously unlikely. Aging (halving every
// counter) keeps the estimate biased toward RECENT frequency, which is what
// makes the policy adaptive rather than all-time-LFU.
type cmSketch struct {
	rows   [4][]uint8 // 4 hash rows; each cell is a 4-bit counter packed 2/byte
	mask   uint64
	seeds  [4]maphash.Seed
	adds   int
	sample int // halve all counters once adds reaches this
}

func newCMSketch(capacity int) *cmSketch {
	width := 1
	target := capacity * 8
	if target < 1024 {
		target = 1024
	}
	for width < target {
		width <<= 1
	}
	s := &cmSketch{mask: uint64(width - 1), sample: width * 8}
	for i := range s.rows {
		s.rows[i] = make([]uint8, width/2+1)
		s.seeds[i] = maphash.MakeSeed()
	}
	return s
}

// pos hashes key for the given row and splits the result into a byte index
// and a nibble selector: counters are packed two per byte (hi/lo nibble) to
// halve sketch memory. The low bit of the masked hash picks the nibble; the
// rest picks the byte.
func (s *cmSketch) pos(row int, key string) (idx int, hi bool) {
	var h maphash.Hash
	h.SetSeed(s.seeds[row])
	_, _ = h.WriteString(key)
	v := h.Sum64() & s.mask
	return int(v >> 1), v&1 == 1
}

func (s *cmSketch) get4(b uint8, hi bool) uint8 {
	if hi {
		return b >> 4
	}
	return b & 0x0f
}

func (s *cmSketch) set4(b *uint8, hi bool, v uint8) {
	if v > 15 {
		v = 15
	}
	if hi {
		*b = (*b & 0x0f) | (v << 4)
	} else {
		*b = (*b & 0xf0) | v
	}
}

func (s *cmSketch) increment(key string) {
	for r := 0; r < 4; r++ {
		i, hi := s.pos(r, key)
		cur := s.get4(s.rows[r][i], hi)
		if cur < 15 {
			s.set4(&s.rows[r][i], hi, cur+1)
		}
	}
	s.adds++
	if s.adds >= s.sample {
		s.age()
	}
}

func (s *cmSketch) estimate(key string) uint8 {
	lo := uint8(15)
	for r := 0; r < 4; r++ {
		i, hi := s.pos(r, key)
		if v := s.get4(s.rows[r][i], hi); v < lo {
			lo = v
		}
	}
	return lo
}

// age halves every counter so the sketch tracks recent frequency, not
// all-time totals (this is the "aging" that makes TinyLFU adaptive). The
// 0x77 mask is the trick that lets one shift-and-mask age BOTH packed nibbles
// at once: shifting the whole byte right by 1 would bleed the high nibble's
// least-significant bit into the low nibble's most-significant bit; masking
// with 0x77 (0111_0111) clears exactly those two boundary bits, so each
// nibble is independently halved. Triggered every s.sample increments.
func (s *cmSketch) age() {
	for r := 0; r < 4; r++ {
		for i := range s.rows[r] {
			b := s.rows[r][i]
			s.rows[r][i] = ((b >> 1) & 0x77)
		}
	}
	s.adds = 0
}

func (s *cmSketch) reset() {
	for r := 0; r < 4; r++ {
		for i := range s.rows[r] {
			s.rows[r][i] = 0
		}
	}
	s.adds = 0
}

// ---------- Window-TinyLFU ----------

// wtinyProtectedFrac is the fraction of the main region kept "protected"
// (frequently re-accessed). The admission window is a fixed 1% of capacity.
const wtinyProtectedFrac = 0.80

type seg uint8

const (
	segWindow seg = iota
	segProbation
	segProtected
)

type wtinyEntry struct {
	key string
	seg seg
}

// wtinyPolicy is Window-TinyLFU: a small LRU admission window (winCap, ~1% of
// capacity) feeding a segmented-LRU main region split into probation and a
// protected sub-region (protCap, ~80% of main). New keys land in the window;
// the window's LRU tail must win a frequency duel (via the Count-Min sketch)
// against the main region's coldest victim to be admitted. This is what
// rejects one-hit-wonders and scan floods that would otherwise flush a plain
// LRU. idx maps every tracked key to its list element regardless of which
// segment it currently lives in.
type wtinyPolicy struct {
	sketch *cmSketch
	drop   func(key string)

	// 0 = unlimited (admission / count eviction disabled).
	winCap, mainCap, protCap int

	window, probation, protected *list.List
	idx                          map[string]*list.Element
}

func newWTinyPolicy(capKeys int, drop func(string)) *wtinyPolicy {
	p := &wtinyPolicy{
		sketch:    newCMSketch(capKeys),
		drop:      drop,
		window:    list.New(),
		probation: list.New(),
		protected: list.New(),
		idx:       map[string]*list.Element{},
	}
	if capKeys > 0 {
		p.winCap = capKeys / 100
		if p.winCap < 1 {
			p.winCap = 1
		}
		p.mainCap = capKeys - p.winCap
		if p.mainCap < 1 {
			p.mainCap = 1
		}
		p.protCap = int(float64(p.mainCap) * wtinyProtectedFrac)
	}
	return p
}

func (p *wtinyPolicy) unlimited() bool { return p.winCap == 0 }
func (p *wtinyPolicy) mainLen() int    { return p.probation.Len() + p.protected.Len() }

func (p *wtinyPolicy) listFor(s seg) *list.List {
	switch s {
	case segWindow:
		return p.window
	case segProtected:
		return p.protected
	default:
		return p.probation
	}
}

// add records a newly inserted key and runs TinyLFU admission. Returns true
// if the key is now resident (in window or probation), false if admission was
// denied. Subtle final return: when the rejected candidate IS the key that
// was just inserted (window had room only for it, then it lost the duel), we
// return false so the caller drops the just-stored entry; when the rejected
// candidate is some OTHER (older) window key, the new key is resident in the
// window, so we return true. Hence `cand.key != key`.
func (p *wtinyPolicy) add(key string) bool {
	p.sketch.increment(key)
	p.idx[key] = p.window.PushFront(&wtinyEntry{key: key, seg: segWindow})
	if p.unlimited() || p.window.Len() <= p.winCap {
		return true // unlimited, or window not full yet
	}
	// Window overflowed: its LRU tail is the candidate for the main region.
	tail := p.window.Back()
	cand := tail.Value.(*wtinyEntry)
	p.window.Remove(tail)

	if p.mainLen() < p.mainCap {
		cand.seg = segProbation
		p.idx[cand.key] = p.probation.PushFront(cand)
		return true
	}
	// Main is full: TinyLFU admission — candidate vs probation victim.
	vEl := p.probation.Back()
	if vEl == nil { // main is all-protected; evict protected tail instead
		vEl = p.protected.Back()
	}
	victim := vEl.Value.(*wtinyEntry)
	if p.sketch.estimate(cand.key) > p.sketch.estimate(victim.key) {
		p.listFor(victim.seg).Remove(vEl)
		delete(p.idx, victim.key)
		p.drop(victim.key)
		cand.seg = segProbation
		p.idx[cand.key] = p.probation.PushFront(cand)
		return true
	}
	// Reject the candidate (it never really enters the cache).
	delete(p.idx, cand.key)
	p.drop(cand.key)
	return cand.key != key
}

func (p *wtinyPolicy) hit(key string) {
	p.sketch.increment(key)
	el, ok := p.idx[key]
	if !ok {
		return
	}
	e := el.Value.(*wtinyEntry)
	switch e.seg {
	case segWindow:
		p.window.MoveToFront(el)
	case segProbation:
		// Promote to protected; demote protected LRU tail if it overflows.
		p.probation.Remove(el)
		e.seg = segProtected
		p.idx[key] = p.protected.PushFront(e)
		if p.protCap > 0 && p.protected.Len() > p.protCap {
			dt := p.protected.Back()
			de := dt.Value.(*wtinyEntry)
			p.protected.Remove(dt)
			de.seg = segProbation
			p.idx[de.key] = p.probation.PushFront(de)
		}
	case segProtected:
		p.protected.MoveToFront(el)
	}
}

func (p *wtinyPolicy) remove(key string) {
	if el, ok := p.idx[key]; ok {
		e := el.Value.(*wtinyEntry)
		p.listFor(e.seg).Remove(el)
		delete(p.idx, key)
	}
}

// evict removes one victim: probation tail, then window tail, then protected
// tail — coldest-first.
func (p *wtinyPolicy) evict() (string, bool) {
	for _, l := range []*list.List{p.probation, p.window, p.protected} {
		if el := l.Back(); el != nil {
			e := el.Value.(*wtinyEntry)
			l.Remove(el)
			delete(p.idx, e.key)
			return e.key, true
		}
	}
	return "", false
}

func (p *wtinyPolicy) clear() {
	p.window.Init()
	p.probation.Init()
	p.protected.Init()
	p.idx = map[string]*list.Element{}
	p.sketch.reset()
}
