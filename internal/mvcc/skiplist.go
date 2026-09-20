package mvcc

import "math/rand"

const (
	maxLevel    = 12
	branchingP  = 0.25
	sentinelKey = ""
)

// skipNode holds one key and its version chain.
type skipNode struct {
	key   string
	chain *chain
	next  []*skipNode
}

// skipList is an ordered map from key to version chain. It is not safe for
// concurrent use; Store serialises access.
//
// A skip list is used instead of a Go map because the control plane needs
// ordered range scans ("list every VCN in this compartment"), which a hash map
// cannot serve without a full sort on every request.
type skipList struct {
	head  *skipNode
	level int
	rnd   *rand.Rand
	count int
}

func newSkipList(seed int64) *skipList {
	return &skipList{
		head:  &skipNode{key: sentinelKey, next: make([]*skipNode, maxLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(seed)),
	}
}

func (s *skipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rnd.Float64() < branchingP {
		lvl++
	}
	return lvl
}

// get returns the chain for key, or nil.
func (s *skipList) get(key string) *chain {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && x.key == key {
		return x.chain
	}
	return nil
}

// getOrCreate returns the chain for key, creating an empty one if absent.
func (s *skipList) getOrCreate(key string) *chain {
	var prev [maxLevel]*skipNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		prev[i] = x
	}
	if nxt := x.next[0]; nxt != nil && nxt.key == key {
		return nxt.chain
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			prev[i] = s.head
		}
		s.level = lvl
	}
	n := &skipNode{key: key, chain: &chain{}, next: make([]*skipNode, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	s.count++
	return n.chain
}

// remove deletes a key entirely. Used by garbage collection once every version
// of the key is below the watermark and the newest one is a tombstone.
func (s *skipList) remove(key string) bool {
	var prev [maxLevel]*skipNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		prev[i] = x
	}
	target := x.next[0]
	if target == nil || target.key != key {
		return false
	}
	for i := 0; i < s.level; i++ {
		if prev[i].next[i] == target {
			prev[i].next[i] = target.next[i]
		}
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
	s.count--
	return true
}

// scan visits keys in [start, end) in ascending order. An empty end means
// "to the end of the key space". Visiting stops when fn returns false.
func (s *skipList) scan(start, end string, fn func(key string, c *chain) bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < start {
			x = x.next[i]
		}
	}
	for n := x.next[0]; n != nil; n = n.next[0] {
		if end != "" && n.key >= end {
			return
		}
		if !fn(n.key, n.chain) {
			return
		}
	}
}
