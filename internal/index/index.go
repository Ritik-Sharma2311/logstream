// Package index is an in-memory sharded inverted index over log entries.
package index

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/Ritik-Sharma2311/logstream/internal/model"
)

const (
	levelPrefix   = "level:"
	servicePrefix = "service:"
)

// Query selects entries containing every word in Text, optionally limited to
// a level and/or service. Matches are returned newest first.
type Query struct {
	Text    string
	Level   string
	Service string
	Limit   int
}

type shard struct {
	mu       sync.RWMutex
	docs     []model.Entry
	postings map[string][]uint32 // term -> ascending doc ids
}

// Index spreads entries round-robin over shards so concurrent writers rarely
// contend on the same lock.
type Index struct {
	shards []*shard
	next   atomic.Uint64
}

// New creates an index with n shards.
func New(n int) *Index {
	if n < 1 {
		n = 1
	}
	ix := &Index{shards: make([]*shard, n)}
	for i := range ix.shards {
		ix.shards[i] = &shard{postings: make(map[string][]uint32)}
	}
	return ix
}

// Add indexes e. Safe for concurrent use.
func (ix *Index) Add(e model.Entry) {
	s := ix.shards[ix.next.Add(1)%uint64(len(ix.shards))]
	terms := entryTerms(e)

	s.mu.Lock()
	id := uint32(len(s.docs))
	s.docs = append(s.docs, e)
	for _, t := range terms {
		s.postings[t] = append(s.postings[t], id)
	}
	s.mu.Unlock()
}

// Len returns the number of indexed entries.
func (ix *Index) Len() int {
	n := 0
	for _, s := range ix.shards {
		s.mu.RLock()
		n += len(s.docs)
		s.mu.RUnlock()
	}
	return n
}

// Search runs q over all shards and returns up to q.Limit entries, newest first.
func (ix *Index) Search(q Query) []model.Entry {
	terms := queryTerms(q)
	if len(terms) == 0 {
		return nil
	}
	limit := q.Limit
	if limit < 1 {
		limit = 20
	}

	out := make([]model.Entry, 0, limit*len(ix.shards))
	for _, s := range ix.shards {
		out = s.search(out, terms, limit)
	}
	slices.SortFunc(out, func(a, b model.Entry) int { return b.Timestamp.Compare(a.Timestamp) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// search intersects the posting lists of terms, walking from the newest id
// down and stopping as soon as limit matches are found.
func (s *shard) search(out []model.Entry, terms []string, limit int) []model.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lists := make([][]uint32, 0, len(terms))
	for _, t := range terms {
		l, ok := s.postings[t]
		if !ok {
			return out // one missing term means an empty AND result
		}
		lists = append(lists, l)
	}
	slices.SortFunc(lists, func(a, b []uint32) int { return len(a) - len(b) })

	driver, rest := lists[0], lists[1:]
	found := 0
	for i := len(driver) - 1; i >= 0 && found < limit; i-- {
		id := driver[i]
		if containsAll(rest, id) {
			out = append(out, s.docs[id])
			found++
		}
	}
	return out
}

func containsAll(lists [][]uint32, id uint32) bool {
	for _, l := range lists {
		i := sort.Search(len(l), func(k int) bool { return l[k] >= id })
		if i == len(l) || l[i] != id {
			return false
		}
	}
	return true
}

func entryTerms(e model.Entry) []string {
	words := tokenize(e.Message)
	terms := make([]string, 0, len(words)+2)
	seen := make(map[string]struct{}, len(words)+2)
	add := func(t string) {
		if _, dup := seen[t]; dup {
			return
		}
		seen[t] = struct{}{}
		terms = append(terms, t)
	}
	for _, w := range words {
		add(w)
	}
	if e.Level != "" {
		add(levelPrefix + strings.ToLower(e.Level))
	}
	if e.Service != "" {
		add(servicePrefix + strings.ToLower(e.Service))
	}
	return terms
}

func queryTerms(q Query) []string {
	terms := tokenize(q.Text)
	if q.Level != "" {
		terms = append(terms, levelPrefix+strings.ToLower(q.Level))
	}
	if q.Service != "" {
		terms = append(terms, servicePrefix+strings.ToLower(q.Service))
	}
	return terms
}

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}
