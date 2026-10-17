package index

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Ritik-Sharma2311/logstream/internal/model"
)

func entry(ts int, level, service, msg string) model.Entry {
	return model.Entry{Timestamp: time.Unix(int64(ts), 0), Level: level, Service: service, Message: msg}
}

func TestSearchAND(t *testing.T) {
	ix := New(4)
	ix.Add(entry(1, "error", "auth", "login failed for user"))
	ix.Add(entry(2, "info", "auth", "login ok for user"))
	ix.Add(entry(3, "error", "billing", "payment failed"))

	got := ix.Search(Query{Text: "login failed"})
	if len(got) != 1 || got[0].Timestamp.Unix() != 1 {
		t.Fatalf("want only entry 1, got %+v", got)
	}
}

func TestSearchFilters(t *testing.T) {
	ix := New(4)
	ix.Add(entry(1, "error", "auth", "failed"))
	ix.Add(entry(2, "info", "auth", "failed"))
	ix.Add(entry(3, "error", "billing", "failed"))

	if got := ix.Search(Query{Text: "failed", Level: "ERROR"}); len(got) != 2 {
		t.Fatalf("level filter: want 2, got %d", len(got))
	}
	if got := ix.Search(Query{Text: "failed", Level: "error", Service: "auth"}); len(got) != 1 {
		t.Fatalf("level+service filter: want 1, got %d", len(got))
	}
	if got := ix.Search(Query{Level: "info"}); len(got) != 1 {
		t.Fatalf("filter-only query: want 1, got %d", len(got))
	}
}

func TestSearchMissingTermAndEmptyQuery(t *testing.T) {
	ix := New(2)
	ix.Add(entry(1, "info", "a", "hello world"))
	if got := ix.Search(Query{Text: "hello nothere"}); len(got) != 0 {
		t.Fatalf("want empty, got %d", len(got))
	}
	if got := ix.Search(Query{}); got != nil {
		t.Fatalf("empty query should return nil, got %d", len(got))
	}
}

func TestSearchNewestFirstAndLimit(t *testing.T) {
	ix := New(3)
	for i := 1; i <= 30; i++ {
		ix.Add(entry(i, "info", "a", "tick"))
	}
	got := ix.Search(Query{Text: "tick", Limit: 5})
	if len(got) != 5 {
		t.Fatalf("want 5, got %d", len(got))
	}
	for i, e := range got {
		if want := int64(30 - i); e.Timestamp.Unix() != want {
			t.Fatalf("pos %d: want ts %d, got %d", i, want, e.Timestamp.Unix())
		}
	}
}

func TestConcurrentAddAndSearch(t *testing.T) {
	ix := New(8)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				ix.Add(entry(i, "info", "svc", fmt.Sprintf("worker%d msg%d common", w, i)))
				ix.Search(Query{Text: "common", Limit: 3})
			}
		}(w)
	}
	wg.Wait()
	if ix.Len() != 4000 {
		t.Fatalf("want 4000, got %d", ix.Len())
	}
}

func buildBenchIndex() *Index {
	ix := New(16)
	words := []string{"timeout", "connection", "refused", "user", "login", "payment", "retry", "cache", "miss", "slow"}
	levels := []string{"debug", "info", "warn", "error"}
	svcs := []string{"auth", "billing", "gateway", "orders", "search"}
	for i := 0; i < 200_000; i++ {
		msg := fmt.Sprintf("%s %s %s id%d", words[i%10], words[(i/10)%10], words[(i/100)%10], i)
		ix.Add(entry(i, levels[i%4], svcs[i%5], msg))
	}
	return ix
}

func BenchmarkSearch200K(b *testing.B) {
	ix := buildBenchIndex()
	cases := map[string]Query{
		// common term plus level filter: early exit after Limit matches
		"common_term_level": {Text: "timeout", Level: "error", Limit: 20},
		// two words plus both filters
		"two_words_two_filters": {Text: "timeout connection", Level: "error", Service: "auth", Limit: 20},
		// rare term: tiny posting list
		"rare_term": {Text: "id123456", Limit: 20},
	}
	for name, q := range cases {
		b.Run(name, func(b *testing.B) {
			if len(ix.Search(q)) == 0 {
				b.Fatal("expected matches")
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ix.Search(q)
			}
		})
	}
}
