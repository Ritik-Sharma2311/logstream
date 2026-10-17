package ingest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ritik-Sharma2311/logstream/internal/model"
)

type countSink struct{ n atomic.Int64 }

func (c *countSink) Add(model.Entry) { c.n.Add(1) }

// blockSink holds every Add until release is closed.
type blockSink struct {
	release chan struct{}
	n       atomic.Int64
}

func (b *blockSink) Add(model.Entry) {
	<-b.release
	b.n.Add(1)
}

func TestBackpressureWhenQueueFull(t *testing.T) {
	sink := &blockSink{release: make(chan struct{})}
	p := New(sink, 2, 1)

	var full int
	for i := 0; i < 50; i++ {
		if err := p.Submit(model.Entry{}); errors.Is(err, ErrFull) {
			full++
		}
	}
	if full == 0 {
		t.Fatal("expected ErrFull once queue and worker were saturated")
	}
	close(sink.release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNoAcceptedEntryDroppedOnShutdown(t *testing.T) {
	sink := &countSink{}
	p := New(sink, 1000, 4)

	var wg sync.WaitGroup
	var accepted atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				if p.Submit(model.Entry{}) == nil {
					accepted.Add(1)
				}
			}
		}()
	}
	time.Sleep(time.Millisecond)
	// Shut down while producers are still running.
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if got, want := sink.n.Load(), accepted.Load(); got != want {
		t.Fatalf("accepted %d but sink saw %d", want, got)
	}
	if err := p.Submit(model.Entry{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed after shutdown, got %v", err)
	}
}

func TestShutdownRespectsContext(t *testing.T) {
	sink := &blockSink{release: make(chan struct{})}
	p := New(sink, 10, 1)
	_ = p.Submit(model.Entry{})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	close(sink.release)
}
