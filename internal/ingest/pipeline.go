// Package ingest accepts log entries into a bounded queue and drains them
// into a sink with a fixed pool of workers.
package ingest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/Ritik-Sharma2311/logstream/internal/model"
)

var (
	// ErrFull means the queue has no room; callers should back off and retry.
	ErrFull = errors.New("ingest queue full")
	// ErrClosed means the pipeline is shutting down and takes no new entries.
	ErrClosed = errors.New("ingest pipeline closed")
)

// Sink receives entries from the workers. It must be safe for concurrent use.
type Sink interface {
	Add(model.Entry)
}

// Stats is a point-in-time view of pipeline counters.
type Stats struct {
	Accepted   uint64 `json:"accepted"`
	Rejected   uint64 `json:"rejected"`
	Processed  uint64 `json:"processed"`
	QueueDepth int    `json:"queue_depth"`
	QueueCap   int    `json:"queue_cap"`
}

// Pipeline is a bounded queue plus worker pool.
type Pipeline struct {
	sink  Sink
	queue chan model.Entry
	wg    sync.WaitGroup

	// mu makes Submit and Shutdown mutually safe: Submit holds the read
	// lock while sending, Shutdown takes the write lock before closing the
	// channel, so a send on a closed channel cannot happen.
	mu     sync.RWMutex
	closed bool

	accepted  atomic.Uint64
	rejected  atomic.Uint64
	processed atomic.Uint64
}

// New starts workers goroutines draining a queue of queueSize entries.
func New(sink Sink, queueSize, workers int) *Pipeline {
	if queueSize < 1 {
		queueSize = 1
	}
	if workers < 1 {
		workers = 1
	}
	p := &Pipeline{sink: sink, queue: make(chan model.Entry, queueSize)}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	for e := range p.queue {
		p.sink.Add(e)
		p.processed.Add(1)
	}
}

// Submit enqueues e without blocking. A nil return means the entry is
// accepted and will reach the sink, even across a graceful Shutdown.
func (p *Pipeline) Submit(e model.Entry) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.queue <- e:
		p.accepted.Add(1)
		return nil
	default:
		p.rejected.Add(1)
		return ErrFull
	}
}

// Shutdown stops accepting entries and waits for the queue to drain. If ctx
// expires first it returns ctx.Err(); workers keep draining in the background.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns the current counters.
func (p *Pipeline) Stats() Stats {
	return Stats{
		Accepted:   p.accepted.Load(),
		Rejected:   p.rejected.Load(),
		Processed:  p.processed.Load(),
		QueueDepth: len(p.queue),
		QueueCap:   cap(p.queue),
	}
}
