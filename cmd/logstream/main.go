package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Ritik-Sharma2311/logstream/internal/api"
	"github.com/Ritik-Sharma2311/logstream/internal/index"
	"github.com/Ritik-Sharma2311/logstream/internal/ingest"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	shards := flag.Int("shards", 16, "index shards")
	queue := flag.Int("queue", 100000, "ingest queue capacity")
	workers := flag.Int("workers", runtime.NumCPU(), "ingest workers")
	flag.Parse()

	idx := index.New(*shards)
	pipe := ingest.New(idx, *queue, *workers)
	srv := &http.Server{Addr: *addr, Handler: api.New(pipe, idx).Handler()}

	go func() {
		log.Printf("listening on %s (shards=%d queue=%d workers=%d)", *addr, *shards, *queue, *workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Stop taking requests first, then let the queue drain into the index.
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := pipe.Shutdown(ctx); err != nil {
		log.Printf("drain incomplete: %v", err)
	}
	log.Printf("stopped, indexed %d entries", idx.Len())
}
