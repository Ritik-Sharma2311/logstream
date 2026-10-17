// Command loadgen pushes synthetic log lines at a running logstream server
// and reports sustained ingest throughput.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	levels   = []string{"debug", "info", "info", "info", "warn", "error"}
	services = []string{"auth", "billing", "gateway", "orders", "search"}
	words    = []string{"timeout", "connection", "refused", "user", "login", "payment", "retry", "cache", "miss", "slow", "request", "failed", "ok"}
)

func line(i int) []byte {
	msg := fmt.Sprintf("%s %s %s id%d", words[i%len(words)], words[(i/3)%len(words)], words[(i/7)%len(words)], i)
	b, _ := json.Marshal(map[string]string{
		"level":   levels[i%len(levels)],
		"service": services[i%len(services)],
		"message": msg,
	})
	return append(b, '\n')
}

func main() {
	url := flag.String("url", "http://localhost:8080", "server base url")
	total := flag.Int("n", 1_000_000, "total lines to send")
	conc := flag.Int("c", 16, "concurrent senders")
	batch := flag.Int("batch", 500, "lines per request")
	flag.Parse()

	var sent, throttled atomic.Int64
	var next atomic.Int64
	client := &http.Client{Timeout: 30 * time.Second}

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				from := int(next.Add(int64(*batch))) - *batch
				if from >= *total {
					return
				}
				to := from + *batch
				if to > *total {
					to = *total
				}
				var buf bytes.Buffer
				for i := from; i < to; i++ {
					buf.Write(line(i))
				}
				payload := buf.Bytes()
				// Resend only what the server did not accept.
				for len(payload) > 0 {
					resp, err := client.Post(*url+"/ingest", "application/x-ndjson", bytes.NewReader(payload))
					if err != nil {
						log.Fatal(err)
					}
					var res struct {
						Accepted int `json:"accepted"`
					}
					_ = json.NewDecoder(resp.Body).Decode(&res)
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					sent.Add(int64(res.Accepted))
					if resp.StatusCode == http.StatusAccepted {
						break
					}
					if resp.StatusCode != http.StatusTooManyRequests {
						log.Fatalf("unexpected status %d", resp.StatusCode)
					}
					throttled.Add(1)
					payload = dropLines(payload, res.Accepted)
					time.Sleep(20 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()

	el := time.Since(start)
	fmt.Printf("sent=%d in %s => %.0f lines/s (429 responses: %d)\n",
		sent.Load(), el.Round(time.Millisecond), float64(sent.Load())/el.Seconds(), throttled.Load())
}

func dropLines(b []byte, n int) []byte {
	for ; n > 0; n-- {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			return nil
		}
		b = b[i+1:]
	}
	return b
}
