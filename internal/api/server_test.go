package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Ritik-Sharma2311/logstream/internal/index"
	"github.com/Ritik-Sharma2311/logstream/internal/ingest"
	"github.com/Ritik-Sharma2311/logstream/internal/model"
)

func newTestServer(queue, workers int) (*httptest.Server, *ingest.Pipeline, *index.Index) {
	idx := index.New(4)
	pipe := ingest.New(idx, queue, workers)
	return httptest.NewServer(New(pipe, idx).Handler()), pipe, idx
}

func post(t *testing.T, url, body string) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := http.Post(url+"/ingest", "application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestIngestThenSearch(t *testing.T) {
	ts, pipe, idx := newTestServer(100, 2)
	defer ts.Close()

	body := `{"level":"error","service":"auth","message":"login failed"}
{"level":"info","service":"auth","message":"login ok"}
`
	resp, out := post(t, ts.URL, body)
	if resp.StatusCode != http.StatusAccepted || out["accepted"].(float64) != 2 {
		t.Fatalf("status %d body %v", resp.StatusCode, out)
	}
	_ = pipe.Shutdown(context.Background())
	if idx.Len() != 2 {
		t.Fatalf("want 2 indexed, got %d", idx.Len())
	}

	r, err := http.Get(ts.URL + "/search?q=login&level=error")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var res struct {
		Count int `json:"count"`
	}
	_ = json.NewDecoder(r.Body).Decode(&res)
	if res.Count != 1 {
		t.Fatalf("want 1 result, got %d", res.Count)
	}
}

func TestIngestReturns429WithAcceptedCount(t *testing.T) {
	// One-slot queue and a slow consumer: most of the batch must be rejected.
	idx := index.New(1)
	slow := slowSink{idx: idx, delay: 50 * time.Millisecond}
	pipe := ingest.New(slow, 1, 1)
	ts := httptest.NewServer(New(pipe, idx).Handler())
	defer ts.Close()

	var b strings.Builder
	for i := 0; i < 20; i++ {
		b.WriteString(`{"level":"info","service":"s","message":"m"}` + "\n")
	}
	resp, out := post(t, ts.URL, b.String())
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
	if out["accepted"].(float64) >= 20 {
		t.Fatalf("accepted should be a partial count, got %v", out["accepted"])
	}
}

func TestIngestRejectsBadJSON(t *testing.T) {
	ts, _, _ := newTestServer(10, 1)
	defer ts.Close()
	resp, _ := post(t, ts.URL, "not json\n")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

type slowSink struct {
	idx   *index.Index
	delay time.Duration
}

func (s slowSink) Add(e model.Entry) {
	time.Sleep(s.delay)
	s.idx.Add(e)
}
