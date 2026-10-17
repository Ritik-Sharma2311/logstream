# logstream

A small in-memory log aggregator in Go. It ingests structured log lines over HTTP, indexes them in a sharded inverted index, and answers keyword searches with level and service filters.

## How it works

```
POST /ingest ──> bounded queue ──> worker pool ──> sharded inverted index <── GET /search
                    │
                    └── queue full => HTTP 429 + Retry-After
```

- **Ingest**: a bounded channel feeds a fixed worker pool. When the queue is full the request gets `429 Too Many Requests` and the body reports how many lines were already accepted, so the client resends only the rest.
- **No accepted line is lost**: on shutdown the HTTP server stops first, then the queue is closed and fully drained into the index before the process exits.
- **Index**: entries are spread round-robin across shards, each with its own lock. Every word, plus `level:` and `service:` terms, maps to an ascending list of document ids.
- **Search**: AND semantics. The shortest posting list drives the scan from the newest entry backwards, other lists are checked by binary search, and the scan stops as soon as `limit` matches are found.

## API

Ingest newline-delimited JSON (`ts` is optional and defaults to now):

```
curl -X POST localhost:8080/ingest --data-binary $'{"level":"error","service":"auth","message":"login failed for user 42"}\n'
```

Search:

```
curl "localhost:8080/search?q=login+failed&level=error&service=auth&limit=20"
```

| Endpoint | Description |
|---|---|
| `POST /ingest` | NDJSON body. `202` accepted, `400` bad line, `429` queue full, `503` shutting down |
| `GET /search` | `q`, `level`, `service`, `limit` (default 20). Newest first |
| `GET /stats` | queue depth and counters, indexed entry count |
| `GET /healthz` | liveness |

## Run

```
make run                 # go run ./cmd/logstream
docker build -t logstream . && docker run -p 8080:8080 logstream
```

Flags: `-addr`, `-shards` (16), `-queue` (100000), `-workers` (CPU count).

## Test and benchmark

```
make test     # vet + race detector
make bench    # search benchmarks over 200K entries
make load     # 1M lines through the HTTP API
```

Measured on an 8-core Linux laptop with the load generator on the same machine:

| Scenario | Result |
|---|---|
| Ingest, 1M lines, 16 clients, batches of 500 | about 440K to 490K lines/s |
| Ingest with a 1000-slot queue and 2 workers | 429s returned, all 300K lines eventually accepted and indexed |
| Search, rare term, 200K entries | about 5 µs |
| Search, common term + level filter | about 17 µs |
| Search, two words + level + service filter (sparse match, worst case) | about 230 µs |

Numbers depend on hardware and query shape; run the benchmarks yourself.

## Limits

State is in memory only, there is no persistence or retention, and search is exact word match (no stemming or ranking).
