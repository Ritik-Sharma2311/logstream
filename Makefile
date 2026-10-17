.PHONY: test bench run load

test:
	go vet ./...
	go test -race -count=1 ./...

bench:
	go test -run xxx -bench . -benchmem ./internal/index

run:
	go run ./cmd/logstream

load:
	go run ./cmd/loadgen -n 1000000 -c 16 -batch 500
