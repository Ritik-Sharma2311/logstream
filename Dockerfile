FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/logstream ./cmd/logstream

FROM alpine:3.20
COPY --from=build /out/logstream /usr/local/bin/logstream
EXPOSE 8080
ENTRYPOINT ["logstream"]
