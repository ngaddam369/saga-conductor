FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /bin/saga-conductor ./cmd/server

FROM alpine:3.21

RUN addgroup -S conductor && adduser -S conductor -G conductor

COPY --from=builder /bin/saga-conductor /usr/local/bin/saga-conductor

USER conductor

EXPOSE 8080 8081

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8081/health/live || exit 1

ENTRYPOINT ["saga-conductor"]
