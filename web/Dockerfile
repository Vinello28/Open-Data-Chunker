# ---- Builder Stage ----
FROM golang:1.23-bookworm AS builder

WORKDIR /build

# Install DuckDB build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy go module files first for better cache
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

# Copy source
COPY main.go .

# Tidy + download deps
RUN go mod tidy && go mod download

# Build static-linked binary (CGO needed for DuckDB)
RUN CGO_ENABLED=1 go build -o /build/server -ldflags="-s -w" .

# ---- Runtime Stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary
COPY --from=builder /build/server /app/server

# Copy static frontend files
COPY static/ /app/static/

# Create data dirs (will be overridden by volume mounts)
RUN mkdir -p /app/public/parquet /app/public/exports /app/public/classified /app/public/cache

EXPOSE 3000

ENV DATA_DIR=/app/public/parquet
ENV EXPORTS_DIR=/app/public/exports
ENV CLASSIFIED_DIR=/app/public/classified
ENV CACHE_DIR=/app/public/cache
ENV PORT=3000

CMD ["/app/server"]
