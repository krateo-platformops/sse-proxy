# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o sse-proxy .

# ---------------------------------------------------------------------------
# Runtime stage – minimal image with no shell
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/sse-proxy /sse-proxy

EXPOSE 8080

# Run as the distroless nonroot user (uid/gid 65532). The base image already
# defaults to this user; declaring it explicitly satisfies Trivy DS-0002 and
# makes the non-root guarantee unambiguous to downstream scanners/orchestrators.
USER 65532:65532

ENTRYPOINT ["/sse-proxy"]
