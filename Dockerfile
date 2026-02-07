# Build stage
FROM golang:1.25-alpine AS builder

# Version can be passed as build argument
ARG VERSION=dev

# Install build dependencies
RUN apk add --no-cache git make

WORKDIR /workspace

# Copy go mod files
COPY go.mod ./

# Copy source code (needed for go mod tidy to resolve all dependencies)
COPY cmd/ cmd/
COPY pkg/ pkg/

# Download dependencies and populate go.sum with all transitive dependencies
RUN go mod download && go mod tidy

# Build the operator with version
RUN CGO_ENABLED=0 GOOS=linux go build -v -ldflags="-w -s -X main.Version=${VERSION}" -o operator ./cmd/operator

# Runtime stage
FROM alpine:3.23.3@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /workspace/operator .

# Default mode is direct
ENTRYPOINT ["/app/operator", "-mode", "direct"]
