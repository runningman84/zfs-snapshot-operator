.PHONY: help build test docker-build docker-push clean fmt vet lint

# Variables
BINARY_NAME=zfs-snapshot-operator
DOCKER_IMAGE?=ghcr.io/runningman84/zfs-snapshot-operator
VERSION?=latest
GOOS?=$(shell go env GOOS)
GOARCH?=$(shell go env GOARCH)

help: ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the operator binary
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o bin/$(BINARY_NAME) ./cmd/main.go

test: ## Run tests
	go test -v -race -coverprofile=coverage.out ./...

coverage: test ## Generate coverage report
	go tool cover -html=coverage.out -o coverage.html

fmt: ## Format code
	go fmt ./...

vet: ## Run go vet
	go vet ./...

lint: ## Run golangci-lint (requires golangci-lint installed)
	golangci-lint run

docker-build: ## Build Docker image
	docker build -f Dockerfile.go -t $(DOCKER_IMAGE):$(VERSION) .

docker-push: ## Push Docker image
	docker push $(DOCKER_IMAGE):$(VERSION)

clean: ## Clean build artifacts
	rm -rf bin/ coverage.out coverage.html

run: ## Run the operator locally (requires kubeconfig)
	go run ./cmd/main.go -kubeconfig=$(HOME)/.kube/config

install-deps: ## Download dependencies
	go mod download
	go mod tidy

.DEFAULT_GOAL := help
