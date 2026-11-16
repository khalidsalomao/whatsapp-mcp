GO ?= go

.PHONY: build test check
build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/whatsapp-mcp .

test:
	$(GO) test -race ./...

check:
	$(GO) vet ./...
	$(GO) test -race ./...
