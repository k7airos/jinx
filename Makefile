GO ?= go
GOFLAGS_MAC = -ldflags=-linkmode=external

.PHONY: test race vet check build-check

test:
	$(GO) test $(GOFLAGS_MAC) ./... -count=1 -timeout=180s

race:
	$(GO) test -race $(GOFLAGS_MAC) ./... -count=1 -timeout=240s

vet:
	$(GO) vet ./...

# Multiple package targets check compilation without leaving executable files.
build-check:
	$(GO) build $(GOFLAGS_MAC) ./cmd/...

check: test vet build-check
