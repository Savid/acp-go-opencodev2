.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION ?= v2.14.0
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: audit build clean coverage-check fmt fmt-check help lint modernize-check test test-integration-live test-integration-smoke tidy vuln

## build: build all packages and the command binary
build:
	go build ./...
	go build -o .tmp/acp-go-opencodev2 ./cmd/acp-go-opencodev2

GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

## coverage-check: run shuffled race tests and report statement coverage
coverage-check:
	go test -race -shuffle=on -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 { found = 1 } END { if (!found) { print "coverage profile has no statement blocks"; exit 1 } }' coverage.out
	@report=$$(go tool cover -func=coverage.out) || exit $$?; printf '%s\n' "$$report" | awk '/^total:/ { found = 1; if ($$3 !~ /^[0-9]+([.][0-9]+)?%$$/) { print "invalid total coverage line"; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: run integration tests against the installed opencode without spending tokens
test-integration-smoke:
	ACP_GO_OPENCODEV2_RUN_LIVE_TOKENS=0 ACP_GO_OPENCODEV2_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=300s -v ./integration/...

## test-integration-live: run integration tests that spend model tokens
test-integration-live:
	ACP_GO_OPENCODEV2_RUN_LIVE_TOKENS=1 ACP_GO_OPENCODEV2_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=900s -v ./integration/...

## lint: run pinned golangci-lint
lint:
	$(GOLANGCI_LINT) run --timeout=10m --allow-parallel-runners ./...

## fmt: format code with golangci-lint
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
	$(GOLANGCI_LINT) fmt ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

## tidy: verify module files are tidy
tidy:
	go mod tidy -diff

## vuln: run govulncheck from the go.mod tool directive
vuln:
	go tool govulncheck ./...

## modernize-check: check Go modernizations without changing files
modernize-check:
	go fix -diff ./...

## audit: run local checks
audit:
	$(MAKE) fmt-check
	$(MAKE) lint
	$(MAKE) build
	$(MAKE) coverage-check
	$(MAKE) tidy
	$(MAKE) vuln
	$(MAKE) modernize-check
	go mod verify

## clean: remove build artifacts
clean:
	rm -rf .tmp coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
