# Executables
GO ?= $(shell which go 2>/dev/null)
GOWORK ?= off

# Locations
BUILD_DIR ?= build
CMD_DIR := $(wildcard cmd/*)

# Set OS and Architecture
ARCH ?= $(shell arch | tr A-Z a-z | sed 's/x86_64/amd64/' | sed 's/i386/amd64/' | sed 's/armv7l/arm/' | sed 's/aarch64/arm64/')
OS ?= $(shell uname | tr A-Z a-z)

# Set build flags
VERSION_PKG = github.com/mutablelogic/go-server/pkg/version
BUILD_LD_FLAGS += -X $(VERSION_PKG).GitTag=$(shell git describe --tags --always)
BUILD_LD_FLAGS += -X $(VERSION_PKG).GitBranch=$(shell git name-rev HEAD --name-only --always)
BUILD_FLAGS = -ldflags "-s -w ${BUILD_LD_FLAGS}"

# The platforms the packages are vetted for, as each has its own way of
# watching files, and only some are tested
VET_PLATFORMS ?= darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64 freebsd/amd64

###############################################################################
# ALL

.PHONY: all
all: build

###############################################################################
# BUILD

# Build the commands in the cmd directory, with cgo, which watching files on
# macOS needs
.PHONY: build
build: tidy $(CMD_DIR)

$(CMD_DIR): go-dep mkdir
	@echo Build $(notdir $@) GOOS=${OS} GOARCH=${ARCH}
	@GOWORK=${GOWORK} GOOS=${OS} GOARCH=${ARCH} CGO_ENABLED=1 ${GO} build ${BUILD_FLAGS} -o ${BUILD_DIR}/$(notdir $@) ./$@

# Build the client, which is the filer command without the server, with the
# "client" tag, and without cgo. It's built as build/filer, in place of the
# server.
.PHONY: client
client: go-dep mkdir
	@echo Build filer client GOOS=${OS} GOARCH=${ARCH}
	@GOWORK=${GOWORK} GOOS=${OS} GOARCH=${ARCH} CGO_ENABLED=0 ${GO} build -tags client ${BUILD_FLAGS} -o ${BUILD_DIR}/filer ./cmd/filer

###############################################################################
# TEST

.PHONY: test
test: vet unit-test coverage-test

# Vet the packages for each platform, with cgo, which watching files on macOS
# needs, for this platform, and without it for the others
.PHONY: vet
vet: go-dep
	@echo Vet
	@GOWORK=${GOWORK} ${GO} vet ./...
	@GOWORK=${GOWORK} ${GO} vet -tags client ./...
	@for platform in ${VET_PLATFORMS}; do \
		echo "Vet $$platform"; \
		GOWORK=${GOWORK} GOOS=$${platform%/*} GOARCH=$${platform#*/} CGO_ENABLED=0 ${GO} vet ./... || exit 1; \
	done

.PHONY: unit-test
unit-test: go-dep
	@echo Unit Tests
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${GO} test ./...

# The tests with the race detector, which needs cgo, as watching files on
# macOS does
.PHONY: race-test
race-test: go-dep
	@echo Race Tests
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${GO} test -race ./...

.PHONY: coverage-test
coverage-test: go-dep mkdir
	@echo Test Coverage
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${GO} test -v -coverprofile ${BUILD_DIR}/coverprofile.out ./...
	@${GO} tool cover -func ${BUILD_DIR}/coverprofile.out > ${BUILD_DIR}/coverage.txt

###############################################################################
# CLEAN

# Other rules
.PHONY: mkdir
mkdir:
	@install -d $(BUILD_DIR)

.PHONY: tidy
tidy: go-dep
	@echo 'go tidy'
	@GOWORK=${GOWORK} $(GO) mod tidy

.PHONY: clean
clean:
	@echo 'clean'
	@rm -fr $(BUILD_DIR)
	@$(GO) clean

###############################################################################
# DEPENDENCIES

.PHONY: go-dep
go-dep:
	@test -f "${GO}" && test -x "${GO}"  || (echo "Missing go binary" && exit 1)
