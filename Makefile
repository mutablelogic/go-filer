# The default goal is all, as the targets of go-media are before it
.DEFAULT_GOAL := all

# Executables
GO ?= $(shell which go 2>/dev/null)
GOWORK ?= off
NPM ?= $(shell which npm 2>/dev/null)
PROTOC ?= $(shell which protoc 2>/dev/null)
PROTOC_GEN_GO ?= $(shell which protoc-gen-go 2>/dev/null)
PROTOC_GEN_GO_GRPC ?= $(shell which protoc-gen-go-grpc 2>/dev/null)

# Locations
BUILD_DIR ?= build
CMD_DIR := $(wildcard cmd/*)
TS_DIR := frontend

# Set OS and Architecture
ARCH ?= $(shell arch | tr A-Z a-z | sed 's/x86_64/amd64/' | sed 's/i386/amd64/' | sed 's/armv7l/arm/' | sed 's/aarch64/arm64/')
OS ?= $(shell uname | tr A-Z a-z)

# Set build flags
VERSION_PKG = github.com/mutablelogic/go-server/pkg/version
BUILD_LD_FLAGS += -X $(VERSION_PKG).GitTag=$(shell git describe --tags --always)
BUILD_LD_FLAGS += -X $(VERSION_PKG).GitBranch=$(shell git name-rev HEAD --name-only --always)
BUILD_FLAGS = -ldflags "-s -w ${BUILD_LD_FLAGS}"

# The store of the indexes of volumes, of the server: sqlite, which is the
# default, memory, or pg, as the build tag indexer_<backend>
INDEXER_BACKEND ?= sqlite
ifeq ($(filter $(INDEXER_BACKEND),memory sqlite pg),)
$(error INDEXER_BACKEND is memory, sqlite or pg, not "$(INDEXER_BACKEND)")
endif
BUILD_TAGS = indexer_$(INDEXER_BACKEND)$(if $(wildcard $(TS_DIR)/app/dist/index.html),$(comma)frontend)

###############################################################################
# GO-MEDIA

# With cgo, the server enriches files with go-metadata's handlers of audio,
# video, and photos' EXIF, HEIF and RAW, which read them with go-media's
# bindings of FFmpeg 8, libexif, libheif, LibRaw, Chromaprint and libfvad.
# They're the libraries of the go-media version in go.mod, which its releases
# have built already, for macOS, Linux and Windows. GO_MEDIA_PREFIX is where
# they're installed: build/go-media, where "make go-media" unpacks the
# archive of the release, which the builds, and tests, with cgo, do first,
# or a build of go-media's libraries, such as build/install in a clone of it,
# after "make" in it, or /opt/go-media in go-media's dev image.
GO_MEDIA_VERSION := $(shell awk '$$1 == "github.com/mutablelogic/go-media" { print $$2 }' go.mod)
GO_MEDIA_LICENSE ?= lgpl
GO_MEDIA_DIR = $(BUILD_DIR)/go-media
GO_MEDIA_PREFIX ?= $(GO_MEDIA_DIR)
GO_MEDIA_OS = $(if $(filter linux darwin,$(OS)),$(OS),windows)
GO_MEDIA_ARCHIVE = go-media-$(GO_MEDIA_VERSION:v%=%)-$(GO_MEDIA_LICENSE)-$(GO_MEDIA_OS)-$(ARCH).$(if $(filter windows,$(GO_MEDIA_OS)),zip,tar.gz)
GO_MEDIA_URL = https://github.com/mutablelogic/go-media/releases/download/$(GO_MEDIA_VERSION)/$(GO_MEDIA_ARCHIVE)

# The archive is downloaded when the libraries are its, rather than another
# build's
GO_MEDIA_DEP = $(if $(filter $(GO_MEDIA_DIR),$(GO_MEDIA_PREFIX)),go-media)

# go-media's libraries include libfvad and Chromaprint, so audio is
# classified, and fingerprinted, and looked up on AcoustID
GO_TAGS = libfvad,chromaprint

# Point pkg-config and cgo at go-media's libraries, rather than whatever's on
# the system, with the flags of its static libraries which cgo doesn't allow
# by default: their -W and -D flags, and LibRaw's -Xpreprocessor, of OpenMP.
# FFmpeg links some Homebrew libraries, such as lame (-lmp3lame), on macOS,
# without their directories in its pkg-config files, so those are added to
# the linker's search path, from each keg rather than Homebrew's prefix. The
# linker warns about the libraries which more than one of them link, which
# isn't a problem.
BREW ?= $(if $(filter darwin,$(OS)),$(shell command -v brew 2>/dev/null))
BREW_LIB_DIRS := $(if $(BREW),$(wildcard $(foreach f,gettext libpng lame,$(shell $(BREW) --prefix $(f) 2>/dev/null)/lib)))
CGO_LDFLAGS_MEDIA = -lstdc++ $(foreach d,$(BREW_LIB_DIRS),-L$(d)) $(if $(filter darwin,$(OS)),-Wl$(comma)-no_warn_duplicate_libraries)
CGO_ENV = PKG_CONFIG_PATH="$(abspath $(GO_MEDIA_PREFIX))/lib/pkgconfig" CGO_CFLAGS_ALLOW="-Xpreprocessor|-(W|D).*" CGO_LDFLAGS_ALLOW="-Xpreprocessor|-(W|D).*" CGO_LDFLAGS="$(strip $(CGO_LDFLAGS) $(CGO_LDFLAGS_MEDIA))"

# Download and unpack the archive of go-media's libraries, for the version in
# go.mod, in build/go-media, once for each archive, which is named for its
# version, license, OS and architecture, so changing any of them downloads
# the archive for them
GO_MEDIA_STAMP = $(GO_MEDIA_DIR)/.$(GO_MEDIA_ARCHIVE)

.PHONY: go-media
go-media: $(GO_MEDIA_STAMP)

$(GO_MEDIA_STAMP):
	@test -n "$(GO_MEDIA_VERSION)" || { echo "go-media isn't in go.mod" >&2; exit 1; }
	@echo "Download $(GO_MEDIA_ARCHIVE)"
	@rm -rf $(GO_MEDIA_DIR) && install -d $(GO_MEDIA_DIR)
	@curl --fail --location --silent --show-error --retry 3 -o $(BUILD_DIR)/$(GO_MEDIA_ARCHIVE) $(GO_MEDIA_URL)
	@$(if $(filter windows,$(GO_MEDIA_OS)),unzip -q -o $(BUILD_DIR)/$(GO_MEDIA_ARCHIVE) -d $(GO_MEDIA_DIR),tar -xzf $(BUILD_DIR)/$(GO_MEDIA_ARCHIVE) -C $(GO_MEDIA_DIR))
	@rm -f $(BUILD_DIR)/$(GO_MEDIA_ARCHIVE)
	@touch $@

###############################################################################
# VET

# The platforms the packages are vetted for, as each has its own way of
# watching files, and only some are tested
VET_PLATFORMS ?= linux/amd64 darwin/arm64 windows/amd64 freebsd/amd64

###############################################################################
# ALL

.PHONY: all
all: build

###############################################################################
# BUILD

# Build the web UI, and then the commands in the cmd directory, which embed
# it, with cgo, which watching files on macOS needs, and go-media's
# libraries, which enriching media does, or without cgo, with
# CGO_ENABLED=0, which doesn't need them, but reads less metadata, and can't
# watch volumes on macOS. The commands are built by another make, so they
# embed the UI which was just built.
.PHONY: build
build: frontend
	@$(MAKE) --no-print-directory tidy $(CMD_DIR)

CGO_ENABLED ?= 1
CGO_BUILD_ENV = $(if $(filter 1,$(CGO_ENABLED)),$(CGO_ENV))
CGO_BUILD_TAGS = $(BUILD_TAGS)$(if $(filter 1,$(CGO_ENABLED)),$(comma)$(GO_TAGS))
CGO_BUILD_DEP = $(if $(filter 1,$(CGO_ENABLED)),$(GO_MEDIA_DEP))
comma := ,

$(CMD_DIR): go-dep mkdir $(CGO_BUILD_DEP)
	@echo Build $(notdir $@) GOOS=${OS} GOARCH=${ARCH} INDEXER_BACKEND=${INDEXER_BACKEND} CGO_ENABLED=${CGO_ENABLED}
	@GOWORK=${GOWORK} GOOS=${OS} GOARCH=${ARCH} CGO_ENABLED=${CGO_ENABLED} ${CGO_BUILD_ENV} ${GO} build -tags "${CGO_BUILD_TAGS}" ${BUILD_FLAGS} -o ${BUILD_DIR}/$(notdir $@) ./$@

# Build the client, which is the filer command without the server, with the
# "client" tag, and without cgo. It's built as build/filer, in place of the
# server.
.PHONY: client
client: go-dep mkdir
	@echo Build filer client GOOS=${OS} GOARCH=${ARCH}
	@GOWORK=${GOWORK} GOOS=${OS} GOARCH=${ARCH} CGO_ENABLED=0 ${GO} build -tags client ${BUILD_FLAGS} -o ${BUILD_DIR}/filer ./cmd/filer

###############################################################################
# FRONTEND

# Build the web UI's modules into frontend/dist, and the server's app into
# frontend/app/dist, which the commands embed, with the "frontend" tag, from
# package-lock.json, once they're type checked. It needs npm, and a token
# which can read GitHub Packages, in GITHUB_TOKEN. Carbon Charts' install
# telemetry is turned off.
.PHONY: frontend
frontend: npm-dep
	@echo Build frontend
	@cd $(TS_DIR) && IBM_TELEMETRY_DISABLED=true ${NPM} ci --silent && ${NPM} run --silent check && ${NPM} run --silent build && ${NPM} run --silent build-app

.PHONY: frontend-test
frontend-test: npm-dep
	@echo Frontend Tests
	@cd $(TS_DIR) && IBM_TELEMETRY_DISABLED=true ${NPM} ci --silent && ${NPM} run --silent check && ${NPM} test --silent

###############################################################################
# GENERATE

# Generate the code of the protocol buffers, such as SeaweedFS's filer
# client, with protoc, which is committed, so it's only needed when the
# .proto files change
.PHONY: protobuf-generate
protobuf-generate: go-dep protoc-dep
	@echo Generate
	@GOWORK=${GOWORK} ${GO} generate ./...

###############################################################################
# TEST

.PHONY: test
test: vet unit-test coverage-test

# Vet the packages for each platform, with cgo, which watching files on macOS
# needs, for this platform, and without it for the others
.PHONY: vet
vet: go-dep $(GO_MEDIA_DEP)
	@echo Vet
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${CGO_ENV} ${GO} vet -tags "${GO_TAGS}" ./...
	@$(MAKE) --no-print-directory vet-platform PLATFORM=client
	@for platform in ${VET_PLATFORMS}; do \
		$(MAKE) --no-print-directory vet-platform PLATFORM=$$platform || exit 1; \
	done

# Vet the packages of one platform, without cgo, such as
# PLATFORM=windows/amd64, or the client's, with PLATFORM=client, which CI
# runs at once, a platform of each job
.PHONY: vet-platform
vet-platform: go-dep
	@echo "Vet ${PLATFORM}"
	@if [ "${PLATFORM}" = "client" ]; then \
		GOWORK=${GOWORK} CGO_ENABLED=0 ${GO} vet -tags client ./...; \
	else \
		GOWORK=${GOWORK} GOOS=$(word 1,$(subst /, ,${PLATFORM})) GOARCH=$(word 2,$(subst /, ,${PLATFORM})) CGO_ENABLED=0 ${GO} vet ./...; \
	fi

.PHONY: unit-test
unit-test: go-dep $(GO_MEDIA_DEP)
	@echo Unit Tests
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${CGO_ENV} ${GO} test -tags "${GO_TAGS}" ./...

# The tests with the race detector, which needs cgo, as watching files on
# macOS does, and so go-media's libraries
.PHONY: race-test
race-test: go-dep $(GO_MEDIA_DEP)
	@echo Race Tests
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${CGO_ENV} ${GO} test -tags "${GO_TAGS}" -race ./...

.PHONY: coverage-test
coverage-test: go-dep mkdir $(GO_MEDIA_DEP)
	@echo Test Coverage
	@GOWORK=${GOWORK} CGO_ENABLED=1 ${CGO_ENV} ${GO} test -tags "${GO_TAGS}" -v -coverprofile ${BUILD_DIR}/coverprofile.out ./...
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
	@rm -fr $(BUILD_DIR) $(TS_DIR)/dist $(TS_DIR)/app/dist
	@$(GO) clean

###############################################################################
# DEPENDENCIES

.PHONY: go-dep
go-dep:
	@test -f "${GO}" && test -x "${GO}"  || (echo "Missing go binary" && exit 1)

# protoc and its plugins for Go and gRPC, which brew installs with
# "brew install protobuf protoc-gen-go protoc-gen-go-grpc"
.PHONY: protoc-dep
protoc-dep:
	@test -f "${PROTOC}" && test -x "${PROTOC}" || (echo "Missing protoc binary: brew install protobuf" && exit 1)
	@test -f "${PROTOC_GEN_GO}" && test -x "${PROTOC_GEN_GO}" || (echo "Missing protoc-gen-go binary: brew install protoc-gen-go" && exit 1)
	@test -f "${PROTOC_GEN_GO_GRPC}" && test -x "${PROTOC_GEN_GO_GRPC}" || (echo "Missing protoc-gen-go-grpc binary: brew install protoc-gen-go-grpc" && exit 1)

.PHONY: npm-dep
npm-dep:
	@test -f "${NPM}" && test -x "${NPM}"  || (echo "Missing npm binary, which builds the web UI" && exit 1)
	@test -n "$${GITHUB_TOKEN}" || (echo "Missing GITHUB_TOKEN, which needs read:packages to install @mutablelogic packages from GitHub Packages" && exit 1)
