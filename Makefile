# Executables
GO ?= $(shell which go 2>/dev/null)
GOWORK ?= off

# Locations
BUILD_DIR ?= build

# The platforms the packages are vetted for, as each has its own way of
# watching files, and only some are tested
VET_PLATFORMS ?= darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64 freebsd/amd64

###############################################################################
# ALL

.PHONY: all
all: test

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
