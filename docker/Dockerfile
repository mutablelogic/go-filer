# syntax=docker/dockerfile:1
ARG OS=linux
ARG ARCH=amd64
ARG GO_VERSION=1.27
ARG NODE_VERSION=24

# Node, which builds the web UI, of its own image, as Debian's is older
# than the UI needs
FROM --platform=${OS}/${ARCH} node:${NODE_VERSION}-trixie-slim AS node

# Build the filer command on the Go image, with cgo, and go-media's
# libraries, which make downloads from its release, of the version in
# go.mod, so it reads the metadata of audio, video and photos
FROM --platform=${OS}/${ARCH} golang:${GO_VERSION}-trixie AS builder
ARG OS
ARG ARCH
WORKDIR /usr/src/app

# The libraries go-media's link with, as the CI's Linux build installs them,
# and g++, of libstdc++
RUN set -x && apt-get update -y \
    && apt-get install -y --no-install-recommends g++ pkg-config curl \
        libde265-dev libaom-dev libdav1d-dev libxml2-dev liblzma-dev zlib1g-dev libssl-dev libzstd-dev \
    && rm -rf /var/lib/apt/lists/*
COPY --from=node /usr/local/bin/node /usr/local/bin/
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && node --version && npm --version

# Download the modules first, so they're only downloaded again when they
# change
COPY go.mod go.sum ./
RUN go mod download

# Build the command, with the web UI, whose @mutablelogic packages are
# installed from GitHub Packages, with the github_token secret, which isn't
# kept in the image. The Debian packages of the libraries it's linked with
# are listed, so the image has them, and only them.
COPY . .
RUN --mount=type=secret,id=github_token,env=GITHUB_TOKEN set -x \
    && OS=${OS} ARCH=${ARCH} CGO_ENABLED=1 make all \
    && install -m 0755 docker/entrypoint.sh build/entrypoint.sh \
    && ldd build/filer | awk '$3 ~ /^\// { print $3 } $1 ~ /^\// { print $1 }' \
        | xargs -r realpath | xargs -r dpkg -S | cut -d: -f1 | sort -u > build/packages \
    && cat build/packages

# Copy the command to /usr/local/bin, on Debian, with the libraries it's
# linked with, the CA certificates backends' APIs are called with, the time
# zones of calendars' events, the types of files' extensions, of
# /etc/mime.types, and gosu, which the entrypoint drops privileges with, to
# the filer user, which the server runs as
FROM --platform=${OS}/${ARCH} debian:trixie-slim
ARG SOURCE
COPY --from=builder /usr/src/app/build/packages /tmp/packages
RUN set -x && apt-get update -y \
    && apt-get install -y --no-install-recommends ca-certificates tzdata media-types gosu $(cat /tmp/packages) \
    && groupadd --system filer \
    && useradd --system --gid filer --no-create-home --shell /usr/sbin/nologin filer \
    && install -d -o filer -g filer /data \
    && rm -rf /var/lib/apt/lists/* /tmp/packages

COPY --from=builder /usr/src/app/build/filer /usr/src/app/build/entrypoint.sh /usr/local/bin/

# Check the command runs, with its libraries
RUN /usr/local/bin/filer --version

# Label the image
LABEL org.opencontainers.image.source=${SOURCE}

# The server keeps its credentials, and the index of its volumes, in
# /data/filer, which is its data directory, and its volumes' caches, which
# can be cleared, in /data/cache/filer, and listens on every interface. A
# volume of a directory is of one which is mounted, such as /media.
ENV XDG_DATA_HOME=/data
ENV XDG_CACHE_HOME=/data/cache

# The server listens on every interface in the container, as it must for
# Docker to forward a published port to it. Which interfaces of the host
# reach it is how the port is published: the server has no authentication
# yet, so publish it on localhost only, with -p 127.0.0.1:8084:8084, rather
# than -p 8084:8084, which is every interface, unless it's behind a proxy
# which signs people in, and never on the internet.
ENV ADDR=0.0.0.0:8084
VOLUME /data
EXPOSE 8084
STOPSIGNAL SIGTERM

# Entrypoint runs as root, optionally remaps the filer user's UID and GID,
# makes /data the filer user's, then drops to the filer user before exec-ing
# filer
ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
