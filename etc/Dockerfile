ARG OS
ARG ARCH
ARG DEBIAN_CODENAME=trixie
ARG GO_MEDIA_VERSION=v1.8.3

# Build stage: compile all commands
FROM --platform=${OS}/${ARCH} golang:1.25-${DEBIAN_CODENAME} AS builder
ARG OS
ARG ARCH
ARG DEBIAN_CODENAME
ARG GO_MEDIA_VERSION
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /usr/src/app
COPY . .

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    cmake \
    make \
    git \
    nasm \
    pkg-config \
    libfreetype-dev \
    libmp3lame-dev \
    libopus-dev \
    libvorbis-dev \
    libvpx-dev \
    libx264-dev \
    libx265-dev \
    libnuma-dev \
    libzvbi-dev && \
    git clone --depth 1 --branch ${GO_MEDIA_VERSION} https://github.com/mutablelogic/go-media /opt/go-media && \
    make -C /opt/go-media ffmpeg && \
    PKG_CONFIG_PATH=/opt/go-media/build/install/lib/pkgconfig \
    CGO_LDFLAGS_ALLOW='-(W|D).*' \
    CGO_LDFLAGS='-lstdc++' \
    OS=${OS} ARCH=${ARCH} make build && \
    rm -rf /var/lib/apt/lists/*

# Runtime stage
FROM --platform=${OS}/${ARCH} debian:${DEBIAN_CODENAME}-slim
ARG OS
ARG ARCH
ARG DEBIAN_CODENAME
ENV DEBIAN_FRONTEND=noninteractive
ARG SOURCE
ARG VERSION
COPY --from=builder /usr/src/app/build/* /usr/local/bin/
COPY etc/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    gosu \
    libfreetype6 \
    libmp3lame0 \
    libnuma1 \
    libopus0 \
    libvorbis0a \
    libvorbisenc2 \
    libvpx9 \
    libx264-164 \
    libx265-215 \
    libzvbi0t64 && \
    rm -rf /var/lib/apt/lists/* && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    groupadd -g 1000 filer && \
    useradd -u 1000 -g 1000 -m -s /bin/false filer && \
    mkdir -p /data && chown filer:filer /data

# Labels
LABEL org.opencontainers.image.source=https://${SOURCE}
LABEL org.opencontainers.image.version=${VERSION}

# Default listen address: all interfaces so the port is reachable outside the container.
# Override with -e FILER_ADDR=<addr> at runtime.
ENV FILER_ADDR=:8087

# Data directory for file storage; mount a host volume here to persist data.
VOLUME /data

# Entrypoint runs as root, optionally remaps UID/GID via FILER_UID/FILER_GID,
# then drops to the filer user before exec-ing the server.
ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
