ARG OS
ARG ARCH

# Build stage: compile all commands
FROM --platform=${OS}/${ARCH} golang:latest AS builder
ARG OS
ARG ARCH
WORKDIR /usr/src/app
COPY . .

RUN \
    apt update -y && \
    OS=${OS} ARCH=${ARCH} make build

# Runtime stage: minimal debian image
FROM --platform=${OS}/${ARCH} debian:trixie-slim
ARG OS
ARG ARCH
ARG SOURCE
COPY --from=builder /usr/src/app/build/* /usr/local/bin/
COPY etc/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN apt update -y && apt install -y ca-certificates gosu passwd && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    groupadd -g 1000 filer && \
    useradd -u 1000 -g 1000 -m -s /bin/false filer && \
    mkdir -p /data && chown filer:filer /data

# Labels
LABEL org.opencontainers.image.source=https://${SOURCE}

# Default listen address: all interfaces so the port is reachable outside the container.
# Override with -e FILER_ADDR=<addr> at runtime.
ENV FILER_ADDR=:8087

# Data directory for file storage; mount a host volume here to persist data.
VOLUME /data

# Entrypoint runs as root, optionally remaps UID/GID via FILER_UID / FILER_GID,
# then drops to the filer user before exec-ing the server.
ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
