#!/bin/sh
set -e

# If FILER_UID or FILER_GID are set, remap the filer user/group so that
# files mounted from the host are accessible with the correct ownership.
if [ -n "${FILER_GID}" ] && [ "${FILER_GID}" != "$(id -g filer)" ]; then
    groupmod -g "${FILER_GID}" filer
fi
if [ -n "${FILER_UID}" ] && [ "${FILER_UID}" != "$(id -u filer)" ]; then
    usermod -u "${FILER_UID}" filer
fi

# Drop privileges and exec the server.
# Default to "run file://data/data" if no arguments are given.
if [ $# -eq 0 ]; then
    set -- run file://data/data
fi

# Ensure /data (and the TMPDIR subdirectory) is writable by the filer user.
# Bind-mounted host volumes inherit the host's ownership, which may differ
# from the container's filer UID/GID. Best-effort: skip silently when the
# filesystem disallows chown (NFS root_squash, read-only mounts, etc.).
# Only create/chown TMPDIR when it lives under /data to avoid accidentally
# modifying system paths like /tmp or /var/tmp.
if [ -d /data ]; then
    chown filer:filer /data 2>/dev/null || true
    case "${TMPDIR}" in
        /data/*)
            if [ ! -d "${TMPDIR}" ]; then
                mkdir -p "${TMPDIR}" 2>/dev/null || true
            fi
            if [ -d "${TMPDIR}" ]; then
                chown filer:filer "${TMPDIR}" 2>/dev/null || true
            fi
            ;;
    esac
fi

exec gosu filer /usr/local/bin/filer "$@"
