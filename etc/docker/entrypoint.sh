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
exec gosu filer /usr/local/bin/filer "$@"
