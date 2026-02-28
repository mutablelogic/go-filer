#!/bin/sh
set -e

# If UID or GID are set, remap the filer user/group so that
# files mounted from the host are accessible with the correct ownership.
if [ -n "${GID}" ] && [ "${GID}" != "$(id -g filer)" ]; then
    groupmod -g "${GID}" filer
fi
if [ -n "${UID}" ] && [ "${UID}" != "$(id -u filer)" ]; then
    usermod -u "${UID}" filer
fi

# Drop privileges and exec the server.
# Default to "run file://data/data" if no arguments are given.
if [ $# -eq 0 ]; then
    set -- run file://data/data
fi
exec gosu filer /usr/local/bin/filer "$@"
