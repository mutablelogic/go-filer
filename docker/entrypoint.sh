#!/bin/sh
set -e

# If UID or GID are set, remap the filer user and group, so the server runs
# as that user and group, such as the owner of a mounted /data, or of the
# directories which are mounted as volumes. An ID which another user, or
# group, already has, such as the users group's 100, is shared with it.
if [ -n "${GID}" ] && [ "${GID}" != "$(id -g filer)" ]; then
    groupmod -o -g "${GID}" filer
fi
if [ -n "${UID}" ] && [ "${UID}" != "$(id -u filer)" ]; then
    usermod -o -u "${UID}" filer
fi

# The server keeps its data in /data, which is the filer user's, so a
# volume, or a directory which is mounted, which isn't, such as a new one,
# is made the filer user's
DATA="${XDG_DATA_HOME:-/data}"
mkdir -p "${DATA}"
if [ "$(stat -c %u "${DATA}")" != "$(id -u filer)" ] || [ "$(stat -c %g "${DATA}")" != "$(id -g filer)" ]; then
    chown -R filer:filer "${DATA}"
fi

# Default to "run" if no arguments are given
if [ $# -eq 0 ]; then
    set -- run
fi

# Run the server, or the command, as the filer user
exec gosu filer /usr/local/bin/filer "$@"
