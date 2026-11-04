#!/bin/sh
set -eu

if [ "${1-}" = '--command' ]; then
    shift
    credentials=$1
    shift
    unset DW_AUTH_TOKEN DW_SERVER_KEY APP_KEY
    # Every container has its own tmpfs. A missing/partial stdin delivery never
    # starts a credentialed process; the enclosing timeout still expires.
    case "$credentials" in
        none) exec "$@";;
        auth | server | both) ;;
        *) exit 125;;
    esac
    while [ ! -f /credentials/ready ]; do sleep 0.1; done
    if [ "$credentials" = auth ] || [ "$credentials" = both ]; then
        DW_AUTH_TOKEN=$(cat /credentials/token)
        case "$DW_AUTH_TOKEN" in fixture-*) hex=${DW_AUTH_TOKEN#fixture-};; *) exit 125;; esac
        [ "${#hex}" -eq 48 ] || exit 125
        case "$hex" in *[!0-9a-f]*) exit 125;; esac
        export DW_AUTH_TOKEN
    fi
    if [ "$credentials" = server ] || [ "$credentials" = both ]; then
        DW_SERVER_KEY=$(cat /credentials/server-key)
        case "$DW_SERVER_KEY" in base64:*) raw=${DW_SERVER_KEY#base64:};; *) exit 125;; esac
        [ "${#raw}" -eq 44 ] || exit 125
        case "$raw" in *[!a-zA-Z0-9+/=]*) exit 125;; esac
        export DW_SERVER_KEY
    fi
    exec "$@"
fi

deadline=$1
grace=$2
shift 2
case "$deadline:$grace" in *[!0-9:]* | :* | *:) exit 125;; esac
remaining=$((deadline - $(date +%s)))
[ "$remaining" -gt 0 ] || exit 124
# GNU timeout supervises the complete entrypoint process group. KILL follows
# TERM even if the child ignores TERM, independently of the Docker client.
exec timeout --signal=TERM --kill-after="${grace}s" "${remaining}s" \
    /bin/sh "$0" --command "$@"
