#!/bin/sh
set -eu
source=$1
shift
if [ "${1-}" = '--child' ]; then
    shift
    credentials=$1
    shift
    unset DW_AUTH_TOKEN DW_SERVER_KEY APP_KEY
    # All roles, including builds/databases, wait for verified effective caps.
    while [ ! -f /credentials/ready ]; do sleep 0.1; done
    case "$credentials" in
        none) ;;
        auth|server|both) ;;
        *) exit 125;;
    esac
    if [ "$credentials" = auth ] || [ "$credentials" = both ]; then
        DW_AUTH_TOKEN=$(cat /credentials/token)
        case "$DW_AUTH_TOKEN" in fixture-*) hex=${DW_AUTH_TOKEN#fixture-};; *) exit 125;; esac
        [ "${#hex}" -eq 48 ] || exit 125
        case "$hex" in *[!0-9a-f]*) exit 125;; esac
        export DW_AUTH_TOKEN
    fi
    if [ "$credentials" = server ] || [ "$credentials" = both ]; then
        DW_SERVER_KEY=$(cat /credentials/server-key)
        case "$DW_SERVER_KEY" in base64:*) key=${DW_SERVER_KEY#base64:};; *) exit 125;; esac
        [ "${#key}" -eq 44 ] || exit 125
        case "$key" in *[!a-zA-Z0-9+/=]*) exit 125;; esac
        export DW_SERVER_KEY
    fi
    exec "$@"
fi
deadline=$1
grace=$2
shift 2
case "$deadline:$grace" in *[!0-9:]*|:*|*:) exit 125;; esac
remaining=$((deadline - $(date +%s)))
[ "$remaining" -gt 0 ] || exit 124
# Re-execute captured inline bytes, never a mutable bind-mounted script path.
exec timeout --signal=TERM --kill-after="${grace}s" "${remaining}s" \
    /bin/sh -c "$source" -- "$source" --child "$@"
