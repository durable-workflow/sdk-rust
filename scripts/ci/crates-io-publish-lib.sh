#!/usr/bin/env bash

# Look up an exact crate version, retrying only failures that can be transient.
# The five attempts and exponential delays bound a failed lookup to 30 seconds
# of backoff while allowing crates.io to recover from a short interruption.
crates_io_version_lookup() {
    local version_api="$1"
    local version_response="$2"
    local user_agent="$3"
    local attempt
    local delay
    local http_status

    for ((attempt = 1; attempt <= 5; attempt++)); do
        http_status=""
        if http_status="$(curl --silent --show-error --location \
            --header "User-Agent: ${user_agent}" \
            --output "$version_response" \
            --write-out '%{http_code}' \
            "$version_api")"; then
            case "$http_status" in
                429 | 5??)
                    ;;
                *)
                    printf '%s\n' "$http_status"
                    return 0
                    ;;
            esac
        fi

        if ((attempt == 5)); then
            printf 'crates.io version lookup exhausted %d attempts' "$attempt" >&2
            if [[ -n "$http_status" ]]; then
                printf ' after HTTP %s' "$http_status" >&2
            else
                printf ' after a transport error' >&2
            fi
            printf '\n' >&2
            return 1
        fi

        delay=$((2 ** attempt))
        printf 'crates.io version lookup attempt %d failed transiently; retrying in %ds\n' \
            "$attempt" "$delay" >&2
        sleep "$delay"
    done
}

# Decide whether the exact package needs publishing. Callers remain responsible
# for post-publish visibility and archive/provenance verification.
publish_exact_crate_if_absent() {
    local http_status="$1"
    local manifest_path="$2"

    publish_error=""
    case "$http_status" in
        200)
            publish_outcome="already_published"
            publish_reason="exact_public_version_already_exists"
            ;;
        404)
            if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
                publish_error="registry_token_missing"
                return 1
            fi
            if ! cargo publish --manifest-path "$manifest_path" --registry crates-io; then
                publish_error="cargo_publish_failed"
                return 1
            fi
            publish_outcome="published"
            publish_reason="exact_public_version_published"
            ;;
        *)
            publish_error="registry_version_lookup_http_${http_status}"
            return 1
            ;;
    esac
}
