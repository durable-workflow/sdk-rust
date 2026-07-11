#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=crates-io-publish-lib.sh
source "$script_dir/crates-io-publish-lib.sh"

fail() {
    printf 'FAIL: %s\n' "$1" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    local message="$3"
    [[ "$actual" == "$expected" ]] || fail "$message (expected $expected, got $actual)"
}

test_transient_lookup_recovers() (
    local tmp_dir
    local status
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    printf 'transport\n429\n503\n200\n' > "$tmp_dir/responses"
    printf '0\n' > "$tmp_dir/calls"

    curl() {
        local call
        local response
        call="$(<"$tmp_dir/calls")"
        call=$((call + 1))
        printf '%d\n' "$call" > "$tmp_dir/calls"
        response="$(sed -n "${call}p" "$tmp_dir/responses")"
        if [[ "$response" == "transport" ]]; then
            return 7
        fi
        printf '%s' "$response"
    }
    sleep() {
        printf '%s\n' "$1" >> "$tmp_dir/sleeps"
    }

    status="$(crates_io_version_lookup \
        'https://crates.io/api/v1/crates/durable-workflow/0.1.4' \
        "$tmp_dir/version.json" 'test-agent')"

    assert_eq "200" "$status" "transient lookup did not recover"
    assert_eq "4" "$(<"$tmp_dir/calls")" "transient lookup used the wrong number of attempts"
    assert_eq $'2\n4\n8' "$(<"$tmp_dir/sleeps")" "transient lookup used unexpected backoff"
)

test_transient_lookup_exhausts() (
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    printf '0\n' > "$tmp_dir/calls"

    curl() {
        local call
        call="$(<"$tmp_dir/calls")"
        printf '%d\n' "$((call + 1))" > "$tmp_dir/calls"
        printf '503'
    }
    sleep() {
        :
    }

    if crates_io_version_lookup \
        'https://crates.io/api/v1/crates/durable-workflow/0.1.4' \
        "$tmp_dir/version.json" 'test-agent'; then
        fail "exhausted transient lookup unexpectedly succeeded"
    fi
    assert_eq "5" "$(<"$tmp_dir/calls")" "transient lookup did not stop at its attempt bound"
)

test_404_publishes_once() (
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    printf '0\n' > "$tmp_dir/publishes"
    export CARGO_REGISTRY_TOKEN="test-token"

    cargo() {
        local count
        [[ "$*" == "publish --manifest-path Cargo.toml --registry crates-io" ]] || return 2
        count="$(<"$tmp_dir/publishes")"
        printf '%d\n' "$((count + 1))" > "$tmp_dir/publishes"
    }

    publish_exact_crate_if_absent "404" "Cargo.toml"
    assert_eq "1" "$(<"$tmp_dir/publishes")" "404 did not authorize exactly one publish"
    assert_eq "published" "$publish_outcome" "404 returned the wrong publication outcome"
)

test_200_is_idempotent() (
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT
    printf '0\n' > "$tmp_dir/publishes"

    cargo() {
        printf '1\n' > "$tmp_dir/publishes"
    }

    publish_exact_crate_if_absent "200" "Cargo.toml"
    assert_eq "0" "$(<"$tmp_dir/publishes")" "200 attempted to publish an existing version"
    assert_eq "already_published" "$publish_outcome" "200 returned the wrong idempotent outcome"
)

test_transient_lookup_recovers
test_transient_lookup_exhausts
test_404_publishes_once
test_200_is_idempotent
printf 'crates.io publication tests passed\n'
