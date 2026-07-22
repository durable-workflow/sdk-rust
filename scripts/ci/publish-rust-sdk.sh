#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=crates-io-publish-lib.sh
source "$script_dir/crates-io-publish-lib.sh"

manifest_path="${RUST_SDK_MANIFEST_PATH:-Cargo.toml}"
evidence_path="${RUST_SDK_RELEASE_EVIDENCE_PATH:-rust-sdk-release-evidence.json}"
release_tag="${RELEASE_TAG:-}"
release_commit="${RELEASE_COMMIT:-}"
release_run_id="${RELEASE_RUN_ID:-}"
release_run_attempt="${RELEASE_RUN_ATTEMPT:-}"

for command in cargo curl git jq sha256sum tar; do
    if ! command -v "$command" >/dev/null 2>&1; then
        printf 'required command not found: %s\n' "$command" >&2
        exit 1
    fi
done

if [[ ! -f "$manifest_path" ]]; then
    printf 'Rust SDK manifest not found: %s\n' "$manifest_path" >&2
    exit 1
fi

if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
    printf 'Rust SDK publication requires a clean release checkout\n' >&2
    exit 1
fi

metadata="$(cargo metadata --manifest-path "$manifest_path" --no-deps --format-version 1)"
package_name="$(jq -er '.packages[0].name' <<<"$metadata")"
package_version="$(jq -er '.packages[0].version' <<<"$metadata")"
package_rust_version="$(jq -er '.packages[0].rust_version' <<<"$metadata")"
package_repository="$(jq -er '.packages[0].repository' <<<"$metadata")"
package_documentation="$(jq -er '.packages[0].documentation' <<<"$metadata")"
target_directory="$(jq -er '.target_directory' <<<"$metadata")"
product_train="$(jq -er '.packages[0].metadata["durable-workflow"]["product-train"]' <<<"$metadata")"
server_compatibility="$(jq -er '.packages[0].metadata["durable-workflow"]["supported-server-versions"]' <<<"$metadata")"
worker_protocol="$(jq -er '.packages[0].metadata["durable-workflow"]["worker-protocol-version"]' <<<"$metadata")"
control_plane="$(jq -er '.packages[0].metadata["durable-workflow"]["control-plane-version"]' <<<"$metadata")"
query_tasks="$(jq -er '.packages[0].metadata["durable-workflow"]["query-tasks"]' <<<"$metadata")"
query_task_minimum_protocol="$(jq -er '.packages[0].metadata["durable-workflow"]["query-task-minimum-worker-protocol-version"]' <<<"$metadata")"
replayed_instance_state_queries="$(jq -er '.packages[0].metadata["durable-workflow"]["replayed-instance-state-queries"]' <<<"$metadata")"
query_state_model="$(jq -er '.packages[0].metadata["durable-workflow"]["query-state-model"]' <<<"$metadata")"
snapshot_inspection_queries="$(jq -er '.packages[0].metadata["durable-workflow"]["snapshot-inspection-queries"]' <<<"$metadata")"
child_workflows="$(jq -er '.packages[0].metadata["durable-workflow"]["child-workflows"]' <<<"$metadata")"
child_workflow_command="$(jq -er '.packages[0].metadata["durable-workflow"]["child-workflow-command"]' <<<"$metadata")"
child_workflow_failure_reasons="$(jq -cer '.packages[0].metadata["durable-workflow"]["child-workflow-failure-reasons"]' <<<"$metadata")"
deterministic_side_effects="$(jq -er '.packages[0].metadata["durable-workflow"]["deterministic-side-effects"]' <<<"$metadata")"
side_effect_command="$(jq -er '.packages[0].metadata["durable-workflow"]["side-effect-command"]' <<<"$metadata")"
side_effect_history_event="$(jq -er '.packages[0].metadata["durable-workflow"]["side-effect-history-event"]' <<<"$metadata")"
version_markers="$(jq -er '.packages[0].metadata["durable-workflow"]["version-markers"]' <<<"$metadata")"
version_marker_command="$(jq -er '.packages[0].metadata["durable-workflow"]["version-marker-command"]' <<<"$metadata")"
version_marker_history_event="$(jq -er '.packages[0].metadata["durable-workflow"]["version-marker-history-event"]' <<<"$metadata")"
version_marker_helpers="$(jq -cer '.packages[0].metadata["durable-workflow"]["version-marker-helpers"]' <<<"$metadata")"

if [[ "$package_name" != "durable-workflow" ]]; then
    printf 'unexpected Rust SDK package name: %s\n' "$package_name" >&2
    exit 1
fi
if [[ ! "$package_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
    printf 'Rust SDK package version must be exact SemVer: %s\n' "$package_version" >&2
    exit 1
fi
if [[ "$package_rust_version" != "1.86" ]]; then
    printf 'unexpected Rust SDK minimum Rust version: %s\n' "$package_rust_version" >&2
    exit 1
fi
if [[ "$package_repository" != "https://github.com/durable-workflow/sdk-rust" ]]; then
    printf 'unexpected Rust SDK repository metadata: %s\n' "$package_repository" >&2
    exit 1
fi
if [[ "$package_documentation" != "https://rust.durable-workflow.com/" ]]; then
    printf 'unexpected Rust SDK documentation metadata: %s\n' "$package_documentation" >&2
    exit 1
fi
if [[ "$product_train" != "$package_version" || "$server_compatibility" != "$product_train" ]]; then
    printf 'Rust SDK package, product train, and supported server must share one release: %s, %s, %s\n' "$package_version" "$product_train" "$server_compatibility" >&2
    exit 1
fi
if [[ "$worker_protocol" != "1.2" || "$control_plane" != "2" || "$query_tasks" != "true" || "$query_task_minimum_protocol" != "1.8" || "$replayed_instance_state_queries" != "true" || "$query_state_model" != "deterministic-workflow-replay" || "$snapshot_inspection_queries" != "true" || "$child_workflows" != "true" || "$child_workflow_command" != "start_child_workflow" || "$child_workflow_failure_reasons" != '["child_workflow","cancelled","terminated"]' || "$deterministic_side_effects" != "true" || "$side_effect_command" != "record_side_effect" || "$side_effect_history_event" != "SideEffectRecorded" || "$version_markers" != "true" || "$version_marker_command" != "record_version_marker" || "$version_marker_history_event" != "VersionMarkerRecorded" || "$version_marker_helpers" != '["patched","deprecate_patch"]' ]]; then
    printf 'unexpected Rust SDK compatibility metadata\n' >&2
    exit 1
fi
if [[ ! "$release_tag" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
    printf 'RELEASE_TAG must identify an exact SDK SemVer release: %s\n' "${release_tag:-<empty>}" >&2
    exit 1
fi
if [[ "$release_tag" != "$package_version" ]]; then
    printf 'release tag %s does not match package version %s\n' "$release_tag" "$package_version" >&2
    exit 1
fi

head_commit="$(git rev-parse HEAD)"
tag_commit="$(git rev-list -n 1 "$release_tag" 2>/dev/null || true)"
if [[ -z "$tag_commit" || "$tag_commit" != "$head_commit" ]]; then
    printf 'release tag %s must point at checkout commit %s\n' "$release_tag" "$head_commit" >&2
    exit 1
fi
if [[ -n "$release_commit" && "$release_commit" != "$head_commit" ]]; then
    printf 'release commit mismatch: expected %s, got %s\n' "$head_commit" "$release_commit" >&2
    exit 1
fi
release_commit="$head_commit"

registry_api="https://crates.io/api/v1/crates/${package_name}"
version_api="${registry_api}/${package_version}"
download_url="${version_api}/download"
user_agent="durable-workflow-sdk-rust/${package_version} (support@durable-workflow.com)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
version_response="$tmp_dir/version.json"
crate_response="$tmp_dir/crate.json"
published_archive="$tmp_dir/published.crate"

local_checksum=""
download_checksum=""
published_checksum=""
published_at=""
published_repository=""
archive_vcs_commit=""
archive_vcs_dirty=""

write_evidence() {
    local outcome="$1"
    local reason="$2"
    local registry_verified="${3:-false}"

    jq -n \
        --arg schema "durable-workflow.rust-sdk.release-evidence" \
        --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg package "$package_name" \
        --arg version "$package_version" \
        --arg source "crates.io://${package_name}@${package_version}" \
        --arg download_url "$download_url" \
        --arg registry_checksum "$published_checksum" \
        --arg local_checksum "$local_checksum" \
        --arg download_checksum "$download_checksum" \
        --arg published_at "$published_at" \
        --arg repository "$package_repository" \
        --arg published_repository "$published_repository" \
        --arg documentation "$package_documentation" \
        --arg product_train "$product_train" \
        --arg server_compatibility "$server_compatibility" \
        --arg worker_protocol "$worker_protocol" \
        --arg control_plane "$control_plane" \
        --arg query_tasks "$query_tasks" \
        --arg query_task_minimum_protocol "$query_task_minimum_protocol" \
        --arg replayed_instance_state_queries "$replayed_instance_state_queries" \
        --arg query_state_model "$query_state_model" \
        --arg snapshot_inspection_queries "$snapshot_inspection_queries" \
        --arg child_workflows "$child_workflows" \
        --arg child_workflow_command "$child_workflow_command" \
        --argjson child_workflow_failure_reasons "$child_workflow_failure_reasons" \
        --arg deterministic_side_effects "$deterministic_side_effects" \
        --arg side_effect_command "$side_effect_command" \
        --arg side_effect_history_event "$side_effect_history_event" \
        --arg version_markers "$version_markers" \
        --arg version_marker_command "$version_marker_command" \
        --arg version_marker_history_event "$version_marker_history_event" \
        --argjson version_marker_helpers "$version_marker_helpers" \
        --arg release_tag "$release_tag" \
        --arg release_commit "$release_commit" \
        --arg release_run_id "$release_run_id" \
        --arg release_run_attempt "$release_run_attempt" \
        --arg archive_vcs_commit "$archive_vcs_commit" \
        --arg archive_vcs_dirty "$archive_vcs_dirty" \
        --arg outcome "$outcome" \
        --arg reason "$reason" \
        --arg registry_verified "$registry_verified" \
        '{
            schema: $schema,
            version: 1,
            generated_at: $generated_at,
            package: $package,
            package_version: $version,
            package_source: $source,
            package_download_url: $download_url,
            registry_checksum_sha256: $registry_checksum,
            local_package_checksum_sha256: $local_checksum,
            downloaded_package_checksum_sha256: $download_checksum,
            published_at: $published_at,
            repository_provenance: {
                package_metadata_repository: $repository,
                crates_io_repository: $published_repository,
                documentation: $documentation,
                archive_vcs_commit: $archive_vcs_commit,
                archive_vcs_dirty: ($archive_vcs_dirty == "true")
            },
            product_train: $product_train,
            supported_server_versions: $server_compatibility,
            protocol_compatibility: {
                worker_protocol: $worker_protocol,
                control_plane: $control_plane,
                query_tasks: ($query_tasks == "true"),
                query_task_minimum_worker_protocol: $query_task_minimum_protocol,
                replayed_instance_state_queries: ($replayed_instance_state_queries == "true"),
                query_state_model: $query_state_model,
                snapshot_inspection_queries: ($snapshot_inspection_queries == "true"),
                child_workflows: ($child_workflows == "true"),
                child_workflow_command: $child_workflow_command,
                child_workflow_failure_reasons: $child_workflow_failure_reasons,
                deterministic_side_effects: ($deterministic_side_effects == "true"),
                side_effect_command: $side_effect_command,
                side_effect_history_event: $side_effect_history_event,
                version_markers: ($version_markers == "true"),
                version_marker_command: $version_marker_command,
                version_marker_history_event: $version_marker_history_event,
                version_marker_helpers: $version_marker_helpers
            },
            release: {
                sdk_tag: $release_tag,
                commit: $release_commit,
                run_id: $release_run_id,
                run_attempt: $release_run_attempt
            },
            outcome: $outcome,
            reason: $reason,
            registry_verified: ($registry_verified == "true"),
            exact_registry_archive_matches_release_checkout: ($registry_verified == "true")
        }' > "$evidence_path"
}

if ! cargo package --manifest-path "$manifest_path"; then
    write_evidence "failed" "local_package_archive_build_failed"
    exit 1
fi

local_archive="${target_directory}/package/${package_name}-${package_version}.crate"
if [[ ! -f "$local_archive" ]]; then
    write_evidence "failed" "local_package_archive_missing"
    printf 'local Cargo package archive was not created: %s\n' "$local_archive" >&2
    exit 1
fi
local_checksum="$(sha256sum "$local_archive" | awk '{print $1}')"
vcs_info_path="${package_name}-${package_version}/.cargo_vcs_info.json"
if ! tar -xOf "$local_archive" "$vcs_info_path" > "$tmp_dir/vcs.json"; then
    write_evidence "failed" "local_package_vcs_provenance_missing"
    printf 'local Cargo package archive is missing VCS provenance\n' >&2
    exit 1
fi
archive_vcs_commit="$(jq -er '.git.sha1' "$tmp_dir/vcs.json")"
archive_vcs_dirty="$(jq -r '.git.dirty // false' "$tmp_dir/vcs.json")"
if [[ "$archive_vcs_commit" != "$release_commit" || "$archive_vcs_dirty" != "false" ]]; then
    write_evidence "failed" "local_package_vcs_provenance_mismatch"
    printf 'local package VCS provenance does not match the clean release commit\n' >&2
    exit 1
fi

write_evidence "pending" "checking_public_registry"

if ! http_status="$(crates_io_version_lookup "$version_api" "$version_response" "$user_agent")"; then
    write_evidence "failed" "registry_version_lookup_transient_failure_exhausted"
    printf 'could not reach a conclusive crates.io version API response\n' >&2
    exit 1
fi

if ! publish_exact_crate_if_absent "$http_status" "$manifest_path"; then
    write_evidence "failed" "$publish_error"
    case "$publish_error" in
        registry_token_missing)
            printf 'CARGO_REGISTRY_TOKEN is required to publish %s %s\n' "$package_name" "$package_version" >&2
            ;;
        registry_version_lookup_http_*)
            printf 'crates.io version lookup failed with HTTP %s\n' "$http_status" >&2
            ;;
    esac
    exit 1
fi

if [[ "$publish_outcome" == "published" ]]; then
    for _attempt in $(seq 1 24); do
        http_status="$(curl --silent --show-error --location \
            --header "User-Agent: ${user_agent}" \
            --output "$version_response" \
            --write-out '%{http_code}' \
            "$version_api" || true)"
        [[ "$http_status" == "200" ]] && break
        sleep 5
    done
    if [[ "$http_status" != "200" ]]; then
        write_evidence "failed" "published_version_not_visible_after_registry_deadline"
        printf 'published Rust SDK version did not become visible at %s\n' "$version_api" >&2
        exit 1
    fi
fi

if ! curl --fail --silent --show-error --location \
    --header "User-Agent: ${user_agent}" \
    --output "$crate_response" \
    "$registry_api"; then
    write_evidence "failed" "registry_package_metadata_lookup_failed"
    exit 1
fi

published_repository="$(jq -er '.crate.repository' "$crate_response")"
published_version="$(jq -er '.version.num' "$version_response")"
published_checksum="$(jq -er '.version.checksum' "$version_response")"
published_at="$(jq -er '.version.created_at' "$version_response")"

if [[ "$published_repository" != "$package_repository" ]]; then
    write_evidence "failed" "published_repository_provenance_mismatch"
    printf 'published crate repository mismatch: expected %s, got %s\n' "$package_repository" "$published_repository" >&2
    exit 1
fi
if [[ "$published_version" != "$package_version" ]]; then
    write_evidence "failed" "published_version_mismatch"
    printf 'published crate version mismatch: expected %s, got %s\n' "$package_version" "$published_version" >&2
    exit 1
fi
if [[ ! "$published_checksum" =~ ^[0-9a-f]{64}$ ]]; then
    write_evidence "failed" "published_checksum_missing"
    printf 'published crate checksum is missing or invalid\n' >&2
    exit 1
fi

if ! curl --fail --silent --show-error --location \
    --header "User-Agent: ${user_agent}" \
    --output "$published_archive" \
    "$download_url"; then
    write_evidence "failed" "published_package_archive_download_failed"
    exit 1
fi
download_checksum="$(sha256sum "$published_archive" | awk '{print $1}')"

if [[ "$download_checksum" != "$published_checksum" ]]; then
    write_evidence "failed" "published_download_checksum_mismatch"
    printf 'downloaded crate checksum does not match the crates.io registry checksum\n' >&2
    exit 1
fi
if [[ "$local_checksum" != "$published_checksum" ]]; then
    write_evidence "failed" "published_source_archive_mismatch"
    printf 'published crate archive differs from the exact release checkout package\n' >&2
    exit 1
fi

write_evidence "$publish_outcome" "$publish_reason" "true"
printf 'Rust SDK %s %s is available from crates.io (%s).\n' "$package_name" "$package_version" "$publish_outcome"
