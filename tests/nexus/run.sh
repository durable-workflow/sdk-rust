#!/usr/bin/env bash
set -euo pipefail

fixture_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$fixture_dir/../.." && pwd)"
: "${NEXUS_STATE_DIR:?set the disposable state directory}"
: "${NEXUS_ENV_FILE:?set the Compose input file}"
source_sha="$(git -C "$repo_root" rev-parse HEAD)"
binary_sha="$(sha256sum "$NEXUS_STATE_DIR/target/debug/examples/nexus_cooperation" | cut -d ' ' -f 1)"
compose=(docker compose --env-file "$NEXUS_ENV_FILE" -f "$fixture_dir/docker-compose.yml")
owned=()
cleanup() {
  for container in "${owned[@]}"; do
    docker stop --time 15 "$container" >/dev/null 2>&1 || true
    docker rm --volumes "$container" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

require_backend() {
  local service container
  for service in server queue repair mysql redis; do
    container="$("${compose[@]}" ps -q "$service")"
    [[ -n "$container" ]]
    [[ "$(docker inspect "$container" --format '{{.State.Running}}')" == true ]]
  done
}

observe() {
  "${compose[@]}" run --rm --no-deps -T \
    -e "NEXUS_RUST_SOURCE=$source_sha" -e "NEXUS_RUST_BINARY_SHA256=$binary_sha" observe "$@"
}
start_worker() {
  local language="$1" role="$2" worker_id="$3" container="$4"
  "${compose[@]}" run -d --no-deps --name "$container" \
    -e "NEXUS_ROLE=$role" -e "NEXUS_ENDPOINT=$case_id" \
    -e "TASK_QUEUE=$case_id-$role" -e "WORKER_ID=$worker_id" \
    -e "WORKFLOW_TYPE=nexus.$role" "$language" >/dev/null
  owned+=("$container")
}

if [[ "${1:-cancellation}" == outcomes ]]; then
  export NEXUS_QUALIFY_OUTCOMES=1
  for namespace in default nexus-source; do
    export NEXUS_NAMESPACE="$namespace"
    "${compose[@]}" run --rm --no-deps -T --entrypoint python observe /fixture/outcomes.py setup "$namespace"
    for direction in rust:php php:rust rust:rust rust:python python:rust; do
      caller="${direction%%:*}"
      target="${direction#*:}"
      for scenario in success failure external missing; do
        require_backend
        case_id="r87o-${namespace}-${direction//:/-}-${scenario}-$(date -u +%s)-$RANDOM"
        caller_container="$case_id-caller"
        target_container="$case_id-target"
        start_worker "$target" target "$case_id-target" "$target_container"
        start_worker "$caller" caller "$case_id-caller" "$caller_container"
        "${compose[@]}" run --rm --no-deps -T --entrypoint python \
          -e "NEXUS_RUST_SOURCE=$source_sha" -e "NEXUS_RUST_BINARY_SHA256=$binary_sha" \
          observe /fixture/outcomes.py run "$namespace" "$case_id" "$caller" "$target" "$scenario"
        for container in "$caller_container" "$target_container"; do
          docker logs "$container" > "$NEXUS_STATE_DIR/outcomes/$container.log" 2>&1
          docker stop --time 15 "$container" >/dev/null
          docker inspect "$container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/outcomes/$container-exit.json"
          jq -e '.ExitCode==0 and .OOMKilled==false' "$NEXUS_STATE_DIR/outcomes/$container-exit.json" >/dev/null
        done
        jq '.outcome="pass" | .worker_exits=[{role:"caller",exit_code:0,oom_killed:false},{role:"target",exit_code:0,oom_killed:false}]' \
          "$NEXUS_STATE_DIR/outcomes/$case_id.json" > "$NEXUS_STATE_DIR/outcomes/$case_id.finished"
        mv -- "$NEXUS_STATE_DIR/outcomes/$case_id.finished" "$NEXUS_STATE_DIR/outcomes/$case_id.json"
        require_backend
        cleanup
        owned=()
      done
    done
  done
  exit 0
fi

for direction in rust:php php:rust rust:rust rust:python python:rust rust:rust:kill; do
  require_backend
  caller="${direction%%:*}"
  rest="${direction#*:}"
  target="${rest%%:*}"
  case_id="r87-${direction//:/-}-$(date -u +%s)-$RANDOM"
  caller_container="$case_id-caller"
  target_container="$case_id-target"
  printf 'Nexus original %s\n' "$case_id"
  start_worker "$target" target "$case_id-target" "$target_container"
  start_worker "$caller" caller "$case_id-caller" "$caller_container"
  observe prepare "$case_id" "$caller" "$target"
  observe started "$case_id"
  observe cancel "$case_id"
  observe cleanup "$case_id"
  if [[ "$direction" == rust:rust:kill ]]; then
    docker logs "$target_container" > "$NEXUS_STATE_DIR/cases/$case_id-target-killed.log" 2>&1
    docker kill --signal KILL "$target_container" >/dev/null
    docker inspect "$target_container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/cases/$case_id-killed.json"
    jq -e '.ExitCode==137 and .OOMKilled==false' "$NEXUS_STATE_DIR/cases/$case_id-killed.json" >/dev/null
    target_container="$case_id-target-cold"
    start_worker "$target" target "$case_id-target-cold" "$target_container"
  fi
  observe terminal "$case_id"
  docker logs "$caller_container" > "$NEXUS_STATE_DIR/cases/$case_id-caller.log" 2>&1
  docker logs "$target_container" > "$NEXUS_STATE_DIR/cases/$case_id-target.log" 2>&1
  observe verify "$case_id"
  for container in "$caller_container" "$target_container"; do
    docker stop --time 15 "$container" >/dev/null
    docker inspect "$container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/cases/$container-exit.json"
    jq -e '.ExitCode==0 and .OOMKilled==false' "$NEXUS_STATE_DIR/cases/$container-exit.json" >/dev/null
  done
  observe finished "$case_id"
  require_backend
  cleanup
  owned=()
done
