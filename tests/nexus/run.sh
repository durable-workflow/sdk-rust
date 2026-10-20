#!/usr/bin/env bash
set -euo pipefail

fixture_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$fixture_dir/../.." && pwd)"
: "${NEXUS_STATE_DIR:?set the disposable state directory}"
: "${NEXUS_ENV_FILE:?set the Compose input file}"
export NEXUS_QUALIFY_ADMISSION=0 NEXUS_QUALIFY_OUTCOMES=0 NEXUS_QUALIFY_UI=0
source_sha="$(git -C "$repo_root" rev-parse HEAD)"
binary_sha="$(sha256sum "$NEXUS_STATE_DIR/target/debug/examples/nexus_cooperation" | cut -d ' ' -f 1)"
compose=(docker compose --env-file "$NEXUS_ENV_FILE" -f "$fixture_dir/docker-compose.yml")
owned=()
control_observer=''
cleanup() {
  for container in "${owned[@]}"; do
    docker stop --time 15 "$container" >/dev/null 2>&1 || true
    docker rm --volumes "$container" >/dev/null 2>&1 || true
  done
}
finish() {
  cleanup
  if [[ -n "$control_observer" ]]; then
    docker stop --time 5 "$control_observer" >/dev/null 2>&1 || true
    docker rm --volumes "$control_observer" >/dev/null 2>&1 || true
  fi
}
trap finish EXIT

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
  local worker_namespace="${NEXUS_NAMESPACE:-default}"
  if [[ "$role" == target ]]; then
    worker_namespace="${NEXUS_TARGET_NAMESPACE:-$worker_namespace}"
  fi
  "${compose[@]}" run -d --no-deps --name "$container" \
    -e "DURABLE_WORKFLOW_RUNTIME_NAMESPACE=$worker_namespace" \
    -e "DURABLE_WORKFLOW_RUNTIME_URL=${5:-http://server:8080}" \
    -e "NEXUS_ROLE=$role" -e "NEXUS_ENDPOINT=$case_id" \
    -e "TASK_QUEUE=$case_id-$role" -e "WORKER_ID=$worker_id" \
    -e "WORKFLOW_TYPE=nexus.$role" "$language" >/dev/null
  owned+=("$container")
}

ensure_control_observer() {
  if [[ -z "$control_observer" ]]; then
    control_observer="r87-control-observer-$(date -u +%s)-$$"
    "${compose[@]}" run -d --no-deps --name "$control_observer" \
      --entrypoint sleep observe infinity >/dev/null
  fi
}

admission_observe() {
  ensure_control_observer
  docker exec \
    -e "NEXUS_TARGET_NAMESPACE=${NEXUS_TARGET_NAMESPACE:-${NEXUS_NAMESPACE:-default}}" \
    -e "NEXUS_RUST_SOURCE=$source_sha" -e "NEXUS_RUST_BINARY_SHA256=$binary_sha" \
    "$control_observer" python /fixture/admissions.py "$@"
}

ack_observe() {
  # Each phase still starts a fresh Python process and reads the original
  # durable receipt. Repeated container creation must not consume the fixed
  # observation budget before the cold caller has a chance to execute.
  ensure_control_observer
  docker exec \
    -e "NEXUS_TARGET_NAMESPACE=$NEXUS_TARGET_NAMESPACE" \
    -e "NEXUS_RUST_SOURCE=$source_sha" -e "NEXUS_RUST_BINARY_SHA256=$binary_sha" \
    "$control_observer" python /fixture/acknowledgements.py "$@"
}

if [[ "${1:-}" == acknowledgement ]]; then
  export NEXUS_QUALIFY_ADMISSION=1
  for namespace in default nexus-source; do admission_observe setup "$namespace"; done
  mkdir -p "$NEXUS_STATE_DIR/acknowledgements"
  for pair in default:default nexus-source:nexus-source default:nexus-source nexus-source:default; do
    export NEXUS_NAMESPACE="${pair%%:*}" NEXUS_TARGET_NAMESPACE="${pair#*:}"
    for direction in rust:php php:rust rust:rust rust:python python:rust; do
      require_backend
      caller="${direction%%:*}"
      target="${direction#*:}"
      case_id="r87k-${pair//:/-}-${direction//:/-}-$(date -u +%s)-$RANDOM"
      caller_container="$case_id-caller"
      target_container="$case_id-target"
      proxy_container="$case_id-proxy"
      "${compose[@]}" run -d --no-deps --use-aliases --name "$proxy_container" ack-proxy >/dev/null
      owned+=("$proxy_container")
      start_worker "$target" target "$case_id-target" "$target_container"
      start_worker "$caller" caller "$case_id-caller" "$caller_container" http://ack-proxy:3000
      ack_observe prepare "$NEXUS_NAMESPACE" "$case_id" "$caller" "$target"
      docker inspect "$caller_container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/acknowledgements/$case_id-original-before-retirement.json"
      if [[ "$(docker inspect "$caller_container" --format '{{.State.Running}}')" == true ]]; then
        docker kill --signal KILL "$caller_container" >/dev/null
      fi
      docker logs "$caller_container" > "$NEXUS_STATE_DIR/acknowledgements/$case_id-original.log" 2>&1
      docker inspect "$caller_container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/acknowledgements/$case_id-original-exit.json"
      ack_observe retire "$NEXUS_NAMESPACE" "$case_id"
      ack_observe target "$NEXUS_NAMESPACE" "$case_id"
      caller_container="$case_id-caller-cold"
      start_worker "$caller" caller "$case_id-caller-cold" "$caller_container"
      ack_observe caller "$NEXUS_NAMESPACE" "$case_id"
      for role in caller target; do
        container="$caller_container"
        [[ "$role" == target ]] && container="$target_container"
        docker stop --time 15 "$container" >/dev/null
        docker logs "$container" > "$NEXUS_STATE_DIR/acknowledgements/$case_id-$role-final.log" 2>&1
        docker inspect "$container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/acknowledgements/$case_id-$role-exit.json"
      done
      ack_observe verify "$NEXUS_NAMESPACE" "$case_id"
      require_backend
      cleanup
      owned=()
    done
  done
  exit 0
fi

if [[ "${1:-cancellation}" == admission || "${1:-}" == cross-namespace ]]; then
  export NEXUS_QUALIFY_ADMISSION=1
  for namespace in default nexus-source; do admission_observe setup "$namespace"; done
  mkdir -p "$NEXUS_STATE_DIR/admissions"
  for namespace in default nexus-source; do
    export NEXUS_NAMESPACE="$namespace"
    export NEXUS_TARGET_NAMESPACE="$namespace"
    if [[ "$1" == cross-namespace ]]; then
      if [[ "$namespace" == default ]]; then
        NEXUS_TARGET_NAMESPACE=nexus-source
      else
        NEXUS_TARGET_NAMESPACE=default
      fi
    fi
    for direction in rust:php php:rust rust:rust rust:python python:rust; do
      caller="${direction%%:*}"
      target="${direction#*:}"
      for profile in sync-accepted async-completed async-accepted; do
        for schedule in ordinary cold; do
          require_backend
          case_id="r87a-${namespace}-${direction//:/-}-${profile}-${schedule}-$(date -u +%s)-$RANDOM"
          caller_container="$case_id-caller"
          target_container="$case_id-target"
          start_worker "$target" target "$case_id-target" "$target_container"
          start_worker "$caller" caller "$case_id-caller" "$caller_container"
          admission_observe prepare "$namespace" "$case_id" "$caller" "$target" "$profile" "$schedule"
          if [[ "$schedule" == cold ]]; then
            docker kill --signal KILL "$caller_container" >/dev/null
            docker logs "$caller_container" > "$NEXUS_STATE_DIR/admissions/$case_id-caller-original.log" 2>&1
            docker inspect "$caller_container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/admissions/$case_id-killed.json"
            jq -e '.ExitCode==137 and .OOMKilled==false' "$NEXUS_STATE_DIR/admissions/$case_id-killed.json" >/dev/null
            admission_observe retire "$namespace" "$case_id"
          else
            docker logs "$caller_container" > "$NEXUS_STATE_DIR/admissions/$case_id-caller-original.log" 2>&1
          fi
          admission_observe target "$namespace" "$case_id"
          if [[ "$schedule" == cold ]]; then
            caller_container="$case_id-caller-cold"
            start_worker "$caller" caller "$case_id-caller-cold" "$caller_container"
          fi
          admission_observe caller "$namespace" "$case_id"
          for role in caller target; do
            container="$caller_container"
            [[ "$role" == target ]] && container="$target_container"
            docker stop --time 15 "$container" >/dev/null
            docker logs "$container" > "$NEXUS_STATE_DIR/admissions/$case_id-$role-final.log" 2>&1
            docker inspect "$container" --format '{{json .State}}' > "$NEXUS_STATE_DIR/admissions/$case_id-$role-exit.json"
          done
          admission_observe verify "$namespace" "$case_id"
          require_backend
          cleanup
          owned=()
        done
      done
    done
  done
  exit 0
fi

if [[ "${1:-cancellation}" == outcomes || "${1:-}" == cross-outcomes || "${1:-}" == catalog ]]; then
  export NEXUS_QUALIFY_OUTCOMES=1
  if [[ "$1" == catalog ]]; then export NEXUS_QUALIFY_UI=1; fi
  for namespace in default nexus-source; do
    "${compose[@]}" run --rm --no-deps -T --entrypoint python observe /fixture/outcomes.py setup "$namespace"
  done
  if [[ "$1" == catalog ]]; then
    "${compose[@]}" run --rm --no-deps -T --entrypoint python observe /fixture/outcomes.py setup nexus-unrelated
    "${compose[@]}" --profile catalog up -d --no-build --wait catalog-default catalog-named catalog-unrelated
  fi
  for namespace in default nexus-source; do
    export NEXUS_NAMESPACE="$namespace"
    export NEXUS_TARGET_NAMESPACE="$namespace"
    if [[ "$1" == cross-outcomes || "$1" == catalog ]]; then
      if [[ "$namespace" == default ]]; then
        NEXUS_TARGET_NAMESPACE=nexus-source
      else
        NEXUS_TARGET_NAMESPACE=default
      fi
    fi
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
          -e "NEXUS_TARGET_NAMESPACE=$NEXUS_TARGET_NAMESPACE" \
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
  if [[ "$1" == catalog ]]; then
    "${compose[@]}" run --rm --no-deps -T \
      -e "NEXUS_RUST_SOURCE=$source_sha" -e "NEXUS_RUST_BINARY_SHA256=$binary_sha" browser
  fi
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
