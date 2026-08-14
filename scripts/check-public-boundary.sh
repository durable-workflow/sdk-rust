#!/usr/bin/env bash
set -euo pipefail

root="${1:-.}"
cd "$root"

status=0

pattern_from_hex() {
  local hex="$1"
  local output=""
  local byte

  while [[ -n "$hex" ]]; do
    byte="${hex:0:2}"
    hex="${hex:2}"
    output+=$(printf '%b' "\\x$byte")
  done

  printf '%s' "$output"
}

file_patterns=(
  "$(pattern_from_hex 7a6f72706f726174696f6e2f)"
  "$(pattern_from_hex 2f776f726b73706163652f)"
  "$(pattern_from_hex 2f686f6d652f7673636f6465)"
  "$(pattern_from_hex 2f686f6d652f6c61622f776f726b73706163652d6871)"
  "$(pattern_from_hex 776f726b73706163652d6871)"
  "$(pattern_from_hex 706970656c696e652f)"
  "$(pattern_from_hex 5b63726f73732d7265706f2066726f6d20)"
  "$(pattern_from_hex 466f7267656a6f)"
  "$(pattern_from_hex 666f7267656a6f)"
  "$(pattern_from_hex 7761726d2d6c6f63616c)"
  "$(pattern_from_hex 72756e6e65725f70726f66696c65)"
)

provider_context_pattern="$(pattern_from_hex 6769746875622e7365727665725f75726c)"

reviewed_provider_context_reference() {
  local file="$1"
  local content="$2"
  local trimmed="${content#"${content%%[![:space:]]*}"}"
  local workflow_condition="if: \${{ ${provider_context_pattern} == 'https://github.com' }}"
  local contract_assertion="self.assertIn(\"${provider_context_pattern} == 'https://github.com'\", action_policy)"
  local contract_count="self.workflow.count(\"${provider_context_pattern} == 'https://github.com'\"),"

  [[ "$file" == ".github/workflows/ci.yml" && "$trimmed" == "$workflow_condition" ]] ||
    [[ "$file" == "scripts/ci/test-source-qualification.py" &&
      ( "$trimmed" == "$contract_assertion" || "$trimmed" == "$contract_count" ) ]]
}

metadata_patterns=(
  "${file_patterns[@]}"
  "$(pattern_from_hex 4c6f6f702d49443a)"
  "$(pattern_from_hex 2e746d702f6c6f6f70732f)"
  "$(pattern_from_hex 6c6f6f702d72756e6e6572)"
  "$(pattern_from_hex 4064757261626c652d776f726b666c6f772e6c6f63616c)"
)

pathspec=(
  .
  ':!.git'
  ':!vendor'
  ':!node_modules'
  ':!build'
  ':!dist'
  ':!coverage'
  ':!storage'
  ':!bootstrap/cache'
  ':!public/build'
  ':!var'
)

for pattern in "${file_patterns[@]}"; do
  while IFS=: read -r file line _; do
    [[ -n "${file:-}" ]] || continue
    printf 'public-boundary: forbidden file content at %s:%s\n' "$file" "$line" >&2
    status=1
  done < <(git grep -n -I -F -e "$pattern" -- "${pathspec[@]}" || true)
done

while IFS=: read -r file line content; do
  [[ -n "${file:-}" ]] || continue
  if reviewed_provider_context_reference "$file" "$content"; then
    continue
  fi
  printf 'public-boundary: forbidden file content at %s:%s\n' "$file" "$line" >&2
  status=1
done < <(git grep -n -I -F -e "$provider_context_pattern" -- "${pathspec[@]}" || true)

if [[ -n "${PUBLIC_BOUNDARY_GIT_RANGE:-}" ]]; then
  read -r -a rev_args <<< "$PUBLIC_BOUNDARY_GIT_RANGE"
else
  rev_args=(-1 HEAD)
fi

if mapfile -t commits < <(git rev-list "${rev_args[@]}" 2>/dev/null); then
  for commit in "${commits[@]}"; do
    metadata="$(git show -s --format='%an <%ae>%n%s%n%b' "$commit")"

    for pattern in "${metadata_patterns[@]}"; do
      if grep -Fq -- "$pattern" <<< "$metadata"; then
        printf 'public-boundary: forbidden commit metadata at %s\n' "${commit:0:12}" >&2
        status=1
        break
      fi
    done
  done
else
  printf 'public-boundary: unable to inspect commit metadata range: %s\n' "${PUBLIC_BOUNDARY_GIT_RANGE:-HEAD}" >&2
  status=1
fi

exit "$status"
