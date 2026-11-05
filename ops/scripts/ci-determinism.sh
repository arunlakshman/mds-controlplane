#!/usr/bin/env bash
# CI gate for the deterministic packages of the control plane:
#   internal/runtime, internal/controlplane/contract, and the control-plane
#   domain packages that pass the check today (module, groups/storage, replica, metadb/engine),
#   and the reference model (internal/spec). Test code is not in the gate, so
#   the property-based tests (pgregory.net/rapid) are not either.
# The simulator engine (internal/verify/simulator) is checked too, with one more
# allowed import: context, because the in-memory log API takes a context. Its
# campaign runner (cmd/cpsim) uses goroutines and wall time on purpose,
# to run many seeds at once, so it is not in the gate.
# Other control-plane packages (engine/memory, topics, groups, metadb, membership) use sync,
# time or other banned things and are not in this gate yet.
#   1. determinismcheck: exact-path import allow-list plus banned constructs.
#   2. Every third-party module they depend on has a reviewed audit record
#      for the exact version in go.mod (so a version bump forces a re-audit).
set -euo pipefail
cd "$(dirname "$0")/../.."

pkgs=(
  ./internal/runtime/...
  ./internal/controlplane/contract/...
  ./internal/controlplane/metadb/engine
  ./internal/controlplane/module
  ./internal/controlplane/groups/storage
  ./internal/controlplane/replica
  ./internal/spec
)

sim=./internal/verify/simulator

echo "== determinismcheck"
go run ./ops/tools/determinismcheck "${pkgs[@]}"

echo "== determinismcheck (simulator engine)"
allow=$(mktemp)
trap 'rm -f "$allow"' EXIT
cat ops/tools/determinismcheck/allow.txt >"$allow"
echo context >>"$allow"
go run ./ops/tools/determinismcheck -allow "$allow" "$sim"

echo "== real time stays out of the gated packages"
# Only live and the production adapters may read real time or real randomness.
# The import allow-list lets every package of this module in, so check the
# whole dependency tree of each gated package for the real-time packages.
realtime='internal/system/(live|realclock)$|manifesterio/dst/(realclock|syncclock)$'
for p in "${pkgs[@]}" "$sim"; do
  if bad=$(go list -deps "$p" | grep -E "$realtime"); then
    echo "gated package $p depends on a real-time package:" >&2
    echo "$bad" >&2
    exit 1
  fi
done

echo "== dependency audit records"
# Only code that the deterministic packages really run needs an audit record.
# The generated gRPC API (internal/controlplane/api/v1) is the input/output
# edge, so we do not follow imports into it: grpc, protobuf and their
# dependencies are not audited here. The packages still may import it for type
# definitions only (decision 57).
edge=github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1
roots=$(go list "${pkgs[@]}" "$sim" | tr '\n' ' ')
missing=0
while read -r modver; do
  [[ -z $modver ]] && continue
  record="docs/dep-audits/$modver.md"
  if [[ -f $record ]]; then
    echo "ok       $modver"
  else
    echo "MISSING  $record  (add an audit record for this exact version)"
    missing=1
  fi
done < <(
  # Line format: import path, module (or "-"), then the imports of that package.
  go list -deps -f '{{.ImportPath}} {{if and .Module (not .Module.Main)}}{{.Module.Path}}@{{.Module.Version}}{{else}}-{{end}} {{join .Imports " "}}' "${pkgs[@]}" "$sim" |
    awk -v edge="$edge" -v roots="$roots" '
      { mod[$1] = $2; n = split($0, f, " "); imps[$1] = ""; for (i = 3; i <= n; i++) imps[$1] = imps[$1] " " f[i] }
      END {
        nr = split(roots, r, " "); top = 0
        for (i = 1; i <= nr; i++) stack[++top] = r[i]
        while (top > 0) {
          p = stack[top--]
          if (p in seen || p == edge) continue
          seen[p] = 1
          if (mod[p] != "" && mod[p] != "-") out[mod[p]] = 1
          m = split(imps[p], q, " ")
          for (i = 1; i <= m; i++) if (!(q[i] in seen)) stack[++top] = q[i]
        }
        for (k in out) print k
      }' | sort -u
)
# An explicit test: a failing [[ ]] does not stop old bash (macOS) under set -e.
if [[ $missing != 0 ]]; then
  exit 1
fi

echo "determinism gate passed"
