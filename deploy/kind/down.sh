#!/usr/bin/env bash
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
for p in "$ROOT"/.cache/kind/*.pid; do [[ -f "$p" ]] && kill "$(cat "$p")" 2>/dev/null; rm -f "$p"; done
kind delete cluster --name streamcell
"$ROOT/hack/e2e-deps.sh" down
