#!/usr/bin/env bash
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
for p in "$ROOT"/.cache/kind/*.pid; do [[ -f "$p" ]] && kill "$(cat "$p")" 2>/dev/null; rm -f "$p"; done
pkill -f "port-forward --address 127.0.0.1 .* 8443:443" 2>/dev/null
docker rm -f streamcell-gw-relay >/dev/null 2>&1
kind delete cluster --name streamcell
"$ROOT/hack/e2e-deps.sh" down
