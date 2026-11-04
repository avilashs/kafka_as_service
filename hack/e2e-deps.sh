#!/usr/bin/env bash
# Starts the dependencies of the e2e suite and prints the environment it needs.
#
#   eval "$(hack/e2e-deps.sh up)"   # start Postgres (docker), Temporal dev server; fetch envtest
#   go test -tags e2e ./test/e2e/ -v
#   hack/e2e-deps.sh down
#
# Pinned tools are downloaded into .cache/ and verified by checksum.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE="$ROOT/.cache"
BIN="$CACHE/bin"
mkdir -p "$BIN"

TEMPORAL_CLI_VERSION=1.10.0
TEMPORAL_CLI_SHA256=521f461f3d108902763bb71e85cd0fd382dc9357b59604ae03da6ae734d16890
ENVTEST_K8S_VERSION=1.34.1
ENVTEST_SHA512=c5e7c237ae18a8c65d0df1214b864ecd19aa9ff4f1383dbd477c202546d778c0f74efe15750ec38f57d8de26ba17cd62f404446cdd3c975399a5d4589de35cdd
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:17}"
PG_PORT="${PG_PORT:-55432}"
TEMPORAL_PORT="${TEMPORAL_PORT:-57233}"

fetch_tools() {
  if [[ ! -x "$BIN/temporal" ]]; then
    local t="$CACHE/temporal.tgz"
    curl -sSfL -o "$t" "https://github.com/temporalio/cli/releases/download/v${TEMPORAL_CLI_VERSION}/temporal_cli_${TEMPORAL_CLI_VERSION}_linux_amd64.tar.gz"
    echo "$TEMPORAL_CLI_SHA256  $t" | sha256sum -c - >&2
    tar -xzf "$t" -C "$BIN" temporal
    rm -f "$t"
  fi
  if [[ ! -x "$CACHE/envtest/kube-apiserver" ]]; then
    local e="$CACHE/envtest.tgz"
    curl -sSfL -o "$e" "https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v${ENVTEST_K8S_VERSION}/envtest-v${ENVTEST_K8S_VERSION}-linux-amd64.tar.gz"
    echo "$ENVTEST_SHA512  $e" | sha512sum -c - >&2
    mkdir -p "$CACHE/envtest"
    tar -xzf "$e" -C "$CACHE/envtest" --strip-components=2
    rm -f "$e"
  fi
}

up() {
  fetch_tools
  if ! docker inspect streamcell-e2e-pg >/dev/null 2>&1; then
    docker run -d --name streamcell-e2e-pg -e POSTGRES_PASSWORD=e2e -p "127.0.0.1:${PG_PORT}:5432" "$POSTGRES_IMAGE" >/dev/null
  elif [[ "$(docker inspect -f '{{.State.Running}}' streamcell-e2e-pg)" != true ]]; then
    docker start streamcell-e2e-pg >/dev/null # left over from an earlier session
  fi
  for _ in $(seq 1 60); do
    docker exec streamcell-e2e-pg pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 1
  done
  if ! curl -s "http://127.0.0.1:$((TEMPORAL_PORT + 1000))" >/dev/null 2>&1; then
    nohup "$BIN/temporal" server start-dev --headless --ip 127.0.0.1 --port "$TEMPORAL_PORT" \
      --ui-port "$((TEMPORAL_PORT + 1000))" --log-level warn >"$CACHE/temporal.log" 2>&1 &
    echo $! >"$CACHE/temporal.pid"
  fi
  for _ in $(seq 1 60); do
    "$BIN/temporal" operator cluster health --address "127.0.0.1:${TEMPORAL_PORT}" >/dev/null 2>&1 && break
    sleep 1
  done
  echo "export STREAMCELL_E2E_DATABASE_URL=postgres://postgres:e2e@127.0.0.1:${PG_PORT}/postgres?sslmode=disable"
  echo "export STREAMCELL_E2E_TEMPORAL=127.0.0.1:${TEMPORAL_PORT}"
  echo "export KUBEBUILDER_ASSETS=$CACHE/envtest"
}

down() {
  docker rm -f streamcell-e2e-pg >/dev/null 2>&1 || true
  if [[ -f "$CACHE/temporal.pid" ]]; then kill "$(cat "$CACHE/temporal.pid")" 2>/dev/null || true; rm -f "$CACHE/temporal.pid"; fi
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
