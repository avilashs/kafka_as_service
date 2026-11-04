.PHONY: build test vet fmt-check e2e-deps e2e kind-up kind-smoke kind-down

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

vet:
	go vet ./... && go vet -tags faultinject ./... && go vet -tags e2e ./test/...

fmt-check:
	@test -z "$$(gofmt -l cmd internal test)" || (gofmt -l cmd internal test; exit 1)

# Postgres (docker), Temporal dev server, envtest binaries.
e2e-deps:
	hack/e2e-deps.sh up

e2e:
	eval "$$(hack/e2e-deps.sh up)" && go test -tags e2e -count=1 -v ./test/e2e/

# Real local cell: kind + Strimzi 1.3.0 + Envoy Gateway, control plane on the host.
kind-up:
	deploy/kind/up.sh

kind-smoke:
	deploy/kind/smoke.sh

kind-down:
	deploy/kind/down.sh
