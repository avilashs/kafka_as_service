.PHONY: build test vet fmt-check e2e-deps e2e images kind-up kind-smoke kind-down

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

vet:
	go vet ./... && go vet -tags faultinject ./... && go vet -tags e2e ./test/...

fmt-check:
	@test -z "$$(gofmt -l cmd internal test)" || (gofmt -l cmd internal test; exit 1)

# Static linux binaries + the cell-tools image (guard webhook and auditor).
images:
	mkdir -p bin/linux
	CGO_ENABLED=0 GOOS=linux go build -trimpath -o bin/linux/streamcell-admission ./cmd/streamcell-admission
	CGO_ENABLED=0 GOOS=linux go build -trimpath -o bin/linux/streamcell-audit ./cmd/streamcell-audit
	docker build -f deploy/Dockerfile.cell-tools -t streamcell/cell-tools:dev .

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
