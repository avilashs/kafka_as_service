.PHONY: build test vet fmt-check e2e-deps e2e images kafka-plugins kafka-image kafka-image-4.3.0 plugin-it kind-up kind-smoke kind-down

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

# The Kafka plugins (authorizer, topic policies), the derived Kafka image that carries them, and the
# integration test that runs them in a real broker.
STRIMZI_KAFKA_IMAGE ?= quay.io/strimzi/kafka:1.3.0-kafka-4.3.1
kafka-plugins:
	cd plugins/kafka && mvn -q -B package

kafka-image: kafka-plugins
	docker build -f deploy/images/kafka/Dockerfile --build-arg BASE=$(STRIMZI_KAFKA_IMAGE) -t streamcell/kafka:4.3.1-dev .

# The kind cell offers 4.3.0 as well, so that a version upgrade can be exercised.
kafka-image-4.3.0: kafka-plugins
	docker build -f deploy/images/kafka/Dockerfile --build-arg BASE=quay.io/strimzi/kafka:1.3.0-kafka-4.3.0 -t streamcell/kafka:4.3.0-dev .

plugin-it: kafka-plugins
	KAFKA_IMAGE=$(STRIMZI_KAFKA_IMAGE) plugins/kafka/it/it.sh

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
