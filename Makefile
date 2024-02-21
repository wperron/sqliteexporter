setup:
	mkdir -p bin
	curl -fsSL https://github.com/open-telemetry/opentelemetry-collector/releases/download/cmd%2Fbuilder%2Fv0.95.0/ocb_0.95.0_linux_amd64 -o ./bin/ocb
	chmod +x ./bin/ocb

custom-collector: setup
	./bin/ocb --config builder-config.yaml

test:
	go test -v -count=1 ./...

run-dev: custom-collector
	./bin/otelcol-dev/otelcol-dev --config=otelcol-dev-config.yaml
