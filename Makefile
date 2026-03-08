.PHONY: test test-compat test-all lint

test:
	go test ./... -v

test-compat:
	docker run -d --name spanner-emulator -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
	@sleep 2
	SPANNER_EMULATOR_HOST=localhost:9010 sh -c 'cd compattest && go test ./... -v'; \
	status=$$?; \
	docker stop spanner-emulator && docker rm spanner-emulator; \
	exit $$status

test-all: test test-compat

lint:
	go vet ./...
