.PHONY: deps build test test-integration security-scan docker-build-prod docker-build-test clean

deps:
	git submodule update --init --recursive

build: deps
	zig build -Doptimize=ReleaseFast

test: deps
	docker compose -f docker/docker-compose.test.yml up --build --remove-orphans --abort-on-container-exit --exit-code-from test_runner

security-scan:
	bash scripts/security_scan.sh

test-integration:
	./docker/run_tests.sh

docker-build-prod: deps
	docker compose -f docker/docker-compose.yml build

docker-build-test: deps
	docker compose -f docker/docker-compose.test.yml build

clean:
	docker compose -f docker/docker-compose.test.yml down --remove-orphans || true
	docker compose -f docker/docker-compose.yml down --remove-orphans || true
	rm -rf zig-cache zig-out
