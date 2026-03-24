# Makefile for pg_facets Python integration tests

.PHONY: test test-fast start-db stop-db logs clean help

help:
	@echo "pg_facets Python Test Runner"
	@echo ""
	@echo "Usage:"
	@echo "  make test       - Build Docker image and run full test suite (recommended)"
	@echo "  make test-fast  - Run tests against existing container"
	@echo "  make start-db   - Start PostgreSQL container with pg_facets"
	@echo "  make stop-db    - Stop PostgreSQL container"
	@echo "  make logs       - Show PostgreSQL container logs"
	@echo "  make clean      - Remove all containers and volumes"

test:
	@./run_tests.sh

test-fast:
	@echo "Running tests against existing container (make start-db if not running)..."
	TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable" \
	PGFACETS_TEST_FAIL_ON_NO_DB=true \
	pytest -v tests/

start-db:
	@echo "Building and starting PostgreSQL with pg_facets..."
	docker-compose -f ../golang/docker-compose.test.yml up -d --build
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in $$(seq 1 30); do \
		if docker-compose -f ../golang/docker-compose.test.yml exec -T pg_facets_test pg_isready -U postgres > /dev/null 2>&1; then \
			echo "PostgreSQL is ready!"; \
			docker-compose -f ../golang/docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;"; \
			docker-compose -f ../golang/docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_facets;"; \
			break; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 2; \
	done

stop-db:
	docker-compose -f ../golang/docker-compose.test.yml down

logs:
	docker-compose -f ../golang/docker-compose.test.yml logs -f pg_facets_test

clean:
	docker-compose -f ../golang/docker-compose.test.yml down -v --remove-orphans
	@echo "Cleaned up containers and volumes"
