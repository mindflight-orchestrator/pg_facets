# Makefile for pg_facets Go tests
#
# Usage:
#   make test      - Build Docker image and run all tests
#   make test-fast - Run tests against existing container (if already running)
#   make start-db  - Start PostgreSQL container only
#   make stop-db   - Stop PostgreSQL container
#   make logs      - Show PostgreSQL logs
#   make clean     - Remove all containers and volumes

.PHONY: test test-fast start-db stop-db logs clean help

# Default target
help:
	@echo "pg_facets Go Test Runner"
	@echo ""
	@echo "Usage:"
	@echo "  make test       - Build Docker image and run all tests (recommended)"
	@echo "  make test-fast  - Run tests against existing container"
	@echo "  make start-db   - Start PostgreSQL container only"
	@echo "  make stop-db    - Stop PostgreSQL container"
	@echo "  make logs       - Show PostgreSQL logs"
	@echo "  make clean      - Remove all containers and volumes"

# Run full test suite (builds Docker, starts container, runs tests, cleans up)
test:
	@./run_tests.sh

# Run tests against existing container (faster, but requires container to be running)
test-fast:
	@echo "Running tests against existing container..."
	@echo "Make sure the container is running: make start-db"
	TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable" \
	PGFACETS_TEST_FAIL_ON_NO_DB=true \
	go test -v -race -timeout 5m ./...

# Start the PostgreSQL container
start-db:
	@echo "Building and starting PostgreSQL with pg_facets..."
	docker-compose -f docker-compose.test.yml up -d --build
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in $$(seq 1 30); do \
		if docker-compose -f docker-compose.test.yml exec -T pg_facets_test pg_isready -U postgres > /dev/null 2>&1; then \
			echo "PostgreSQL is ready!"; \
			docker-compose -f docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;"; \
			docker-compose -f docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_facets;"; \
			break; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 2; \
	done

# Stop the PostgreSQL container
stop-db:
	docker-compose -f docker-compose.test.yml down

# Show PostgreSQL logs
logs:
	docker-compose -f docker-compose.test.yml logs -f pg_facets_test

# Clean up everything
clean:
	docker-compose -f docker-compose.test.yml down -v --remove-orphans
	@echo "Cleaned up containers and volumes"
