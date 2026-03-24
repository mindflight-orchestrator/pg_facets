#!/bin/bash
# Run native tokenization tests in Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "Building Docker images for testing..."
echo "=============================================="
echo ""

# Build the main pg_facets image
echo "Building pg_facets extension..."
docker-compose -f docker-compose.yml build

# Build the test runner image
echo "Building test runner..."
docker-compose -f docker-compose.test.yml build test_runner

echo ""
echo "=============================================="
echo "Starting test environment..."
echo "=============================================="
echo ""

# Start services and run tests
# Capture exit code from docker-compose (which exits with the container's exit code)
set +e  # Don't exit on error, we'll handle it
docker-compose -f docker-compose.test.yml up --abort-on-container-exit
EXIT_CODE=$?
set -e  # Re-enable exit on error

echo ""
echo "=============================================="
if [ "$EXIT_CODE" = "0" ]; then
    echo "✓ All tests passed!"
else
    echo "✗ Some tests failed (exit code: $EXIT_CODE)"
fi
echo "=============================================="

# Save PostgreSQL logs before cleanup
echo ""
echo "Saving PostgreSQL logs..."
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOGS_DIR="${LOG_DIR}/logs"
mkdir -p "$LOGS_DIR"
POSTGRES_LOG_FILE="${LOGS_DIR}/postgres_log_${TIMESTAMP}.log"

# Get logs from PostgreSQL container
if docker ps -a --format '{{.Names}}' | grep -q "^pg_facets_test$"; then
    echo "Capturing logs from pg_facets_test container..."
    docker logs pg_facets_test > "$POSTGRES_LOG_FILE" 2>&1
    echo "PostgreSQL logs saved to: $POSTGRES_LOG_FILE"
    
    # Also get test runner logs
    TEST_RUNNER_LOG_FILE="${LOGS_DIR}/test_runner_log_${TIMESTAMP}.log"
    if docker ps -a --format '{{.Names}}' | grep -q "^pg_facets_test_runner$"; then
        echo "Capturing logs from pg_facets_test_runner container..."
        docker logs pg_facets_test_runner > "$TEST_RUNNER_LOG_FILE" 2>&1
        echo "Test runner logs saved to: $TEST_RUNNER_LOG_FILE"
    fi
else
    echo "Warning: pg_facets_test container not found, cannot capture logs"
fi

# Cleanup
echo ""
echo "Cleaning up..."
docker-compose -f docker-compose.test.yml down

exit $EXIT_CODE

