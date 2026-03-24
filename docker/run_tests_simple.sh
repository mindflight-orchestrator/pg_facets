#!/bin/bash
# Simple test runner - assumes pg_facets container is already running
# Much faster - no Docker builds needed

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

CONTAINER_NAME="${PG_FACETS_CONTAINER:-pg_facets}"

echo "=============================================="
echo "Native Tokenization Tests (Simple Mode)"
echo "=============================================="
echo ""

# Check if container exists
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "ERROR: Container '$CONTAINER_NAME' is not running"
    echo ""
    echo "Start it with:"
    echo "  cd extensions/pg_facets && docker-compose -f docker/docker-compose.yml up -d"
    exit 1
fi

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until docker exec $CONTAINER_NAME pg_isready -U postgres > /dev/null 2>&1; do
    sleep 1
done
echo "✓ PostgreSQL is ready"
echo ""

# Run tests
echo "--- Test 1: Quick Validation ---"
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres < test/sql/quick_native_validation.sql

echo ""
echo "--- Test 2: Full Native Tokenization Tests ---"
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres < test/sql/native_tokenization_test.sql

echo ""
echo "=============================================="
echo "Tests complete!"
echo "=============================================="

