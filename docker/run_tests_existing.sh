#!/bin/bash
# Run tests against existing PostgreSQL container
# Works with any container name - just set CONTAINER_NAME env var

set -e

CONTAINER_NAME="${CONTAINER_NAME:-mfo-installer-postgres-1}"
DB_NAME="${DB_NAME:-postgres}"
DB_USER="${DB_USER:-postgres}"

echo "=============================================="
echo "Running Native Tokenization Tests"
echo "Container: $CONTAINER_NAME"
echo "=============================================="
echo ""

# Check if container exists and is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "ERROR: Container '$CONTAINER_NAME' is not running"
    echo ""
    echo "Available containers:"
    docker ps --format '  {{.Names}}'
    echo ""
    echo "Set CONTAINER_NAME env var to use a different container:"
    echo "  CONTAINER_NAME=your_container ./docker/run_tests_existing.sh"
    exit 1
fi

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until docker exec $CONTAINER_NAME pg_isready -U $DB_USER > /dev/null 2>&1; do
    sleep 1
done
echo "✓ PostgreSQL is ready"
echo ""

# Check if extension is installed
HAS_EXT=$(docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -tAc "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_facets')" 2>/dev/null || echo "f")
if [ "$HAS_EXT" != "t" ]; then
    echo "⚠ WARNING: pg_facets extension not found in container"
    echo "  Installing extensions..."
    docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;" 2>&1 | grep -v "already exists" || true
    docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS pg_facets;" 2>&1 | grep -v "already exists" || true
    echo ""
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR/../test/sql"

echo "--- Test 1: Quick Validation ---"
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < $TEST_DIR/quick_native_validation.sql

echo ""
echo "--- Test 2: Full Native Tokenization Tests ---"
docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < $TEST_DIR/native_tokenization_test.sql

echo ""
echo "=============================================="
echo "Tests complete!"
echo "=============================================="

