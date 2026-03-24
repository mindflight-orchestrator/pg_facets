#!/bin/bash
# Quick test runner - uses existing Docker container if available
# Much faster than full rebuild

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=============================================="
echo "Quick Native Tokenization Tests"
echo "=============================================="
echo ""

# Check if pg_facets container is already running
if docker ps --format '{{.Names}}' | grep -q '^pg_facets$'; then
    echo "✓ Using existing pg_facets container"
    CONTAINER_NAME="pg_facets"
else
    echo "Starting pg_facets container..."
    docker-compose -f docker/docker-compose.yml up -d
    sleep 5
    CONTAINER_NAME="pg_facets"
fi

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
until docker exec $CONTAINER_NAME pg_isready -U postgres > /dev/null 2>&1; do
    sleep 1
done
echo "✓ PostgreSQL is ready"
echo ""

# Run quick validation test
echo "Running quick validation test..."
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres < test/sql/quick_native_validation.sql

echo ""
echo "=============================================="
echo "Quick validation complete!"
echo ""
echo "To run full tests, use:"
echo "  docker exec -i pg_facets psql -U postgres -d postgres < test/sql/native_tokenization_test.sql"
echo "=============================================="

