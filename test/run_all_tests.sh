#!/bin/bash
# Run all pg_facets tests

set -e

# Default database name
DB_NAME="${PGDATABASE:-postgres}"
DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"
DB_USER="${PGUSER:-postgres}"

echo "=============================================="
echo "pg_facets Test Runner"
echo "=============================================="
echo "Database: $DB_NAME"
echo "Host: $DB_HOST:$DB_PORT"
echo "User: $DB_USER"
echo "=============================================="
echo ""

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

# Function to run a test file
run_test() {
    local test_file="$1"
    local test_name=$(basename "$test_file" .sql)
    
    echo ""
    echo "Running: $test_name"
    echo "----------------------------------------------"
    
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$test_file" 2>&1; then
        echo "✓ $test_name completed"
    else
        echo "✗ $test_name failed"
        return 1
    fi
}

# Run tests in order
FAILED=0

echo ""
echo "Running Minimal Facets Tests..."
run_test "$SQL_DIR/minimal_facets_test.sql" || FAILED=1

echo ""
echo "Running Minimal BM25 Tests..."
run_test "$SQL_DIR/minimal_bm25_test.sql" || FAILED=1

echo ""
echo "=============================================="
if [ $FAILED -eq 0 ]; then
    echo "All tests completed successfully!"
else
    echo "Some tests failed. Check output above."
    exit 1
fi
echo "=============================================="
