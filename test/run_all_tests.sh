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
echo "Running Complete Test Suite..."
run_test "$SQL_DIR/complete_test.sql" || FAILED=1

echo ""
echo "Running Native Tokenization Tests..."
run_test "$SQL_DIR/native_tokenization_test.sql" || FAILED=1

echo ""
echo "Running BM25 Search Tests..."
run_test "$SQL_DIR/bm25_search_test.sql" || FAILED=1

echo ""
echo "Running Bitmap Optimization Tests..."
run_test "$SQL_DIR/bitmap_optimization_test.sql" || FAILED=1

echo ""
echo "Running Hash ID Tests..."
run_test "$SQL_DIR/hash_id_test.sql" || FAILED=1

echo ""
echo "Running ID Reconstruction Tests..."
run_test "$SQL_DIR/id_reconstruction_test.sql" || FAILED=1

echo ""
echo "Running Facet Regression Tests..."
run_test "$SQL_DIR/facet_regression_test.sql" || FAILED=1

echo ""
echo "Running BM25 Text Primary Key Tests..."
run_test "$SQL_DIR/bm25_text_pk_test.sql" || FAILED=1

echo ""
echo "Running BM25 Helper Functions Tests (0.4.2)..."
run_test "$SQL_DIR/bm25_helpers_test.sql" || FAILED=1

echo ""
echo "Running Parallel Indexing Tests (0.4.2)..."
run_test "$SQL_DIR/parallel_indexing_test.sql" || FAILED=1

echo ""
echo "Running Native Tokenization Tests..."
run_test "$SQL_DIR/native_tokenization_test.sql" || FAILED=1

echo ""
echo "Running Version 0.4.3 Tests (UNLOGGED, pg_cron, ACID)..."
run_test "$SQL_DIR/version_0.4.3_test.sql" || FAILED=1

echo ""
echo "Running ACID Compliance Tests..."
run_test "$SQL_DIR/acid_compliance_test.sql" || FAILED=1

echo ""
echo "=============================================="
if [ $FAILED -eq 0 ]; then
    echo "All tests completed successfully!"
else
    echo "Some tests failed. Check output above."
    exit 1
fi
echo "=============================================="
