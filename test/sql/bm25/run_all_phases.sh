#!/bin/bash
# Run all BM25 phase tests

set -e

echo "=============================================="
echo "Running All BM25 Phase Tests"
echo "=============================================="
echo ""

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR"

# Database connection (adjust as needed)
DB_NAME="${PGDATABASE:-postgres}"
DB_USER="${PGUSER:-postgres}"
DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"

echo "Database: $DB_NAME@$DB_HOST:$DB_PORT"
echo ""

# Run each phase test
phases=(
    "phase1_core_infrastructure"
    "phase2_bm25_scoring"
    "phase3_replace_tsrank"
    "phase4_prefix_matching"
    "phase5_fuzzy_matching"
    "phase6_performance"
)

total_passed=0
total_failed=0

for phase in "${phases[@]}"; do
    echo "=============================================="
    echo "Running $phase tests..."
    echo "=============================================="
    
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$TEST_DIR/$phase.sql" 2>&1; then
        echo ""
        echo "✓ $phase: PASSED"
        ((total_passed++))
    else
        echo ""
        echo "✗ $phase: FAILED"
        ((total_failed++))
    fi
    echo ""
done

echo "=============================================="
echo "Test Summary"
echo "=============================================="
echo "Passed: $total_passed"
echo "Failed: $total_failed"
echo "Total:  $((total_passed + total_failed))"
echo ""

if [ $total_failed -eq 0 ]; then
    echo "✓ All tests passed!"
    exit 0
else
    echo "✗ Some tests failed"
    exit 1
fi
