#!/bin/bash
# Run native tokenization test suite
# This script runs the comprehensive test suite for native tokenization and batch inserts

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default database connection
DB_NAME="${PGDATABASE:-postgres}"
DB_USER="${PGUSER:-postgres}"
DB_HOST="${PGHOST:-localhost}"
DB_PORT="${PGPORT:-5432}"

echo -e "${GREEN}==============================================${NC}"
echo -e "${GREEN}pg_facets Native Tokenization Test Suite${NC}"
echo -e "${GREEN}==============================================${NC}"
echo ""

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo -e "${RED}ERROR: psql command not found${NC}"
    exit 1
fi

# Test database connection
echo "Testing database connection..."
if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" > /dev/null 2>&1; then
    echo -e "${RED}ERROR: Cannot connect to database${NC}"
    echo "Please set PGDATABASE, PGUSER, PGHOST, PGPORT environment variables"
    exit 1
fi

echo -e "${GREEN}PASS: Database connection successful${NC}"
echo ""

# Check if extensions are installed
echo "Checking required extensions..."
MISSING_EXTENSIONS=()

if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT 1 FROM pg_extension WHERE extname='pg_facets'" | grep -q 1; then
    MISSING_EXTENSIONS+=("pg_facets")
fi

if ! psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT 1 FROM pg_extension WHERE extname='roaringbitmap'" | grep -q 1; then
    MISSING_EXTENSIONS+=("roaringbitmap")
fi

if [ ${#MISSING_EXTENSIONS[@]} -gt 0 ]; then
    echo -e "${YELLOW}WARNING: Missing extensions: ${MISSING_EXTENSIONS[*]}${NC}"
    echo "Installing missing extensions..."
    for ext in "${MISSING_EXTENSIONS[@]}"; do
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS $ext" || {
            echo -e "${RED}ERROR: Failed to install extension: $ext${NC}"
            exit 1
        }
    done
fi

echo -e "${GREEN}PASS: All required extensions installed${NC}"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TEST_FILE="$SCRIPT_DIR/native_tokenization_test.sql"

if [ ! -f "$TEST_FILE" ]; then
    echo -e "${RED}ERROR: Test file not found: $TEST_FILE${NC}"
    exit 1
fi

# Run tests
echo "Running native tokenization tests..."
echo ""

psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$TEST_FILE"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}==============================================${NC}"
    echo -e "${GREEN}All tests completed successfully!${NC}"
    echo -e "${GREEN}==============================================${NC}"
else
    echo -e "${RED}==============================================${NC}"
    echo -e "${RED}Some tests failed. Check output above.${NC}"
    echo -e "${RED}==============================================${NC}"
fi

exit $EXIT_CODE

