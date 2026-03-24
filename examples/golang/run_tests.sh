#!/bin/bash
# run_tests.sh - Run Go tests against a real PostgreSQL instance with pg_facets
#
# This script:
# 1. Builds the pg_facets Docker image
# 2. Starts PostgreSQL with the extension
# 3. Waits for it to be healthy
# 4. Runs Go tests (FAIL mode, not skip mode)
# 5. Tears down the container

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  pg_facets Go Test Runner${NC}"
echo -e "${YELLOW}========================================${NC}"

# Cleanup function
cleanup() {
    # Save PostgreSQL logs before cleanup
    echo -e "\n${YELLOW}Saving PostgreSQL logs...${NC}"
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    # Use environment variable if set by parent script, otherwise calculate
    if [ -n "$PGFACETS_LOGS_DIR" ]; then
        LOGS_DIR="$PGFACETS_LOGS_DIR"
    else
        # Calculate from script location (works when run standalone)
        LOG_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
        LOGS_DIR="${LOG_DIR}/logs"
    fi
    mkdir -p "$LOGS_DIR"
    POSTGRES_LOG_FILE="${LOGS_DIR}/postgres_log_golang_${TIMESTAMP}.log"
    
    # Get logs from PostgreSQL container
    if docker ps -a --format '{{.Names}}' | grep -q "^pg_facets_test$"; then
        echo -e "${YELLOW}Capturing logs from pg_facets_test container...${NC}"
        docker logs pg_facets_test > "$POSTGRES_LOG_FILE" 2>&1
        echo -e "${GREEN}PostgreSQL logs saved to: ${POSTGRES_LOG_FILE}${NC}"
    else
        echo -e "${YELLOW}Warning: pg_facets_test container not found${NC}"
    fi
    
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    docker-compose -f docker-compose.test.yml down -v --remove-orphans 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Step 1: Build and start PostgreSQL
echo -e "\n${YELLOW}Step 1: Building and starting PostgreSQL with pg_facets...${NC}"
docker-compose -f docker-compose.test.yml build --no-cache
docker-compose -f docker-compose.test.yml up -d

# Step 2: Wait for PostgreSQL to be healthy
echo -e "\n${YELLOW}Step 2: Waiting for PostgreSQL to be ready...${NC}"
MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose -f docker-compose.test.yml exec -T pg_facets_test pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}PostgreSQL is ready!${NC}"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting for PostgreSQL... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}ERROR: PostgreSQL did not become ready in time${NC}"
    docker-compose -f docker-compose.test.yml logs pg_facets_test
    exit 1
fi

# Step 3: Verify extensions are installed
echo -e "\n${YELLOW}Step 3: Verifying pg_facets extension...${NC}"
docker-compose -f docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS vector;"
docker-compose -f docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;"
docker-compose -f docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_facets;"
docker-compose -f docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'roaringbitmap', 'pg_facets');"

# Step 4: Run Go tests with FAIL mode (not skip mode)
echo -e "\n${YELLOW}Step 4: Running Go tests...${NC}"
export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
export PGFACETS_TEST_FAIL_ON_NO_DB=true

# Run tests with verbose output
if go test -v -race -timeout 5m ./...; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}  ALL TESTS PASSED!${NC}"
    echo -e "${GREEN}========================================${NC}"
    EXIT_CODE=0
else
    echo -e "\n${RED}========================================${NC}"
    echo -e "${RED}  TESTS FAILED!${NC}"
    echo -e "${RED}========================================${NC}"
    EXIT_CODE=1
fi

# Cleanup happens via trap
exit $EXIT_CODE
