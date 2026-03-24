#!/bin/bash
# run_tests.sh — Run Python integration tests against PostgreSQL with pg_facets.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    docker-compose -f ../golang/docker-compose.test.yml down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo -e "${YELLOW}======================================================${NC}"
echo -e "${YELLOW}  pg_facets Python Integration Test Runner${NC}"
echo -e "${YELLOW}======================================================${NC}"

echo -e "\n${YELLOW}Step 1: Building and starting PostgreSQL with pg_facets...${NC}"
docker-compose -f ../golang/docker-compose.test.yml build --no-cache
docker-compose -f ../golang/docker-compose.test.yml up -d

echo -e "\n${YELLOW}Step 2: Waiting for PostgreSQL to be ready...${NC}"
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose -f ../golang/docker-compose.test.yml exec -T pg_facets_test pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}PostgreSQL is ready!${NC}"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}ERROR: PostgreSQL did not become ready in time${NC}"
    exit 1
fi

echo -e "\n${YELLOW}Step 3: Installing pg_facets extensions...${NC}"
docker-compose -f ../golang/docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS roaringbitmap;"
docker-compose -f ../golang/docker-compose.test.yml exec -T pg_facets_test psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS pg_facets;"

echo -e "\n${YELLOW}Step 4: Running Python tests...${NC}"
export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
export PGFACETS_TEST_FAIL_ON_NO_DB=true

pip install -q -r requirements.txt
if pytest -v tests/; then
    echo -e "\n${GREEN}======================================================${NC}"
    echo -e "${GREEN}  ALL TESTS PASSED!${NC}"
    echo -e "${GREEN}======================================================${NC}"
    EXIT_CODE=0
else
    echo -e "\n${RED}======================================================${NC}"
    echo -e "${RED}  TESTS FAILED!${NC}"
    echo -e "${RED}======================================================${NC}"
    EXIT_CODE=1
fi
exit $EXIT_CODE
