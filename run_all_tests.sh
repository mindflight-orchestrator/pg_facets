#!/bin/bash
# Comprehensive test runner for pg_facets
# Runs: Zig tests, Docker/SQL tests, and Golang tests
#
# Usage:
#   ./run_all_tests.sh              # Run all tests
#   ./run_all_tests.sh --zig-only   # Run only Zig tests
#   ./run_all_tests.sh --docker-only # Run only Docker/SQL tests
#   ./run_all_tests.sh --golang-only # Run only Golang tests
#   ./run_all_tests.sh --skip-docker # Skip Docker tests (faster)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
RUN_ZIG=true
RUN_DOCKER=true
RUN_GOLANG=true
LOG_TO_FILE=false
LOG_FILE=""

for arg in "$@"; do
    case $arg in
        --zig-only)
            RUN_DOCKER=false
            RUN_GOLANG=false
            ;;
        --docker-only)
            RUN_ZIG=false
            RUN_GOLANG=false
            ;;
        --golang-only)
            RUN_ZIG=false
            RUN_DOCKER=false
            ;;
        --skip-docker)
            RUN_DOCKER=false
            ;;
        -logfile)
            LOG_TO_FILE=true
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --zig-only       Run only Zig unit tests"
            echo "  --docker-only    Run only Docker/SQL tests"
            echo "  --golang-only    Run only Golang tests"
            echo "  --skip-docker    Skip Docker tests (faster)"
            echo "  -logfile         Save output to log file with timestamp"
            echo "  --help, -h       Show this help message"
            exit 0
            ;;
    esac
done

# Create logs directory if it doesn't exist
LOGS_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOGS_DIR"

# Setup logging if requested
if [ "$LOG_TO_FILE" = true ]; then
    # Generate log filename with timestamp
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    LOG_FILE="${LOGS_DIR}/test_results_${TIMESTAMP}.log"
    echo -e "${BLUE}Logging output to: ${LOG_FILE}${NC}"
    echo ""
    
    # Create a function to handle both screen and file output
    # Use tee to write to both stdout and log file
    exec > >(tee "$LOG_FILE")
    exec 2>&1
fi

# Track test results
ZIG_PASSED=false
DOCKER_PASSED=false
GOLANG_PASSED=false
OVERALL_EXIT=0

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  pg_facets Comprehensive Test Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Test Plan:"
echo -e "  ${YELLOW}Zig Tests:${NC}     $([ "$RUN_ZIG" = true ] && echo "✓" || echo "✗")"
echo -e "  ${YELLOW}Docker Tests:${NC}   $([ "$RUN_DOCKER" = true ] && echo "✓" || echo "✗")"
echo -e "  ${YELLOW}Golang Tests:${NC}   $([ "$RUN_GOLANG" = true ] && echo "✓" || echo "✗")"
echo ""

# ============================================================================
# 1. Zig Unit Tests
# ============================================================================
if [ "$RUN_ZIG" = true ]; then
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  Step 1: Running Zig Unit Tests${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo ""
    
    # Check if zig is available
    if ! command -v zig &> /dev/null; then
        echo -e "${RED}ERROR: zig command not found${NC}"
        echo -e "${YELLOW}  Skipping Zig tests. Install Zig to run these tests.${NC}"
        ZIG_PASSED=false
        OVERALL_EXIT=1
    else
        echo -e "${BLUE}Running Zig tests...${NC}"
        
        # Run Zig tests from test_utils.zig
        if ./run_zig_tests.sh 2>&1; then
            echo -e "${GREEN}✓ Zig tests passed${NC}"
            ZIG_PASSED=true
        else
            echo -e "${RED}✗ Zig tests failed${NC}"
            ZIG_PASSED=false
            OVERALL_EXIT=1
        fi
    fi
    echo ""
fi

# ============================================================================
# 2. Docker/SQL Tests
# ============================================================================
if [ "$RUN_DOCKER" = true ]; then
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  Step 2: Running Docker/SQL Tests${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo ""
    
    # Check if docker is available
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}ERROR: docker command not found${NC}"
        echo -e "${YELLOW}  Skipping Docker tests. Install Docker to run these tests.${NC}"
        DOCKER_PASSED=false
        OVERALL_EXIT=1
    elif ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo -e "${RED}ERROR: docker-compose command not found${NC}"
        echo -e "${YELLOW}  Skipping Docker tests. Install docker-compose to run these tests.${NC}"
        DOCKER_PASSED=false
        OVERALL_EXIT=1
    else
        echo -e "${BLUE}Building and starting Docker test environment...${NC}"
        
        cd docker
        
        # Use docker compose (newer) or docker-compose (older)
        DOCKER_COMPOSE_CMD="docker-compose"
        if docker compose version &> /dev/null; then
            DOCKER_COMPOSE_CMD="docker compose"
        fi
        
        # Build and run tests
        if $DOCKER_COMPOSE_CMD -f docker-compose.test.yml build; then
            # Run tests and capture the result
            $DOCKER_COMPOSE_CMD -f docker-compose.test.yml up --abort-on-container-exit
            COMPOSE_EXIT=$?
            
            # Try to get the exit code from the test_runner container
            # The container might be gone, so use the compose exit code as fallback
            EXIT_CODE=$($DOCKER_COMPOSE_CMD -f docker-compose.test.yml ps -q test_runner 2>/dev/null | xargs docker inspect -f '{{ .State.ExitCode }}' 2>/dev/null || echo "$COMPOSE_EXIT")
            
            # If we couldn't get the exit code, use compose exit
            if [ -z "$EXIT_CODE" ]; then
                EXIT_CODE=$COMPOSE_EXIT
            fi
            
            if [ "$EXIT_CODE" = "0" ]; then
                echo -e "${GREEN}✓ Docker/SQL tests passed${NC}"
                DOCKER_PASSED=true
            else
                echo -e "${RED}✗ Docker/SQL tests failed (exit code: $EXIT_CODE)${NC}"
                DOCKER_PASSED=false
                OVERALL_EXIT=1
            fi
        else
            echo -e "${RED}✗ Docker build failed${NC}"
            DOCKER_PASSED=false
            OVERALL_EXIT=1
        fi
        
        # Save PostgreSQL logs before cleanup
        echo -e "${BLUE}Saving PostgreSQL logs...${NC}"
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        POSTGRES_LOG_FILE="${LOGS_DIR}/postgres_log_${TIMESTAMP}.log"
        TEST_RUNNER_LOG_FILE="${LOGS_DIR}/test_runner_log_${TIMESTAMP}.log"
        
        # Get logs from PostgreSQL container
        if docker ps -a --format '{{.Names}}' | grep -q "^pg_facets_test$"; then
            echo -e "${BLUE}Capturing logs from pg_facets_test container...${NC}"
            docker logs pg_facets_test > "$POSTGRES_LOG_FILE" 2>&1
            echo -e "${GREEN}PostgreSQL logs saved to: ${POSTGRES_LOG_FILE}${NC}"
        else
            echo -e "${YELLOW}Warning: pg_facets_test container not found${NC}"
        fi
        
        # Get logs from test runner container
        if docker ps -a --format '{{.Names}}' | grep -q "^pg_facets_test_runner$"; then
            echo -e "${BLUE}Capturing logs from pg_facets_test_runner container...${NC}"
            docker logs pg_facets_test_runner > "$TEST_RUNNER_LOG_FILE" 2>&1
            echo -e "${GREEN}Test runner logs saved to: ${TEST_RUNNER_LOG_FILE}${NC}"
        else
            echo -e "${YELLOW}Warning: pg_facets_test_runner container not found${NC}"
        fi
        
        # Cleanup
        echo -e "${BLUE}Cleaning up Docker containers...${NC}"
        $DOCKER_COMPOSE_CMD -f docker-compose.test.yml down 2>/dev/null || true
        
        cd ..
    fi
    echo ""
fi

# ============================================================================
# 3. Golang Tests
# ============================================================================
if [ "$RUN_GOLANG" = true ]; then
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  Step 3: Running Golang Tests${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo ""
    
    # Check if go is available
    if ! command -v go &> /dev/null; then
        echo -e "${RED}ERROR: go command not found${NC}"
        echo -e "${YELLOW}  Skipping Golang tests. Install Go to run these tests.${NC}"
        GOLANG_PASSED=false
        OVERALL_EXIT=1
    else
        echo -e "${BLUE}Running Golang tests...${NC}"
        
        # Set log directory for Go tests (absolute path)
        SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
        export PGFACETS_LOGS_DIR="${SCRIPT_DIR}/logs"
        
        cd examples/golang
        
        # Run the golang test script
        if ./run_tests.sh; then
            echo -e "${GREEN}✓ Golang tests passed${NC}"
            GOLANG_PASSED=true
        else
            echo -e "${RED}✗ Golang tests failed${NC}"
            GOLANG_PASSED=false
            OVERALL_EXIT=1
        fi
        
        cd ../..
    fi
    echo ""
fi

# ============================================================================
# Summary
# ============================================================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Test Results Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if [ "$RUN_ZIG" = true ]; then
    if [ "$ZIG_PASSED" = true ]; then
        echo -e "  ${GREEN}✓ Zig Tests:${NC}       PASSED"
    else
        echo -e "  ${RED}✗ Zig Tests:${NC}       FAILED"
    fi
fi

if [ "$RUN_DOCKER" = true ]; then
    if [ "$DOCKER_PASSED" = true ]; then
        echo -e "  ${GREEN}✓ Docker Tests:${NC}    PASSED"
    else
        echo -e "  ${RED}✗ Docker Tests:${NC}     FAILED"
    fi
fi

if [ "$RUN_GOLANG" = true ]; then
    if [ "$GOLANG_PASSED" = true ]; then
        echo -e "  ${GREEN}✓ Golang Tests:${NC}    PASSED"
    else
        echo -e "  ${RED}✗ Golang Tests:${NC}    FAILED"
    fi
fi

echo ""

if [ $OVERALL_EXIT -eq 0 ]; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  ALL TESTS PASSED! ✓${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}  SOME TESTS FAILED! ✗${NC}"
    echo -e "${RED}========================================${NC}"
fi

# Show log file location if logging was enabled
if [ "$LOG_TO_FILE" = true ] && [ -n "$LOG_FILE" ]; then
    echo ""
    echo -e "${BLUE}Full test output saved to: ${LOG_FILE}${NC}"
fi

exit $OVERALL_EXIT

