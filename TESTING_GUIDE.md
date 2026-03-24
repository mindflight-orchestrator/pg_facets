# Comprehensive Testing Guide for pg_facets

This guide explains how to run all tests for pg_facets, including Zig unit tests, Docker/SQL integration tests, and Golang client library tests.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Prerequisites](#prerequisites)
3. [Quick Testing Options](#quick-testing-options)
4. [Test Runner Script](#test-runner-script)
5. [Test Suites](#test-suites)
6. [Docker Setup](#docker-setup)
7. [Running Tests Manually](#running-tests-manually)
8. [Performance Benchmarks](#performance-benchmarks)
9. [Troubleshooting](#troubleshooting)
10. [CI/CD Integration](#cicd-integration)
11. [Quick Reference](#quick-reference)

## Quick Start

The fastest way to run all tests:

```bash
cd extensions/pg_facets

# Run all tests (Zig, Docker/SQL, and Golang)
./run_all_tests.sh

# Run only specific test suites
./run_all_tests.sh --zig-only
./run_all_tests.sh --docker-only
./run_all_tests.sh --golang-only

# Skip Docker tests (faster, for development)
./run_all_tests.sh --skip-docker
```

## Prerequisites

### For All Tests
- Bash shell
- Basic Unix utilities

### For Zig Tests
- Zig 0.15.2+ installed and in PATH
- Run: `zig version` to verify

### For Docker Tests
- Docker installed and running
- Docker Compose (or `docker compose` command)
- Run: `docker --version` and `docker-compose --version` to verify

### For Golang Tests
- Go 1.19+ installed and in PATH
- Run: `go version` to verify

## Quick Testing Options

Full Docker rebuilds can take 1+ hours. Use these faster approaches for development:

### Option 1: Use Existing Container (Fastest - ~30 seconds)

If you already have a `pg_facets` container running:

```bash
cd extensions/pg_facets
./docker/run_tests_simple.sh
```

This assumes:
- Container name is `pg_facets` (or set `PG_FACETS_CONTAINER` env var)
- Container is already running with the extension installed

### Option 2: Quick Validation Only (~5 seconds)

Just verify functions exist:

```bash
cd extensions/pg_facets
./docker/run_quick_tests.sh
```

Or manually:
```bash
docker exec -i pg_facets psql -U postgres -d postgres < test/sql/quick_native_validation.sql
```

### Option 3: Direct PostgreSQL Connection (Fastest if DB is local)

If you have PostgreSQL running locally with the extension:

```bash
# Build extension locally (no Docker)
cd extensions/pg_facets
zig build

# Install extension (adjust paths as needed)
sudo cp zig-out/lib/libpg_facets.so /usr/lib/postgresql/17/lib/
sudo cp pg_facets.control /usr/share/postgresql/17/extension/
sudo cp sql/pg_facets--*.sql /usr/share/postgresql/17/extension/

# Run tests
psql -d your_database -f test/sql/quick_native_validation.sql
psql -d your_database -f test/sql/native_tokenization_test.sql
```

### Option 4: Incremental Docker Build (Faster rebuilds)

If you need to rebuild but want it faster:

```bash
cd extensions/pg_facets

# Build only the extension (cached layers help)
docker-compose -f docker/docker-compose.yml build --no-cache pg_facets

# Start container
docker-compose -f docker/docker-compose.yml up -d

# Run tests
./docker/run_tests_simple.sh
```

## Test Runner Script

The `run_all_tests.sh` script provides a unified way to run all tests for pg_facets:

- **Zig Unit Tests**: Fast unit tests for core logic
- **Docker/SQL Tests**: Integration tests with PostgreSQL
- **Golang Tests**: Client library and integration tests

### Usage

```bash
cd extensions/pg_facets

# Run all tests
./run_all_tests.sh

# Run only specific test suites
./run_all_tests.sh --zig-only
./run_all_tests.sh --docker-only
./run_all_tests.sh --golang-only

# Skip Docker tests (faster, for development)
./run_all_tests.sh --skip-docker
```

### Test Output

The script provides color-coded output:

- 🟢 **Green**: Tests passed
- 🔴 **Red**: Tests failed
- 🟡 **Yellow**: Information/warnings
- 🔵 **Blue**: Section headers

### Exit Codes

- `0`: All tests passed
- `1`: One or more tests failed

### Performance

Typical test execution times:

- **Zig tests**: < 1 second
- **Docker tests**: 2-5 minutes (includes build time)
- **Golang tests**: 1-3 minutes (includes build time)
- **All tests**: 3-8 minutes total

## Test Suites

### 1. Zig Unit Tests

**Location**: `src/test_utils.zig`

**What it tests**:
- Basic allocator functionality
- String hash map operations
- Memory alignment calculations

**Run manually**:
```bash
zig test src/test_utils.zig
```

**Or via build system**:
```bash
zig build test
```

### 2. Docker/SQL Tests

**Location**: `docker/docker-compose.test.yml` and `test/sql/*.sql`

**What it tests**:
- Extension installation and setup
- Native tokenization functions
- BM25 indexing and search
- Memory safety edge cases
- Performance benchmarks

**Test files included**:
- `quick_native_validation.sql` - Quick sanity checks
- `native_tokenization_test.sql` - Full tokenization tests
- `native_performance_benchmark.sql` - Performance tests
- `memory_safety_test.sql` - Edge cases and memory safety
- `complete_test.sql` - Comprehensive test suite
- `bm25_search_test.sql` - BM25 text search functionality
- `performance_benchmark.sql` - Performance benchmarks and timing tests
- `hash_id_test.sql` - Hash-based ID handling
- `id_reconstruction_test.sql` - ID reconstruction from chunks

**Run manually**:
```bash
cd docker
docker-compose -f docker-compose.test.yml build
docker-compose -f docker-compose.test.yml up --abort-on-container-exit
docker-compose -f docker-compose.test.yml down
```

### 3. Golang Tests

**Location**: `examples/golang/*_test.go`

**What it tests**:
- Go client library functionality
- Integration with pg_facets extension
- Performance benchmarks
- Error handling

**Run manually**:
```bash
cd examples/golang
./run_tests.sh
```

### Memory Safety Tests

The memory safety test suite (`test/sql/memory_safety_test.sql`) verifies:

- ✅ Very large content strings (10MB limit)
- ✅ Empty/null string handling
- ✅ Large query strings (1MB limit)
- ✅ Parallel indexing edge cases
- ✅ Language string limits

These tests ensure the fixes for segmentation faults are working correctly.

## Docker Setup

### Option 1: Using the Main Docker Setup

The main Docker setup is located in `extensions/pg_facets/docker/`:

```bash
cd extensions/pg_facets/docker

# Build the Docker image
docker-compose build

# Start PostgreSQL with pg_facets
docker-compose up -d

# Check logs to verify extension is loaded
docker-compose logs pg_facets
```

### Option 2: Using Golang Test Docker Setup

The Golang test setup includes a dedicated Dockerfile for testing:

```bash
cd extensions/pg_facets/examples/golang

# Build and start test container
docker-compose -f docker-compose.test.yml build --no-cache
docker-compose -f docker-compose.test.yml up -d
```

### Updating Dockerfile

If the Dockerfile doesn't include all SQL files, update it:

```dockerfile
# Copy all SQL files
COPY --from=builder /tmp/pg_facets/sql/pg_facets--*.sql /usr/share/postgresql/$PG_MAJOR/extension/
```

## Running Tests Manually

### SQL/Zig Tests

**IMPORTANT:** Before running SQL tests, you must start the Docker container:

```bash
# Option 1: Using main Docker setup
cd extensions/pg_facets/docker
docker-compose up -d

# Wait for PostgreSQL to be ready
sleep 10
docker-compose exec pg_facets pg_isready -U postgres

# Option 2: Using Golang test Docker setup
cd extensions/pg_facets/examples/golang
docker-compose -f docker-compose.test.yml up -d
```

**Quick Start**:

```bash
cd extensions/pg_facets/test

# Set database connection (adjust port if needed - Docker default is 5433)
export PGHOST=localhost
export PGPORT=5433  # Change to 5432 if using main docker-compose
export PGUSER=postgres
export PGDATABASE=postgres

# Verify connection
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "SELECT 1;"

# Run all tests
./run_all_tests.sh
```

**Individual Test Files**:

```bash
# Complete test suite
psql -h localhost -p 5433 -U postgres -d postgres -f test/sql/complete_test.sql

# Bitmap optimization tests
psql -h localhost -p 5433 -U postgres -d postgres -f test/sql/bitmap_optimization_test.sql

# BM25 search tests
psql -h localhost -p 5433 -U postgres -d postgres -f test/sql/bm25_search_test.sql

# Performance benchmarks
psql -h localhost -p 5433 -U postgres -d postgres -f test/sql/performance_benchmark.sql
```

**Using Docker Exec**:

```bash
# Get container name
CONTAINER_NAME=$(docker ps --filter "name=pg_facets" --format "{{.Names}}" | head -1)

# Run tests inside container
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres -f /usr/share/postgresql/17/extension/pg_facets_test.sql

# Or copy test file and run
docker cp test/sql/complete_test.sql $CONTAINER_NAME:/tmp/
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres -f /tmp/complete_test.sql
```

### Golang Tests

**Quick Start (Recommended)**:

```bash
cd extensions/pg_facets/examples/golang

# Run all tests (builds Docker, starts container, runs tests, cleans up)
./run_tests.sh
```

**Using Make**:

```bash
cd extensions/pg_facets/examples/golang

# Full test suite
make test

# Fast test (requires container to be running)
make test-fast
```

**Manual Setup**:

```bash
cd extensions/pg_facets/examples/golang

# 1. Start PostgreSQL container
make start-db
# or
docker-compose -f docker-compose.test.yml up -d

# 2. Wait for PostgreSQL to be ready
# (The start-db target does this automatically)

# 3. Run Go tests
export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
export PGFACETS_TEST_FAIL_ON_NO_DB=true
go test -v -race -timeout 5m ./...

# 4. Stop container when done
make stop-db
# or
docker-compose -f docker-compose.test.yml down
```

**Individual Test Files**:

```bash
# Test Zig native implementation
go test -v ./faceting_zig_native_test.go ./faceting_zig_native.go

# Test SQL-based implementation
go test -v ./faceting_zig_test.go ./faceting_zig.go

# Benchmark tests
go test -v -bench=. ./faceting_benchmark_test.go
```

**Test Environment Variables**:

| Variable | Description | Default |
|----------|-------------|---------|
| `TEST_DATABASE_URL` | PostgreSQL connection string | `postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable` |
| `PGFACETS_TEST_FAIL_ON_NO_DB` | Fail tests if DB unavailable | `false` (set to `true` for CI) |

## Performance Benchmarks

Performance benchmarks help verify that optimizations are working:

```bash
# Using Docker exec
CONTAINER_NAME=$(docker ps --filter "name=pg_facets" --format "{{.Names}}" | head -1)
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres -f /tmp/performance_benchmark.sql

# Or copy and run
docker cp test/sql/performance_benchmark.sql $CONTAINER_NAME:/tmp/
docker exec -i $CONTAINER_NAME psql -U postgres -d postgres -f /tmp/performance_benchmark.sql
```

For performance testing with larger datasets:

```bash
# Connect to your database
docker exec -it pg_facets psql -U postgres -d postgres

# Run benchmark (adjust dataset size in the SQL file first)
\i test/sql/native_performance_benchmark.sql
```

## Complete Test Workflow

Here's a complete workflow to test everything:

```bash
# 1. Navigate to project root
cd /path/to/mfo-postgres-ext

# 2. Build and start Docker container
cd extensions/pg_facets/docker
docker-compose build --no-cache
docker-compose up -d

# 3. Wait for PostgreSQL to be ready
sleep 10
docker-compose exec pg_facets pg_isready -U postgres

# 4. Verify extension is installed
docker-compose exec pg_facets psql -U postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_facets';"

# 5. Run SQL tests
cd ../test
export PGHOST=localhost
export PGPORT=5433  # Adjust if different
export PGUSER=postgres
export PGDATABASE=postgres
./run_all_tests.sh

# 6. Run performance benchmarks
psql -h localhost -p 5433 -U postgres -d postgres -f sql/performance_benchmark.sql

# 7. Run Golang tests
cd ../examples/golang
./run_tests.sh

# 8. Cleanup
cd ../../docker
docker-compose down
```

## Troubleshooting

### Zig Tests Fail

**Problem**: `zig: command not found`
**Solution**: Install Zig from https://ziglang.org/download/

**Problem**: Test compilation errors
**Solution**: Check Zig version compatibility (requires 0.15.2+)

### Docker Tests Fail

**Problem**: `docker: command not found`
**Solution**: Install Docker Desktop or Docker Engine

**Problem**: Port conflicts
**Solution**: The test uses port 5434. Stop any PostgreSQL on that port:
```bash
docker ps | grep postgres
docker stop <container_id>
```

**Problem**: Build failures
**Solution**: Check Docker logs:
```bash
cd docker
docker-compose -f docker-compose.test.yml logs
```

### Golang Tests Fail

**Problem**: `go: command not found`
**Solution**: Install Go from https://go.dev/dl/

**Problem**: Database connection errors
**Solution**: Ensure Docker tests run first (they set up the database)

**Problem**: Module not found
**Solution**: Run `go mod download` in `examples/golang/`

### Extension Not Found

If you get "extension pg_facets does not exist":

```bash
# Check if extension files are in the right place
docker exec -it <container> ls -la /usr/share/postgresql/17/extension/pg_facets*

# Create extension manually
docker exec -it <container> psql -U postgres -c "CREATE EXTENSION pg_facets;"
```

### Wrong Version

If extension shows wrong version:

```bash
# Drop and recreate
docker exec -it <container> psql -U postgres -c "DROP EXTENSION pg_facets CASCADE;"
docker exec -it <container> psql -U postgres -c "CREATE EXTENSION pg_facets;"
```

### Native Functions Not Available

If native Zig functions are not working:

1. Check if the shared library is built:
   ```bash
   docker exec -it <container> ls -la /usr/lib/postgresql/17/lib/pg_facets.so
   ```

2. Check PostgreSQL logs:
   ```bash
   docker-compose logs pg_facets | grep -i error
   ```

3. Rebuild the Docker image to ensure Zig code is compiled:
   ```bash
   docker-compose build --no-cache
   ```

### Connection Issues

If you can't connect to PostgreSQL:

```bash
# Check if container is running
docker ps | grep pg_facets

# Check port mapping
docker port <container_name>

# Check logs
docker-compose logs pg_facets
```

### Container Not Found

```bash
# Start the container
cd extensions/pg_facets
docker-compose -f docker/docker-compose.yml up -d
```

### Extension Not Installed

```bash
docker exec -it pg_facets psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_facets;"
```

### Function Not Found

The native function requires the extension to be rebuilt with the new code. Make sure you've:
1. Built the extension: `zig build`
2. Copied the .so file to the container
3. Restarted PostgreSQL or reloaded the extension

### Test Failures

If tests fail:

1. **Check extension version**: Ensure you're testing against the correct version
   ```sql
   SELECT facets._get_version();
   ```

2. **Check test data**: Some tests require specific test data setup
   ```bash
   # Run base.sql first if needed
   psql -h localhost -p 5433 -U postgres -d postgres -f test/sql/base.sql
   ```

3. **Check logs**: Look for error messages in test output

4. **Run tests individually**: Isolate which test is failing

## CI/CD Integration

The test runner is designed for CI/CD pipelines:

```yaml
# Example GitHub Actions
- name: Run all tests
  run: |
    cd extensions/pg_facets
    ./run_all_tests.sh
```

For faster CI runs, you can split tests:
```yaml
- name: Run Zig tests
  run: ./run_all_tests.sh --zig-only

- name: Run Docker tests
  run: ./run_all_tests.sh --docker-only

- name: Run Golang tests
  run: ./run_all_tests.sh --golang-only
```

Or use a bash script:

```bash
#!/bin/bash
set -e

# Build and test
cd extensions/pg_facets/docker
docker-compose build
docker-compose up -d
sleep 10

# Run SQL tests
cd ../test
export PGHOST=localhost PGPORT=5433 PGUSER=postgres PGDATABASE=postgres
./run_all_tests.sh

# Run Golang tests
cd ../examples/golang
./run_tests.sh

# Cleanup
cd ../../docker
docker-compose down -v
```

## Continuous Testing

For development, you can run tests in watch mode:

```bash
# Terminal 1: Keep Docker running
cd docker
docker-compose up -d

# Terminal 2: Run tests repeatedly
while true; do
    cd extensions/pg_facets
    ./run_all_tests.sh --skip-docker
    sleep 5
done
```

## Recommended Workflow

1. **First time**: Build extension locally, test with local PostgreSQL
2. **Quick checks**: Use `run_quick_tests.sh` 
3. **Full validation**: Use `run_tests_simple.sh` with existing container
4. **Performance**: Run benchmarks directly in psql
5. **CI/CD**: Use `run_all_tests.sh` for comprehensive testing

This avoids the 1+ hour Docker rebuild cycle for development!

## Quick Reference

### Docker Commands

```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# Rebuild
docker-compose build --no-cache

# Logs
docker-compose logs -f pg_facets

# Execute SQL
docker-compose exec pg_facets psql -U postgres -c "SELECT 1;"
```

### Test Commands

```bash
# All tests via test runner
cd extensions/pg_facets && ./run_all_tests.sh

# All SQL tests
cd test && ./run_all_tests.sh

# All Golang tests
cd examples/golang && ./run_tests.sh

# Performance benchmark
psql -f test/sql/performance_benchmark.sql
```

### Verification Commands

```bash
# Check version
psql -c "SELECT facets._get_version();"

# Check extensions
psql -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_facets', 'roaringbitmap');"

# Check native functions
psql -c "SELECT proname FROM pg_proc WHERE proname LIKE '%_native' AND pronamespace = 'facets'::regnamespace;"
```

## Additional Resources

- **Performance Optimizations**: See `PERFORMANCE_OPTIMIZATIONS.md`
- **Documentation**: See `DOCUMENTATION.md`
- **Usage Examples**: See `USAGE.md`
- **Test README**: See `test/README.md`
