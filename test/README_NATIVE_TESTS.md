# Native Tokenization Test Suite

This directory contains comprehensive tests for the native Zig tokenization and batch insert optimizations.

## Test Files

| File | Description | Duration |
|------|-------------|----------|
| `quick_native_validation.sql` | Quick check that native functions exist | < 1 second |
| `native_tokenization_test.sql` | Comprehensive correctness and functionality tests | ~30 seconds |
| `native_performance_benchmark.sql` | Performance comparison and benchmarks | ~2-5 minutes |

## Quick Start

### 1. Quick Validation (Recommended First Step)

```bash
psql -d your_database -f test/sql/quick_native_validation.sql
```

This verifies that:
- `bm25_index_worker_native` function exists
- Extension is properly installed

### 2. Full Test Suite

```bash
# Using the test runner script
./test/sql/run_native_tokenization_tests.sh

# Or directly with psql
psql -d your_database -f test/sql/native_tokenization_test.sql
```

### 3. Performance Benchmark

```bash
psql -d your_database -f test/sql/native_performance_benchmark.sql
```

## Test Coverage

### `native_tokenization_test.sql`

**Test 1**: Native Worker Function Exists
- Verifies `bm25_index_worker_native` is available

**Test 2**: Table Registration
- Tests faceting table registration

**Test 3**: BM25 Sync Trigger
- Verifies trigger creation

**Test 4**: Manual BM25 Indexing
- Tests native tokenization on small dataset

**Test 5**: Tokenization Correctness
- Compares native tokenization with SQL `to_tsvector`
- Ensures lexemes match

**Test 6**: Batch Insert Performance
- Tests batch insert functionality
- Measures throughput

**Test 7**: Parallel Worker Distribution
- Validates work distribution algorithm
- Ensures even workload split

**Test 8**: Full Parallel Indexing
- End-to-end parallel indexing test (requires dblink)

**Test 9**: Performance Comparison
- Compares native vs SQL worker performance
- Reports speedup factor

**Test 10**: Batch Size Validation
- Verifies batch inserts are working
- Checks term counts

**Test 11**: Index Quality
- Validates indexed terms are reasonable
- Checks statistics

### `native_performance_benchmark.sql`

**Benchmark 1**: Native Worker Performance
- Measures throughput (docs/sec, terms/sec)
- Reports processing time

**Benchmark 2**: SQL Worker Performance
- Compares with SQL worker (if available)
- Shows performance improvement

**Benchmark 3**: Batch Insert Efficiency
- Analyzes batch insert effectiveness
- Reports reduction in INSERT statements

**Benchmark 4**: Parallel Worker Scaling
- Tests scaling with 1-4 workers
- Measures parallel efficiency

## Expected Results

### Correctness Tests
All tests should show `PASS` status. If any test fails:
1. Check that the extension is properly built and installed
2. Verify PostgreSQL version compatibility (17.x recommended)
3. Ensure all required extensions are installed (roaringbitmap, dblink for parallel tests)

### Performance Benchmarks

**Expected Speedup**: Native worker should be **10-50x faster** than SQL worker for tokenization.

**Batch Insert Efficiency**: 
- Without batching: ~1.2 billion INSERTs for 12M documents
- With batching: ~120,000 INSERTs (10,000x reduction)

**Throughput Targets**:
- Small dataset (1K docs): > 100 docs/sec
- Medium dataset (10K docs): > 50 docs/sec  
- Large dataset (100K+ docs): > 20 docs/sec

## Troubleshooting

### Function Not Found

If `bm25_index_worker_native` is not found:

1. **Rebuild the extension**:
   ```bash
   cd extensions/pg_facets
   zig build
   ```

2. **Reinstall the extension**:
   ```sql
   DROP EXTENSION pg_facets;
   CREATE EXTENSION pg_facets;
   ```

3. **Check library path**:
   ```sql
   SHOW dynamic_library_path;
   ```

### Performance Issues

If performance is not as expected:

1. **Check PostgreSQL version**: Native functions work best on PostgreSQL 17.x
2. **Verify CPU features**: AVX2 optimizations require modern CPUs
3. **Check work_mem**: Increase `work_mem` for better batch insert performance
4. **Monitor system resources**: Ensure sufficient CPU and memory

### Parallel Tests Fail

If parallel indexing tests fail:

1. **Install dblink extension**:
   ```sql
   CREATE EXTENSION IF NOT EXISTS dblink;
   ```

2. **Check connection string**: Tests use current database connection
3. **Verify permissions**: User needs CREATE and INSERT permissions

## Running Tests in CI/CD

For automated testing:

```bash
#!/bin/bash
set -e

# Set database connection
export PGDATABASE=test_db
export PGUSER=test_user
export PGHOST=localhost
export PGPORT=5432

# Run quick validation
psql -f test/sql/quick_native_validation.sql

# Run full test suite
psql -f test/sql/native_tokenization_test.sql

# Run performance benchmark (optional, takes longer)
# psql -f test/sql/native_performance_benchmark.sql
```

## Test Data

Tests use synthetic data by default. For production-scale testing:

1. Modify `generate_series` ranges in test files
2. Or use actual production tables:
   ```sql
   -- Replace test table with production table
   ALTER TABLE test_native_token.documents 
   RENAME TO documents_prod;
   ```

## Next Steps

After tests pass:

1. **Production Deployment**: Deploy to staging environment
2. **Monitor Performance**: Track indexing times in production
3. **Scale Testing**: Test with actual 12M document dataset
4. **Optimize Further**: Adjust batch sizes if needed

## Related Documentation

- [Main Test README](README.md)
- [BM25 Documentation](../../DOCUMENTATION.md)
- [Migration Guide](../../examples/MIGRATION_TO_0.4.2.md)

