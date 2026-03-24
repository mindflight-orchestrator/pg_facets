# pg_facets Test Suite

This directory contains SQL test files for the pg_facets extension.

## Test Files

| File | Description | Tests |
|------|-------------|-------|
| `complete_test.sql` | Comprehensive test suite covering all major functionality | 45 tests |
| `bm25_search_test.sql` | BM25 full-text search tests | 19 tests |
| `bitmap_optimization_test.sql` | Bitmap-based optimization tests for large result sets | 17 tests |
| `base.sql` | Base setup for tests | - |

## Running Tests

### With Docker

From the repo root: `./run_all_tests_docker.sh` (builds and runs pg_facets container, then SQL + Go tests).

### Run All Tests (local DB)

```bash
# Connect to your PostgreSQL database and run:
psql -d your_database -f test/sql/complete_test.sql
psql -d your_database -f test/sql/bm25_search_test.sql
psql -d your_database -f test/sql/bitmap_optimization_test.sql
```

### Run Individual Test Files

```bash
# Complete test suite
psql -d your_database -f test/sql/complete_test.sql

# BM25 search tests
psql -d your_database -f test/sql/bm25_search_test.sql

# Bitmap optimization tests (important for large datasets)
psql -d your_database -f test/sql/bitmap_optimization_test.sql
```

## Bitmap Optimization Tests

The `bitmap_optimization_test.sql` file tests the new bitmap-based functions that avoid array explosions when working with large result sets:

### Functions Tested

1. **`filter_documents_by_facets_bitmap()`**
   - Returns a `roaringbitmap` instead of individual document IDs
   - Critical for large result sets (e.g., 8 million documents)
   - Memory efficient: ~few MB for millions of docs vs ~32MB+ for arrays

2. **`hierarchical_facets_bitmap()`**
   - Accepts `roaringbitmap` directly instead of `int[]`
   - Avoids costly `rb_build()` conversion from arrays
   - Faster facet calculation for filtered results

3. **Optimized `search_documents_with_facets()`**
   - Uses bitmap path for empty queries with facet filters
   - Uses `rb_contains()` for text search with facet filters
   - Only extracts actual rows needed for pagination

### Performance Expectations

| Scenario | Before (Arrays) | After (Bitmaps) |
|----------|-----------------|-----------------|
| 8M document filter | ~32MB+ memory, potential OOM | ~few MB, fast |
| Empty query + facet | Multiple array conversions | Direct bitmap ops |
| Facet calculation | Array → Bitmap conversion | Direct bitmap usage |

## Prerequisites

Before running tests, ensure:

1. pg_facets extension is installed
2. roaringbitmap extension is installed
3. You have a test database with sufficient permissions

```sql
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;
```

## Test Output

Tests output PASS/FAIL messages. Look for:
- `PASS:` - Test passed
- `FAIL:` - Test failed (check the message for details)
- `INFO:` - Informational message (not a failure)
- `SKIP:` - Test skipped (usually due to missing prerequisites)
