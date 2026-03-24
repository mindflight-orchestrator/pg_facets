# BM25 Setup and Maintenance Guide

## Overview

This guide explains how to set up, rebuild, and maintain BM25 full-text search indexes for IMDB data using `pg_facets`. BM25 provides relevance-based search scoring, which is superior to traditional PostgreSQL text search for large datasets.

## BM25 Index Storage Location

BM25 indexes are stored in the **`facets` schema** in three tables:

### 1. `facets.bm25_index` - Inverted Index
- **Purpose**: Maps terms to documents (the actual search index)
- **Structure**:
  - `table_id` (oid): The OID of the indexed table
  - `term_hash` (bigint): Hash of the lexeme (from PostgreSQL's `to_tsvector`)
  - `term_text` (text): Original lexeme text (for debugging/prefix matching)
  - `doc_ids` (roaringbitmap): Bitmap of document IDs containing this term
  - `term_freqs` (jsonb): Map of `doc_id -> term_frequency` for each document
  - `language` (text): Text search config used (default: 'english')

### 2. `facets.bm25_documents` - Document Metadata
- **Purpose**: Stores metadata about each indexed document
- **Structure**:
  - `table_id` (oid): The OID of the indexed table
  - `doc_id` (bigint): Document ID (matches the primary key of the source table)
  - `doc_length` (integer): Number of tokens in the document
  - `language` (text): Text search config used
  - `created_at`, `updated_at` (timestamp): Timestamps

### 3. `facets.bm25_statistics` - Collection Statistics
- **Purpose**: Stores collection-level statistics needed for BM25 scoring
- **Structure**:
  - `table_id` (oid): The OID of the indexed table
  - `total_documents` (bigint): Total number of indexed documents (N)
  - `avg_document_length` (float): Average document length (avgdl)
  - `last_updated` (timestamp): Last update time

## Schema Update Safety

### Safe Operations ✅

These operations **will NOT** affect BM25 indexes:

- **ALTER statements** on `facets` schema tables (adding columns, modifying functions, etc.)
- **Updates to `providers_imdb` schema** - BM25 data is stored in `facets` schema, so changes to your application schema won't affect it
- **Function updates** - Updating SQL functions won't drop BM25 data
- **Extension updates** - Updating `pg_facets` extension (via `ALTER EXTENSION`) preserves data
- **Index creation/dropping** - Creating or dropping indexes on BM25 tables is safe

### Unsafe Operations ⚠️

These operations **WILL** delete BM25 indexes:

- **DROP SCHEMA facets** - This will delete all BM25 data
- **DROP TABLE facets.faceted_table** - This will CASCADE delete all BM25 data for that table
- **DROP TABLE from facets.faceted_table** - Removing a table registration will cascade delete its BM25 indexes

### Backup Strategy

Before performing any potentially unsafe operations, backup your BM25 indexes:

```bash
# Backup all BM25 tables
pg_dump -t facets.bm25_index -t facets.bm25_documents -t facets.bm25_statistics your_database > bm25_backup.sql

# Or backup specific table's BM25 data
psql -d your_database -c "
  COPY (
    SELECT * FROM facets.bm25_index 
    WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid
  ) TO STDOUT WITH CSV HEADER
" > title_bm25_index_backup.csv
```

## Setup Process

### Prerequisites

1. **Run faceting setup first** (creates the faceting tables and registers them):
   ```sql
   \i application/sql/04_faceting_setup.sql
   ```

2. **Install required extensions**:
   ```sql
   CREATE EXTENSION IF NOT EXISTS pg_facets;
   CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- Optional: for fuzzy/prefix matching
   CREATE EXTENSION IF NOT EXISTS dblink;   -- Optional: for parallel indexing
   ```

### Initial Setup

1. **Run BM25 setup script**:
   ```sql
   \i application/sql/06_bm25_setup.sql
   ```

   This creates:
   - BM25 sync triggers (keeps indexes up-to-date on row changes)
   - Rebuild functions for bulk indexing
   - Helper functions for titles and names

2. **Verify tables are registered**:
   ```sql
   SELECT table_id::regclass::text, key_column 
   FROM facets.faceted_table 
   WHERE table_id::regclass::text LIKE '%faceting_mv%';
   ```

   Should show:
   - `providers_imdb.title_basics_faceting_mv`
   - `providers_imdb.name_basics_faceting_mv`

## Bulk Indexing Methods

### Method 1: Parallel Indexing (RECOMMENDED for Large Datasets)

**Best for**: Large datasets, when `dblink` extension is available

**Note**: The optimized parallel mode uses a **staging table with ROW_NUMBER()** instead of OFFSET/LIMIT, which is 90-95% faster for large datasets. All workers can start within 30 seconds instead of 15-30 minutes.

**How it works**: Spawns multiple database connections to process documents in parallel

**Usage**:
```sql
-- Auto-detect workers (uses 4 if dblink available, otherwise sequential)
SELECT providers_imdb.rebuild_title_basics_bm25();

-- Explicitly use 8 parallel workers
SELECT providers_imdb.rebuild_title_basics_bm25(p_num_workers => 8);

-- With custom connection string
SELECT providers_imdb.rebuild_title_basics_bm25(
    p_num_workers => 4,
    p_connection_string => 'dbname=imdb host=localhost'
);
```

**Advantages**:
- Fastest method for large datasets (90-95% faster than OFFSET-based partitioning)
- All workers start within 30 seconds (vs 15-30 minutes with OFFSET)
- Automatically partitions work across workers using staging table
- Each worker processes its partition independently
- Staging table is automatically cleaned up after indexing

**Requirements**:
- `dblink` extension must be installed
- Sufficient database connections available

#### Why dblink is Needed for Parallelism (Even on Localhost)

**The Problem**: PostgreSQL's PL/pgSQL functions run in a **single transaction/session**. Even though PostgreSQL has parallel query features (`max_parallel_workers_per_gather`), these only work for **SELECT queries**, not for procedural code that performs **INSERT/UPDATE operations** (like BM25 indexing).

**Without dblink**:
- A single function processes documents **sequentially** (one at a time)
- All operations happen in one transaction
- You cannot spawn concurrent workers within the same function execution
- Result: Slow, single-threaded processing

**With dblink**:
- Each dblink connection creates a **separate database session**
- Each session runs **independently** and concurrently
- Multiple sessions can process different partitions simultaneously
- Even on localhost, these are separate connections to the same database
- Result: True parallelism with 4-8 workers processing simultaneously

**Why it's faster despite connection overhead**:

1. **I/O-bound operations**: BM25 indexing is primarily I/O-bound (reading documents, writing to tables). Multiple workers can utilize multiple CPU cores and disk I/O channels simultaneously.

2. **Minimal overhead**: The overhead of dblink connections (even on localhost) is **negligible** compared to the time spent indexing:
   - Connection setup: ~1-5ms per connection
   - Indexing per document: ~0.1-1ms per document
   - For 1M documents: Connection overhead = 4-20ms total, Indexing time = 100-1000 seconds

3. **Partitioning**: Each worker processes a different partition of data, so they don't compete for the same rows. This reduces lock contention.

4. **Example performance**:
   - **Sequential (1 worker)**: 1M documents × 0.5ms/doc = **~8.3 minutes**
   - **Parallel (4 workers)**: 1M documents ÷ 4 × 0.5ms/doc = **~2.1 minutes** (4× faster)
   - Even with 10ms connection overhead per worker, you save **~6 minutes**

**Localhost vs Remote**: The performance benefit is the same whether connecting to localhost or a remote server. The key is having **multiple independent sessions** running concurrently, not the network latency (which is minimal on localhost anyway).

**Alternative without dblink**: You could manually open multiple psql sessions and run different partitions, but dblink automates this and handles coordination, error handling, and result collection.

### Method 2: Sequential Indexing with Progress Reporting

**Best for**: When parallel indexing isn't available, or for debugging

**Advantages**: 
- Uses cursor-based iteration (no OFFSET) - faster than OFFSET for large datasets
- Provides real-time progress reporting
- Easier to debug issues

**How it works**: Processes documents one-by-one using cursor-based iteration (faster than OFFSET)

**Usage**:
```sql
-- Sequential with default progress reporting (every 50K docs)
SELECT providers_imdb.rebuild_title_basics_bm25(p_num_workers => 1);

-- Sequential with custom progress step size (every 10K docs)
SELECT providers_imdb.rebuild_title_basics_bm25(
    p_num_workers => 1,
    p_progress_step_size => 10000
);
```

**Progress Output Example**:
```
[BM25 REBUILD] providers_imdb.title_basics_faceting_mv: Progress: 50000 / 1000000 documents (5.0%) - Elapsed: 45.2 seconds - Rate: 1106 docs/sec - Estimated remaining: 860.3 seconds (14.3 minutes)
```

**Advantages**:
- Works without `dblink` extension
- Provides detailed progress reporting
- Easier to debug issues
- Uses cursor-based iteration (faster than OFFSET for large datasets)

### Method 3: Batch Indexing (for Small-Medium Datasets)

**Best for**: Small to medium datasets (<100K documents), or when you need to index specific documents

**How it works**: Processes a JSONB array of documents in batches

**Usage**:
```sql
SELECT * FROM facets.bm25_index_documents_batch(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    (
        SELECT jsonb_agg(jsonb_build_object(
            'doc_id', id,
            'content', content
        ))
        FROM providers_imdb.title_basics_faceting_mv
        LIMIT 10000
    ),
    'content',
    'english',
    1000  -- batch size for statistics recalculation
);
```

**Advantages**:
- Good for incremental updates
- Can process specific document subsets
- Automatic statistics recalculation

## Rebuilding BM25 Indexes

### When to Rebuild

Rebuild BM25 indexes when:
- Initial setup (first time indexing)
- After bulk data imports
- After major data updates
- If indexes become corrupted or inconsistent
- After schema changes that affect content columns

### Rebuild Process

1. **For Titles**:
   ```sql
   -- Parallel (recommended if dblink available)
   SELECT providers_imdb.rebuild_title_basics_bm25(p_num_workers => 10);
   
   -- Sequential with progress reporting
   SELECT providers_imdb.rebuild_title_basics_bm25(p_num_workers => 1);
   ```

2. **For Names**:
   ```sql
   SELECT providers_imdb.rebuild_name_basics_bm25();
   ```

3. **Refresh Faceting Tables and Rebuild BM25** (one-step process):
   ```sql
   -- Refresh faceting table, then rebuild BM25
   SELECT providers_imdb.refresh_title_facets_and_bm25();
   SELECT providers_imdb.refresh_name_facets_and_bm25();
   ```

### Expected Duration

For a dataset with **~10 million documents**:
- **Parallel (4 workers)**: ~30-60 minutes
- **Sequential**: ~2-4 hours

Progress reporting will show estimated completion time.

## Verification and Troubleshooting

### Verify Indexes Were Created

After rebuilding, verify that data was indexed:

```sql
-- Check document count
SELECT COUNT(*) as indexed_documents
FROM facets.bm25_documents
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;

-- Check term count
SELECT COUNT(*) as indexed_terms
FROM facets.bm25_index
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;

-- Check statistics
SELECT * FROM facets.bm25_statistics
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;
```

### Common Issues

#### Issue 1: Empty Tables After Rebuild

**Symptoms**: Tables `bm25_index`, `bm25_documents`, and `bm25_statistics` are empty after rebuild

**Possible Causes**:
1. **Table not registered**: The faceting table must be registered in `facets.faceted_table`
   ```sql
   -- Check registration
   SELECT * FROM facets.faceted_table 
   WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;
   
   -- If not registered, run:
   \i application/sql/04_faceting_setup.sql
   ```

2. **Transaction rollback**: Check PostgreSQL logs for errors that caused rollback
   ```sql
   -- Check for recent errors
   SELECT * FROM pg_stat_statements 
   WHERE query LIKE '%bm25%' 
   ORDER BY calls DESC;
   ```

3. **No content**: Verify that the `content` column has data
   ```sql
   SELECT COUNT(*) FROM providers_imdb.title_basics_faceting_mv 
   WHERE content IS NOT NULL AND content <> '';
   ```

#### Issue 2: Rebuild Takes Too Long

**Solutions**:
1. **Use parallel indexing** (if dblink available):
   ```sql
   SELECT providers_imdb.rebuild_title_basics_bm25(p_num_workers => 8);
   ```

2. **Increase progress step size** to reduce logging overhead:
   ```sql
   SELECT providers_imdb.rebuild_title_basics_bm25(
       p_num_workers => 1,
       p_progress_step_size => 100000  -- Report every 100K instead of 50K
   );
   ```

3. **Optimize PostgreSQL settings**:
   ```sql
   SET work_mem = '512MB';
   SET maintenance_work_mem = '4GB';
   SET max_parallel_workers_per_gather = 16;
   ```

#### Issue 3: Progress Reporting Shows No Progress

**Check**:
1. Verify the function is actually running (check `pg_stat_activity`)
2. Check PostgreSQL logs for errors
3. Verify documents exist in the source table
4. Check that the `content` column is not empty

### Diagnostic Queries

Use the built-in monitoring and cleanup functions (available after running `06_bm25_setup.sql`):

```sql
-- Check index status
SELECT * FROM facets.bm25_status();

-- Check progress during rebuild (in another session)
SELECT * FROM facets.bm25_progress('providers_imdb.title_basics_faceting_mv'::regclass);

-- Check active BM25 processes
SELECT * FROM facets.bm25_active_processes();

-- Full cleanup (disconnect dblinks, drop staging tables, kill stuck processes)
SELECT * FROM facets.bm25_full_cleanup();

-- Individual cleanup functions
SELECT * FROM facets.bm25_cleanup_dblinks();
SELECT * FROM facets.bm25_cleanup_staging();
SELECT * FROM facets.bm25_kill_stuck('5 minutes');
```

Quick manual checks:

```sql
-- Check if table is registered
SELECT facets.bm25_is_table_registered('providers_imdb.title_basics_faceting_mv'::regclass);

-- Get comprehensive index statistics
SELECT * FROM facets.bm25_get_index_stats('providers_imdb.title_basics_faceting_mv'::regclass);

-- Check sample indexed terms
SELECT term_text, rb_cardinality(doc_ids) as doc_count
FROM facets.bm25_index
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid
ORDER BY doc_count DESC
LIMIT 20;

-- Test a search
SELECT * FROM facets.bm25_search(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    'star wars',
    'english',
    false,  -- prefix_match
    false,  -- fuzzy_match
    0.3,    -- fuzzy_threshold
    1.2,    -- k1
    0.75,   -- b
    10      -- limit
);
```

## Performance Optimization

### For Large Datasets

1. **Use parallel indexing**: Always prefer parallel mode when `dblink` is available
2. **Adjust worker count**: More workers = faster, but more database connections
   - Recommended: 4-8 workers for most systems
   - Maximum: Limited by `max_connections` and available CPU cores

3. **Optimize PostgreSQL settings**:
   ```sql
   -- Before rebuild
   SET work_mem = '512MB';
   SET maintenance_work_mem = '4GB';
   SET max_parallel_workers_per_gather = 16;
   SET max_parallel_workers = 32;
   ```

4. **Monitor progress**: Use progress reporting to track indexing speed
   - If rate drops significantly, check for locks or resource contention

### For Incremental Updates

Use triggers (automatically set up by `06_bm25_setup.sql`) for incremental updates:
- Triggers automatically index new documents
- Triggers update indexes when content changes
- Triggers remove documents when rows are deleted

For bulk updates, rebuild the entire index for best performance.

## Maintenance

### Regular Maintenance

1. **Monitor index size**:
   ```sql
   SELECT 
       pg_size_pretty(pg_total_relation_size('facets.bm25_index')) as index_size,
       pg_size_pretty(pg_total_relation_size('facets.bm25_documents')) as documents_size;
   ```

2. **Check statistics freshness**:
   ```sql
   SELECT 
       table_id::regclass::text,
       total_documents,
       avg_document_length,
       last_updated
   FROM facets.bm25_statistics
   ORDER BY last_updated DESC;
   ```

3. **Rebuild if needed**: If statistics are stale or indexes seem inconsistent, rebuild

### Backup and Restore

**Backup**:
```bash
pg_dump -t facets.bm25_index -t facets.bm25_documents -t facets.bm25_statistics imdb > bm25_backup.sql
```

**Restore**:
```bash
psql -d imdb < bm25_backup.sql
```

## Best Practices

1. **Always run faceting setup first** (`04_faceting_setup.sql`) before BM25 setup
2. **Use parallel indexing** for initial builds on large datasets
3. **Monitor progress** during rebuilds to catch issues early
4. **Verify indexes** after rebuild to ensure data was indexed
5. **Backup before major schema changes** that might affect BM25 tables
6. **Use triggers** for incremental updates (already configured)
7. **Rebuild periodically** if you do bulk updates outside of triggers

## Function Reference

### Rebuild Functions

- `providers_imdb.rebuild_bm25_index_for_table()` - Generic rebuild function
- `providers_imdb.rebuild_title_basics_bm25()` - Rebuild titles index
- `providers_imdb.rebuild_name_basics_bm25()` - Rebuild names index
- `providers_imdb.refresh_title_facets_and_bm25()` - Refresh faceting + rebuild BM25
- `providers_imdb.refresh_name_facets_and_bm25()` - Refresh faceting + rebuild BM25

### Parameters

- `p_num_workers`: Number of parallel workers (0 = auto, 1 = sequential, >1 = parallel)
- `p_connection_string`: Database connection string for parallel mode (NULL = current DB)
- `p_progress_step_size`: Progress reporting frequency in documents (default: 50000)

### pg_facets Functions

- `facets.bm25_index_document()` - Index a single document
- `facets.bm25_index_documents_batch()` - Index documents from JSONB array
- `facets.bm25_index_documents_parallel()` - Parallel indexing with workers
- `facets.bm25_search()` - Search with BM25 scoring
- `facets.bm25_recalculate_statistics()` - Recalculate collection statistics
- `facets.bm25_get_index_stats()` - Get index statistics
- `facets.bm25_is_table_registered()` - Check if table is registered

## Additional Resources

- [pg_facets Documentation](../pg_facets/code_reference/pg_facets/DOCUMENTATION.md)
- [BM25 Indexing Review](../pg_facets/code_reference/pg_facets/BM25_INDEXING_REVIEW.md)
- [BM25 Integration Summary](../pg_facets/code_reference/pg_facets/BM25_INTEGRATION_SUMMARY.md)

