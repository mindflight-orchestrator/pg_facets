# BM25 Search Integration Guide

This guide explains how to set up and use BM25 (Best Matching 25) full-text search with pg_facets, based on the concrete SQL setup files in the examples directory.

## Overview

BM25 is a ranking function used to estimate the relevance of documents to a given search query. In pg_facets, BM25 search can be used independently or combined with faceted filtering and vector embeddings for hybrid search.

## Architecture

BM25 indexes in pg_facets are stored in three tables in the `facets` schema:

### 1. `facets.bm25_index` - Inverted Index
The main inverted index mapping terms to documents:
- `table_id` (oid): The OID of the indexed table
- `term_hash` (bigint): Hash of the lexeme (from PostgreSQL's `to_tsvector`)
- `term_text` (text): Original lexeme text (for debugging/prefix matching)
- `doc_ids` (roaringbitmap): Bitmap of document IDs containing this term
- `term_freqs` (jsonb): Map of `doc_id -> term_frequency` for each document
- `language` (text): Text search config used (default: 'english')

### 2. `facets.bm25_documents` - Document Metadata
Stores metadata about each indexed document:
- `table_id` (oid): The OID of the indexed table
- `doc_id` (bigint): Document ID (matches the primary key of the source table)
- `doc_length` (integer): Number of tokens in the document
- `language` (text): Text search config used
- `created_at`, `updated_at` (timestamp): Timestamps

### 3. `facets.bm25_statistics` - Collection Statistics
Stores collection-level statistics needed for BM25 scoring:
- `table_id` (oid): The OID of the indexed table
- `total_documents` (bigint): Total number of indexed documents (N)
- `avg_document_length` (float): Average document length (avgdl)
- `last_updated` (timestamp): Last update time

## Setup Process

Based on `examples/04_faceting_setup.sql` and `examples/06_bm25_setup.sql`, here's the complete setup process:

### Step 1: Create Table with Content Column

The table must have a `content` column (or another named column) that contains the text to be indexed:

```sql
CREATE TABLE providers_imdb.title_basics_faceting_mv AS
SELECT 
    ABS(HASHTEXT(tb.tconst))::INTEGER AS id,
    ABS(HASHTEXT(tb.tconst))::INTEGER AS document_id,
    
    -- Content for full-text search (combine relevant columns)
    COALESCE(
        tb.primaryTitle || ' ' || 
        COALESCE(tb.originalTitle, '') || ' ' || 
        COALESCE(tb.genres, ''),
        ''
    ) AS content,
    
    -- Metadata as JSONB
    jsonb_build_object(
        'tconst', tb.tconst,
        'titleType', tb.titleType,
        'primaryTitle', tb.primaryTitle,
        -- ... other fields
    ) AS metadata,
    
    -- Timestamps
    COALESCE(
        make_timestamp(COALESCE(tb.startYear, 1900), 1, 1, 0, 0, 0),
        CURRENT_TIMESTAMP
    ) AS created_at,
    
    CURRENT_TIMESTAMP AS updated_at,
    
    -- Facet columns
    tb.titleType AS title_type,
    -- ... other facet columns
    
FROM providers_imdb.title_basics tb
WHERE tb.tconst IS NOT NULL;
```

**Key Points:**
- The `content` column name is important - it's used by `facets.search_documents_with_facets()` (defaults to 'content' if not specified)
- Combine multiple text columns into a single `content` column for comprehensive indexing
- The `document_id` column is required for compatibility with `facets.search_documents_with_facets()`

### Step 2: Register Table with Facets

Before BM25 indexing, the table must be registered with `facets.add_faceting_to_table()`:

```sql
SELECT facets.add_faceting_to_table(
    'providers_imdb.title_basics_faceting_mv',
    'id',  -- id column
    ARRAY[
        facets.plain_facet('title_type'),
        facets.plain_facet('primary_genre'),
        -- ... other facets
    ]::facets.facet_definition[],
    20,   -- chunk_bits (2^20 = ~1M documents per chunk)
    TRUE, -- keep_deltas (for incremental updates)
    TRUE  -- populate (initial population)
);
```

**Note:** Even if you only need BM25 (no facets), you still need to register the table. Pass an empty array for facets:

```sql
SELECT facets.add_faceting_to_table(
    'providers_imdb.title_basics_faceting_mv',
    'id',
    ARRAY[]::facets.facet_definition[],
    20,
    TRUE,
    FALSE  -- Don't populate facets, just register the table
);
```

### Step 3: Create BM25 Sync Triggers

Using `facets.bm25_create_sync_trigger()` (available in pg_facets 0.4.2+):

```sql
PERFORM facets.bm25_create_sync_trigger(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    'id',        -- id_column
    'content',   -- content_column
    'english'    -- language (or NULL to use table's bm25_language)
);
```

This automatically:
- Creates a trigger function that handles INSERT/UPDATE/DELETE
- Creates the trigger on the table
- Keeps the BM25 index in sync with table changes

### Step 4: Build Initial BM25 Index

For initial indexing, use `facets.bm25_rebuild_index()`:

```sql
SELECT facets.bm25_rebuild_index(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    'id',        -- id_column
    'content',   -- content_column
    'english',   -- language
    0,           -- num_workers (0 = auto, 1 = sequential, >1 = parallel)
    NULL,        -- connection_string (for parallel mode)
    50000        -- progress_step_size (report progress every N documents)
);
```

**Performance Options:**
- `num_workers = 0`: Auto-detect (uses parallel if `dblink` extension available)
- `num_workers = 1`: Sequential indexing (slower but no dependencies)
- `num_workers > 1`: Parallel indexing (requires `dblink` extension, 90-95% faster)

### Step 5: Create GIN Index (Optional but Recommended)

For faster text search queries, create a GIN index on the content column:

```sql
CREATE INDEX title_basics_content_gin_idx 
ON providers_imdb.title_basics_faceting_mv 
USING GIN (to_tsvector('english', content));
```

**Note:** This is optional if you're only using BM25 search, but recommended if you also use `facets.search_documents_with_facets()` with text search.

## Usage Examples

### Basic BM25 Search

```sql
SELECT * FROM facets.bm25_search(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    'star wars',      -- search query
    'english',        -- language
    false,            -- prefix_match
    false,            -- fuzzy_match
    0.3,              -- fuzzy_threshold
    1.2,              -- k1 (BM25 parameter)
    0.75,             -- b (BM25 parameter)
    10                -- limit
);
```

### BM25 Search with Facets

Use `facets.search_documents_with_facets()` to combine BM25 search with facet filtering:

```sql
SELECT * FROM facets.search_documents_with_facets(
    'providers_imdb',                                    -- schema
    'title_basics_faceting_mv',                          -- table
    'star wars',                                         -- BM25 search query
    '{"title_type": "movie"}'::jsonb,                    -- facet filters
    NULL,                                                -- vector_column (optional)
    'content',                                           -- content_column
    'metadata',                                          -- metadata_column
    'created_at',                                        -- created_at_column
    'updated_at',                                        -- updated_at_column
    10,                                                  -- limit
    0,                                                   -- offset
    0.0,                                                 -- min_score
    NULL,                                                -- vector_weight
    20                                                   -- facet_limit
);
```

### BM25 Response Schema

BM25 search functions return structured data for easy frontend integration:

#### Basic BM25 Search Response
```json
[
  {
    "doc_id": 12345,
    "score": 1.234
  },
  {
    "doc_id": 67890,
    "score": 0.987
  }
]
```

#### BM25 with Facets Response (via `search_documents_with_facets`)
```json
{
  "results": [
    {
      "document_id": 12345,
      "score": 0.85,
      "content": "High-performance laptop...",
      "metadata": {"title": "Gaming Laptop", "category": "electronics"}
    }
  ],
  "facets": {
    "regular_facets": {
      "category": [
        {"facet_name": "category", "facet_value": "electronics", "cardinality": 1250, "facet_id": 1}
      ]
    }
  },
  "total_found": 2500,
  "search_time": 45
}
```

### Combined BM25 + Vector Search

```sql
SELECT * FROM facets.search_documents_with_facets(
    'providers_imdb',
    'title_basics_faceting_mv',
    'star wars',                                         -- BM25 query
    '{"title_type": "movie"}'::jsonb,                    -- facets
    'embedding',                                         -- vector column
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    0.0,
    0.5,                                                 -- vector_weight (0.5 = 50% BM25, 50% vector)
    20
);
```

## Rebuilding BM25 Indexes

### Full Rebuild

For a complete rebuild (useful after bulk data changes):

```sql
SELECT facets.bm25_rebuild_index(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    'id',
    'content',
    'english',
    4,              -- Use 4 parallel workers
    NULL,           -- Use current database connection
    50000           -- Report progress every 50K documents
);
```

### Project-Specific Wrapper Functions

Based on `examples/06_bm25_setup.sql`, you can create convenience wrappers:

```sql
CREATE OR REPLACE FUNCTION providers_imdb.rebuild_title_basics_bm25(
    p_num_workers int DEFAULT 0,
    p_connection_string text DEFAULT NULL,
    p_progress_step_size int DEFAULT 50000
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM facets.bm25_rebuild_index(
        'providers_imdb.title_basics_faceting_mv'::regclass,
        'id',
        'content',
        'english',
        p_num_workers,
        p_connection_string,
        p_progress_step_size
    );
END;
$$;
```

Then call it simply:

```sql
SELECT providers_imdb.rebuild_title_basics_bm25(4);  -- Use 4 workers
```

### Combined Facet + BM25 Refresh

For refreshing both facets and BM25 together:

```sql
CREATE OR REPLACE FUNCTION providers_imdb.refresh_title_facets_and_bm25(
    p_resume boolean DEFAULT false,
    p_bm25_num_workers int DEFAULT 0,
    p_bm25_connection_string text DEFAULT NULL,
    p_bm25_progress_step_size int DEFAULT 50000
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Refresh facets
    PERFORM providers_imdb.refresh_title_basics_faceting(p_resume);
    
    -- Rebuild BM25
    PERFORM providers_imdb.rebuild_title_basics_bm25(
        p_bm25_num_workers, 
        p_bm25_connection_string, 
        p_bm25_progress_step_size
    );
END;
$$;
```

## Monitoring and Maintenance

### Check BM25 Index Status

```sql
SELECT * FROM facets.bm25_status();
```

### Check Indexing Progress

```sql
SELECT * FROM facets.bm25_progress('providers_imdb.title_basics_faceting_mv'::regclass);
```

### Verify Documents Are Indexed

```sql
SELECT COUNT(*) as indexed_docs
FROM facets.bm25_documents
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;
```

### Check Statistics

```sql
SELECT * FROM facets.bm25_statistics
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;
```

### View Sample Indexed Terms

```sql
SELECT term_text, rb_cardinality(doc_ids) as doc_count
FROM facets.bm25_index
WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid
ORDER BY doc_count DESC
LIMIT 20;
```

## Cleanup Functions

### Clean Up Stuck Processes

```sql
SELECT * FROM facets.bm25_kill_stuck('5 minutes'::interval);
```

### Clean Up Orphaned Staging Tables

```sql
SELECT * FROM facets.bm25_cleanup_staging();
```

### Full Cleanup

```sql
SELECT * FROM facets.bm25_full_cleanup('5 minutes'::interval);
```

## Performance Considerations

### Parallel Indexing

For large datasets (millions of documents), parallel indexing is highly recommended:

1. **Install `dblink` extension:**
   ```sql
   CREATE EXTENSION IF NOT EXISTS dblink;
   ```

2. **Use parallel indexing:**
   ```sql
   SELECT facets.bm25_rebuild_index(
       'providers_imdb.title_basics_faceting_mv'::regclass,
       'id',
       'content',
       'english',
       4,              -- 4 parallel workers
       NULL,           -- Use current connection
       50000
   );
   ```

**Performance gains:**
- Sequential: ~1,000-2,000 documents/second
- Parallel (4 workers): ~10,000-20,000 documents/second
- 90-95% faster for large datasets

### Incremental Updates

BM25 sync triggers automatically keep the index updated for:
- INSERT: New documents are indexed immediately
- UPDATE: Changed content is re-indexed
- DELETE: Documents are removed from the index

For bulk updates, consider:
1. Disable triggers temporarily
2. Perform bulk updates
3. Rebuild index once at the end

```sql
-- Disable trigger
ALTER TABLE providers_imdb.title_basics_faceting_mv DISABLE TRIGGER ALL;

-- Bulk updates
UPDATE providers_imdb.title_basics_faceting_mv SET ...;

-- Rebuild index
SELECT facets.bm25_rebuild_index(...);

-- Re-enable trigger
ALTER TABLE providers_imdb.title_basics_faceting_mv ENABLE TRIGGER ALL;
```

## Schema Safety

BM25 indexes are stored in the `facets` schema, separate from your application schema:

**Safe Operations:**
- ✅ ALTER statements on your application tables
- ✅ Adding/removing columns (except the content column)
- ✅ Function updates
- ✅ Updates to your application schema

**Unsafe Operations:**
- ❌ DROP SCHEMA facets (will delete all BM25 data)
- ❌ DROP TABLE from facets.faceted_table (will CASCADE delete BM25 data)
- ❌ Dropping the content column

**Backup Strategy:**
Before major schema changes, backup BM25 data:

```bash
pg_dump -t facets.bm25_index -t facets.bm25_documents -t facets.bm25_statistics
```

## Troubleshooting

### Issue: Index Remains Empty After Rebuild

**Check:**
1. Table is registered in `facets.faceted_table`:
   ```sql
   SELECT * FROM facets.faceted_table 
   WHERE table_id = 'providers_imdb.title_basics_faceting_mv'::regclass::oid;
   ```

2. Content column exists and has data:
   ```sql
   SELECT COUNT(*) FROM providers_imdb.title_basics_faceting_mv WHERE content IS NOT NULL;
   ```

3. Check PostgreSQL logs for errors during rebuild

### Issue: Rebuild Takes Too Long

**Solutions:**
- Use parallel indexing (`num_workers > 1`)
- Increase `progress_step_size` to reduce logging overhead
- Check system resources (CPU, memory, disk I/O)

### Issue: Statistics Not Updated

**Solution:** Statistics are automatically recalculated by `bm25_rebuild_index()`. If needed, manually recalculate:

```sql
SELECT facets.bm25_recalculate_statistics('providers_imdb.title_basics_faceting_mv'::regclass);
```

### Issue: Sync Trigger Not Working

**Check:**
1. Trigger exists:
   ```sql
   SELECT * FROM pg_trigger WHERE tgname LIKE '%bm25%';
   ```

2. Trigger is enabled:
   ```sql
   SELECT tgname, tgenabled FROM pg_trigger WHERE tgname LIKE '%bm25%';
   ```

3. Recreate trigger if needed:
   ```sql
   DROP TRIGGER IF EXISTS bm25_sync_trigger ON providers_imdb.title_basics_faceting_mv;
   PERFORM facets.bm25_create_sync_trigger(...);
   ```

## Best Practices

1. **Always register tables** with `facets.add_faceting_to_table()` before indexing
2. **Use parallel indexing** for large datasets (requires `dblink` extension)
3. **Create sync triggers** to keep indexes up-to-date automatically
4. **Monitor progress** during large rebuilds using `facets.bm25_progress()`
5. **Backup BM25 data** before major schema changes
6. **Use appropriate chunk_bits** when registering tables (20 = ~1M docs per chunk)
7. **Combine with facets** for powerful hybrid search capabilities

## Integration with Facets

BM25 search works seamlessly with faceted filtering:

1. **Facets narrow the search space** (e.g., filter to specific categories)
2. **BM25 ranks within filtered results** (e.g., most relevant matches)
3. **Vector search can be added** for semantic similarity (hybrid search)

This three-layer approach provides:
- **Precision**: Facets ensure results are in the right category
- **Relevance**: BM25 ranks by text match quality
- **Semantic understanding**: Vector search finds conceptually similar content

## References

- **Setup Files:**
  - `examples/04_faceting_setup.sql` - Table and facet setup
  - `examples/06_bm25_setup.sql` - BM25 trigger and rebuild functions

- **Core Functions:**
  - `facets.bm25_rebuild_index()` - Rebuild BM25 index
  - `facets.bm25_search()` - Search using BM25
  - `facets.bm25_create_sync_trigger()` - Create sync trigger
  - `facets.search_documents_with_facets()` - Combined search with facets

- **Monitoring:**
  - `facets.bm25_status()` - Check index status
  - `facets.bm25_progress()` - Check indexing progress
  - `facets.bm25_active_processes()` - List running processes

