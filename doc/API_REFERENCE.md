# pg_facets API Reference

**Version 0.4.3** | Complete function reference for pg_facets extension

## Table of Contents

- [Setup Functions](#setup-functions)
- [Facet Operations](#facet-operations)
- [Search Functions](#search-functions)
- [BM25 Functions](#bm25-functions)
- [Maintenance Functions](#maintenance-functions)
- [Monitoring Functions](#monitoring-functions)

## Setup Functions

### `facets.add_faceting_to_table()`
Register a table for faceting and create necessary structures.

```sql
facets.add_faceting_to_table(
    p_table regclass,
    key name,
    facets facets.facet_definition[],
    chunk_bits int = NULL,
    keep_deltas bool = true,
    populate bool = true,
    skip_table_creation bool = false,
    unlogged bool = false
) RETURNS void
```

### `facets.setup_table_with_bm25()`
One-stop setup for faceting + BM25 search (recommended).

```sql
facets.setup_table_with_bm25(
    p_table regclass,
    p_id_column text = 'id',
    p_content_column text = 'content',
    p_facets facets.facet_definition[] = NULL,
    p_language text = 'english',
    p_create_trigger boolean = true,
    p_chunk_bits int = NULL,
    p_populate_facets boolean = true,
    p_build_bm25_index boolean = true,
    p_bm25_workers int = 0
) RETURNS void
```

## Facet Operations

### `facets.get_facet_counts()`
Get counts for a specific facet with optional filtering.

```sql
facets.get_facet_counts(
    p_table_id oid,
    p_facet_name text,
    p_filter_bitmap roaringbitmap = NULL,
    p_limit int = 10
) RETURNS SETOF facets.facet_counts
```

### `facets.hierarchical_facets()`
Get all facet counts with hierarchical organization.

```sql
facets.hierarchical_facets(
    p_table_id oid,
    n integer = 5,
    p_filter_bitmap roaringbitmap = NULL
) RETURNS jsonb
```

### `facets.hierarchical_facets_bitmap()`
Optimized version using bitmap directly (faster).

```sql
facets.hierarchical_facets_bitmap(
    p_table_id oid,
    n integer = 5,
    p_filter_bitmap roaringbitmap = NULL
) RETURNS jsonb
```

### `facets.filter_documents_by_facets_bitmap()`
Filter documents by facet criteria, returns bitmap.

```sql
facets.filter_documents_by_facets_bitmap(
    p_schema_name text,
    p_facets jsonb,
    p_table_name text = NULL
) RETURNS roaringbitmap
```

## Search Functions

### `facets.search_documents_with_facets()`
Combined search with facets, BM25, and optional vector search.

```sql
facets.search_documents_with_facets(
    p_schema_name text,
    p_table_name text,
    p_query text,
    p_facets jsonb = NULL,
    p_vector_column text = NULL,
    p_content_column text = 'content',
    p_metadata_column text = 'metadata',
    p_created_at_column text = 'created_at',
    p_updated_at_column text = 'updated_at',
    p_limit integer = 10,
    p_offset integer = 0,
    p_min_score double precision = 0.0,
    p_vector_weight double precision = 0.5,
    p_facet_limit integer = 5,
    p_language text = NULL
) RETURNS TABLE(
    results jsonb,
    facets jsonb,
    total_found bigint,
    search_time integer
)
```

## BM25 Functions

### Indexing

#### `facets.bm25_index_document()`
Index a single document for BM25 search.

```sql
facets.bm25_index_document(
    p_table_id regclass,
    p_doc_id bigint,
    p_content text,
    p_content_column text = 'content',
    p_language text = NULL
) RETURNS void
```

#### `facets.bm25_index_documents_batch()`
Index multiple documents efficiently.

```sql
facets.bm25_index_documents_batch(
    p_table_id regclass,
    p_documents jsonb,
    p_content_column text = 'content',
    p_language text = 'english'
) RETURNS TABLE(count bigint, elapsed_ms bigint, errors jsonb)
```

#### `facets.bm25_rebuild_index()`
Rebuild entire BM25 index.

```sql
facets.bm25_rebuild_index(
    p_table regclass,
    p_id_column text = 'id',
    p_content_column text = 'content',
    p_language text = 'english',
    p_num_workers int = 0,
    p_connection_string text = NULL,
    p_progress_step_size int = 50000
) RETURNS void
```

### Search

#### `facets.bm25_search()`
Search documents using BM25 scoring.

```sql
facets.bm25_search(
    p_table_id regclass,
    p_query text,
    p_language text = 'english',
    p_prefix_match boolean = false,
    p_fuzzy_match boolean = false,
    p_fuzzy_threshold float = 0.3,
    p_k1 float = 1.2,
    p_b float = 0.75,
    p_limit int = 10
) RETURNS TABLE(doc_id bigint, score float)
```

#### `facets.bm25_score()`
Calculate BM25 score for specific document-query pair.

```sql
facets.bm25_score(
    p_table_id regclass,
    p_query text,
    p_doc_id bigint,
    p_language text = 'english',
    p_k1 float = 1.2,
    p_b float = 0.75
) RETURNS float
```

### Analysis

#### `facets.bm25_term_stats()`
Get top terms by frequency (equivalent to `ts_stat`).

```sql
facets.bm25_term_stats(
    p_table_id oid,
    p_limit int = 10
) RETURNS TABLE(term_text text, ndoc bigint, nentry bigint)
```

#### `facets.bm25_doc_stats()`
Get documents ordered by length.

```sql
facets.bm25_doc_stats(
    p_table_id oid,
    p_limit int = 10
) RETURNS TABLE(doc_id bigint, doc_length int, unique_terms bigint)
```

#### `facets.bm25_collection_stats()`
Get collection-wide statistics.

```sql
facets.bm25_collection_stats(
    p_table_id oid
) RETURNS TABLE(
    total_documents bigint,
    avg_document_length float,
    total_terms bigint,
    unique_terms bigint
)
```

#### `facets.bm25_explain_doc()`
Analyze BM25 score contribution by term for a document.

```sql
facets.bm25_explain_doc(
    p_table_id oid,
    p_doc_id bigint,
    p_k1 float = 1.2,
    p_b float = 0.75
) RETURNS TABLE(
    term_text text,
    tf int,
    df bigint,
    idf float,
    bm25_weight float
)
```

## Maintenance Functions

### Delta Management

#### `facets.merge_deltas()`
Merge pending deltas for a table.

```sql
facets.merge_deltas(p_table regclass) RETURNS int
```

#### `facets.merge_deltas_all()`
Merge deltas for all registered tables.

```sql
facets.merge_deltas_all() RETURNS TABLE(
    table_name text,
    rows_merged bigint,
    elapsed_ms int,
    status text
)
```

#### `facets.merge_deltas_smart()`
Merge only when thresholds are exceeded.

```sql
facets.merge_deltas_smart(
    p_table regclass,
    p_min_delta_count int = 5000,
    p_max_delta_age interval = '10 minutes'
) RETURNS TABLE(
    table_name text,
    rows_merged bigint,
    elapsed_ms int,
    status text
)
```

### Table Management

#### `facets.set_table_unlogged()`
Convert table to UNLOGGED for bulk loading.

```sql
facets.set_table_unlogged(
    p_table regclass,
    p_include_deltas boolean = true,
    p_include_bm25 boolean = true
) RETURNS void
```

#### `facets.set_table_logged()`
Convert table back to LOGGED for durability.

```sql
facets.set_table_logged(
    p_table regclass,
    p_include_deltas boolean = true,
    p_include_bm25 boolean = true
) RETURNS void
```

### BM25 Sync Triggers

#### `facets.bm25_create_sync_trigger()`
Create trigger for automatic BM25 index updates.

```sql
facets.bm25_create_sync_trigger(
    p_table regclass,
    p_id_column text = 'id',
    p_content_column text = 'content',
    p_language text = NULL
) RETURNS void
```

#### `facets.bm25_drop_sync_trigger()`
Remove BM25 sync trigger.

```sql
facets.bm25_drop_sync_trigger(p_table regclass) RETURNS void
```

## Monitoring Functions

### Status Checks

#### `facets.bm25_status()`
Get status of all BM25 indexes.

```sql
facets.bm25_status() RETURNS TABLE(
    table_name text,
    table_oid oid,
    indexed_documents bigint,
    total_terms bigint,
    language text,
    last_updated timestamp
)
```

#### `facets.bm25_progress()`
Check indexing progress for a table.

```sql
facets.bm25_progress(p_table regclass = NULL) RETURNS TABLE(
    table_name text,
    total_documents bigint,
    indexed_documents bigint,
    progress_percent float,
    elapsed_seconds int,
    estimated_completion timestamp
)
```

#### `facets.delta_status()`
Check delta status for all tables.

```sql
facets.delta_status() RETURNS TABLE(
    table_name text,
    delta_count bigint,
    delta_size_mb float,
    recommendation text
)
```

### Cleanup Functions

#### `facets.bm25_cleanup_dblinks()`
Clean up orphaned dblink connections.

```sql
facets.bm25_cleanup_dblinks() RETURNS TABLE(connection_name text, status text)
```

#### `facets.bm25_cleanup_staging()`
Remove orphaned staging tables.

```sql
facets.bm25_cleanup_staging() RETURNS TABLE(table_name text, status text)
```

#### `facets.bm25_full_cleanup()`
Complete cleanup of stuck processes and orphaned resources.

```sql
facets.bm25_full_cleanup(p_kill_threshold interval = '5 minutes') RETURNS TABLE(
    operation text,
    items_processed int,
    status text
)
```

## Data Types

### `facets.facet_counts`
```sql
CREATE TYPE facets.facet_counts AS (
    facet_name text,
    facet_value text,
    cardinality bigint,
    facet_id int
);
```

### `facets.facet_filter`
```sql
CREATE TYPE facets.facet_filter AS (
    facet_name text,
    facet_value text
);
```

### `facets.facet_definition`
Complex type for facet definitions (see main documentation for usage).

## Response Schemas

See [DOCUMENTATION.md](DOCUMENTATION.md) for complete JSON response schemas for all functions.

## Performance Tips

1. Use `hierarchical_facets_bitmap()` instead of `hierarchical_facets()` for better performance
2. Use `setup_table_with_bm25()` for new tables instead of manual setup
3. Enable parallel indexing (`num_workers > 1`) for large datasets (requires `dblink`)
4. Use UNLOGGED tables for bulk loading, then convert to LOGGED
5. Set up pg_cron jobs for automatic delta merging

## Quick Examples

### Complete Setup
```sql
-- One-stop setup with faceting + BM25
SELECT facets.setup_table_with_bm25(
    'public.products'::regclass,
    'id', 'content',
    ARRAY[facets.plain_facet('category'), facets.plain_facet('brand')],
    'english', true, NULL, true, true, 0
);
```

### Search with Facets
```sql
SELECT * FROM facets.search_documents_with_facets(
    'public', 'products', 'laptop', '{"category": "electronics"}'::jsonb,
    NULL, 'content', NULL, 'created_at', 'updated_at',
    20, 0, 0.0, NULL, 10, 'english'
);
```

### Get Facets Only
```sql
SELECT facets.hierarchical_facets_bitmap(
    'public.products'::regclass::oid, 10, NULL
);
```
