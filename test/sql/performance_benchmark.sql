-- Performance Benchmark for search_documents_with_facets
-- Measures execution time for different query patterns
-- Run with: psql -f performance_benchmark.sql

\set ON_ERROR_STOP on
\timing on

\echo '================================================================================'
\echo 'Performance Benchmark: search_documents_with_facets'
\echo '================================================================================'
\echo ''

-- Test 1: BM25 search with facet filter (current slow case)
\echo '--- Test 1: BM25 Search + Facet Filter (Current Slow Case) ---'
\echo 'Query: text="bergman", facet={"primary_profession":"actress"}'
\echo ''

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT * FROM facets.search_documents_with_facets(
    'providers_imdb',  -- p_schema_name
    'name_basics_faceting_mv',  -- p_table_name
    'bergman',  -- p_query
    '{"primary_profession":"actress"}'::jsonb,  -- p_facets (JSONB)
    NULL,  -- p_vector_column
    'content',  -- p_content_column
    'metadata',  -- p_metadata_column
    'created_at',  -- p_created_at_column
    'updated_at',  -- p_updated_at_column
    20, -- p_limit
    0, -- p_offset
    NULL, -- p_min_score
    NULL, -- p_vector_weight
    1000  -- p_facet_limit
);

\echo ''
\echo '--- Test 2: Facet Filter Only (Empty Query) ---'
\echo 'Query: empty, facet={"primary_profession":"actress"}'
\echo ''

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT * FROM facets.search_documents_with_facets(
    'providers_imdb',
    'name_basics_faceting_mv',
    '',  -- empty query
    '{"primary_profession":"actress"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    20,
    0,
    NULL,
    NULL,
    1000
);

\echo ''
\echo '--- Test 3: BM25 Search Only (No Facets) ---'
\echo 'Query: text="bergman", no facets'
\echo ''

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT * FROM facets.search_documents_with_facets(
    'providers_imdb',
    'name_basics_faceting_mv',
    'bergman',
    NULL,  -- no facets
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    20,
    0,
    NULL,
    NULL,
    1000
);

\echo ''
\echo '--- Test 4: Component Timing - Filter Bitmap Creation ---'
\echo ''

EXPLAIN (ANALYZE, BUFFERS)
SELECT facets.filter_documents_by_facets_bitmap(
    'providers_imdb',
    '{"primary_profession":"actress"}'::jsonb,
    'name_basics_faceting_mv'
) AS filter_bitmap;

\echo ''
\echo '--- Test 5: Component Timing - Text Search Only ---'
\echo ''

DO $$
DECLARE
    v_schema text := 'providers_imdb';
    v_table text := 'name_basics_faceting_mv';
    v_query text := 'bergman';
    v_content_col text := 'content';
    v_key text;
BEGIN
    SELECT key INTO v_key
    FROM facets.faceted_table
    WHERE schemaname = v_schema AND tablename = v_table;
    
    EXECUTE format('
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT rb_build_agg(%I) AS search_bitmap
        FROM %I.%I
        WHERE to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $1)
    ', v_key, v_schema, v_table, v_content_col) USING v_query;
END $$;

\echo ''
\echo '--- Test 6: Component Timing - Hierarchical Facets ---'
\echo ''

DO $$
DECLARE
    v_table_id oid;
    v_filter_bitmap roaringbitmap;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'providers_imdb' AND tablename = 'name_basics_faceting_mv';
    
    v_filter_bitmap := facets.filter_documents_by_facets_bitmap(
        'providers_imdb',
        '{"primary_profession":"actress"}'::jsonb,
        'name_basics_faceting_mv'
    );
    
    EXPLAIN (ANALYZE, BUFFERS)
    SELECT facets.hierarchical_facets_bitmap(v_table_id, 1000, v_filter_bitmap);
END $$;

\echo ''
\echo '--- Test 7: Large Result Set (Higher Limit) ---'
\echo 'Query: text="bergman", facet={"primary_profession":"actress"}, limit=100'
\echo ''

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT * FROM facets.search_documents_with_facets(
    'providers_imdb',
    'name_basics_faceting_mv',
    'bergman',
    '{"primary_profession":"actress"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    100,  -- higher limit
    0,
    NULL,
    NULL,
    1000
);

\echo ''
\echo '================================================================================'
\echo 'Benchmark Complete'
\echo '================================================================================'
