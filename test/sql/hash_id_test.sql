-- =============================================================================
-- TEST: Hash-based ID Reconstruction
-- This test verifies that facets work correctly with hash-based (non-sequential) IDs
-- The bug was that bitmap IDs were being used directly without reconstruction from
-- (chunk_id << chunk_bits) | in_chunk_id
-- =============================================================================

\echo '=== TEST: Hash-based ID Faceting ==='

-- Cleanup
DROP SCHEMA IF EXISTS test_hash_ids CASCADE;
CREATE SCHEMA test_hash_ids;

-- Create test table with HASH-based IDs (simulating real-world scenario)
-- Using ABS(HASHTEXT(...))::INTEGER to generate large, non-sequential IDs
CREATE TABLE test_hash_ids.documents (
    id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert test data with hash-based IDs
INSERT INTO test_hash_ids.documents (id, category, title) VALUES
    (ABS(HASHTEXT('doc1'))::INTEGER, 'Electronics', 'Smartphone X'),
    (ABS(HASHTEXT('doc2'))::INTEGER, 'Electronics', 'Laptop Pro'),
    (ABS(HASHTEXT('doc3'))::INTEGER, 'Electronics', 'Tablet Z'),
    (ABS(HASHTEXT('doc4'))::INTEGER, 'Books', 'SQL Cookbook'),
    (ABS(HASHTEXT('doc5'))::INTEGER, 'Books', 'Zig Programming'),
    (ABS(HASHTEXT('doc6'))::INTEGER, 'Clothing', 'T-Shirt'),
    (ABS(HASHTEXT('doc7'))::INTEGER, 'Clothing', 'Jeans'),
    (ABS(HASHTEXT('doc8'))::INTEGER, 'Electronics', 'Headphones'),
    (ABS(HASHTEXT('doc9'))::INTEGER, 'Books', 'PostgreSQL Guide'),
    (ABS(HASHTEXT('doc10'))::INTEGER, 'Clothing', 'Jacket');

-- Show the generated IDs (they should be large, non-sequential)
\echo ''
\echo 'Generated hash-based IDs:'
SELECT id, category, title FROM test_hash_ids.documents ORDER BY id LIMIT 5;
SELECT MIN(id) as min_id, MAX(id) as max_id FROM test_hash_ids.documents;

-- Add faceting
\echo ''
\echo 'Adding faceting to table...'
SELECT facets.add_faceting_to_table(
    'test_hash_ids.documents'::regclass,
    'id',
    ARRAY[
        ROW(NULL, NULL, 'category', 'plain', 'category', NULL, false, true)::facets.facet_definition
    ]::facets.facet_definition[],
    16,  -- Use small chunk_bits to ensure multiple chunks for hash IDs
    true,
    true
);

-- Show chunk_bits configured
\echo ''
\echo 'Chunk bits configured:'
SELECT chunk_bits FROM facets.faceted_table WHERE tablename = 'documents';

-- Show what's in the facets table
\echo ''
\echo 'Facets table contents (showing chunk_id distribution):'
SELECT facet_id, facet_value, chunk_id, rb_cardinality(postinglist) as doc_count
FROM test_hash_ids.documents_facets
ORDER BY facet_value, chunk_id;

-- =============================================================================
-- TEST 1: filter_documents_by_facets_bitmap should return ORIGINAL IDs
-- =============================================================================
\echo ''
\echo '--- TEST 1: filter_documents_by_facets_bitmap returns original IDs ---'

WITH bitmap_result AS (
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_hash_ids',
        '{"category":"Electronics"}'::jsonb,
        'documents'
    ) AS bm
),
bitmap_ids AS (
    SELECT unnest(rb_to_array(bm)) AS bitmap_id FROM bitmap_result
),
expected_ids AS (
    SELECT id FROM test_hash_ids.documents WHERE category = 'Electronics'
)
SELECT 
    CASE 
        WHEN (SELECT COUNT(*) FROM bitmap_ids bi JOIN expected_ids ei ON bi.bitmap_id = ei.id) = 
             (SELECT COUNT(*) FROM expected_ids)
        THEN 'PASS: Bitmap IDs match original table IDs'
        ELSE 'FAIL: Bitmap IDs do NOT match original table IDs'
    END AS test_result,
    (SELECT COUNT(*) FROM bitmap_ids) AS bitmap_count,
    (SELECT COUNT(*) FROM expected_ids) AS expected_count,
    (SELECT COUNT(*) FROM bitmap_ids bi JOIN expected_ids ei ON bi.bitmap_id = ei.id) AS matching_count;

-- Show the actual IDs
\echo ''
\echo 'Bitmap IDs vs Table IDs for Electronics:'
WITH bitmap_result AS (
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_hash_ids',
        '{"category":"Electronics"}'::jsonb,
        'documents'
    ) AS bm
),
bitmap_ids AS (
    SELECT unnest(rb_to_array(bm)) AS bitmap_id FROM bitmap_result
)
SELECT 
    b.bitmap_id,
    d.title AS found_title
FROM bitmap_ids b
LEFT JOIN test_hash_ids.documents d ON b.bitmap_id = d.id;

-- =============================================================================
-- TEST 2: search_documents_with_facets should return correct results
-- =============================================================================
\echo ''
\echo '--- TEST 2: search_documents_with_facets with facet filter (empty query) ---'

SELECT 
    CASE 
        WHEN jsonb_array_length(results) = 4  -- 4 Electronics items
        THEN 'PASS: search_documents_with_facets returns correct result count'
        ELSE 'FAIL: Expected 4 results, got ' || jsonb_array_length(results)
    END AS test_result,
    total_found,
    jsonb_array_length(results) AS result_count,
    results
FROM facets.search_documents_with_facets(
    p_schema_name := 'test_hash_ids',
    p_table_name := 'documents',
    p_query := '',
    p_facets := '{"category":"Electronics"}'::jsonb,
    p_content_column := 'title',
    p_limit := 10,
    p_offset := 0
);

-- =============================================================================
-- TEST 3: Verify IDs in results match original table
-- =============================================================================
\echo ''
\echo '--- TEST 3: Verify result IDs exist in original table ---'

WITH search_result AS (
    SELECT results FROM facets.search_documents_with_facets(
        p_schema_name := 'test_hash_ids',
        p_table_name := 'documents',
        p_query := '',
        p_facets := '{"category":"Electronics"}'::jsonb,
        p_content_column := 'title',
        p_limit := 10,
        p_offset := 0
    )
),
result_ids AS (
    SELECT (jsonb_array_elements(results)->>'id')::integer AS result_id FROM search_result
),
verified AS (
    SELECT r.result_id, d.title
    FROM result_ids r
    LEFT JOIN test_hash_ids.documents d ON r.result_id = d.id
)
SELECT 
    CASE 
        WHEN COUNT(*) FILTER (WHERE title IS NULL) = 0 
        THEN 'PASS: All result IDs exist in original table'
        ELSE 'FAIL: ' || COUNT(*) FILTER (WHERE title IS NULL) || ' IDs not found in table'
    END AS test_result,
    COUNT(*) AS total_results,
    COUNT(*) FILTER (WHERE title IS NOT NULL) AS verified_ids
FROM verified;

-- =============================================================================
-- TEST 4: Test with text search + facet filter
-- =============================================================================
\echo ''
\echo '--- TEST 4: search_documents_with_facets with text search + facet filter ---'

SELECT 
    CASE 
        WHEN total_found > 0 AND jsonb_array_length(results) > 0
        THEN 'PASS: Text search with facet filter works'
        ELSE 'FAIL: Expected results for "Pro" in Electronics'
    END AS test_result,
    total_found,
    jsonb_array_length(results) AS result_count
FROM facets.search_documents_with_facets(
    p_schema_name := 'test_hash_ids',
    p_table_name := 'documents',
    p_query := 'Pro',
    p_facets := '{"category":"Electronics"}'::jsonb,
    p_content_column := 'title',
    p_limit := 10,
    p_offset := 0
);

-- =============================================================================
-- CLEANUP
-- =============================================================================
\echo ''
\echo 'Cleaning up...'
SELECT facets.drop_faceting('test_hash_ids.documents'::regclass);
DROP SCHEMA test_hash_ids CASCADE;

\echo ''
\echo '=== Hash-based ID Test Complete ==='
