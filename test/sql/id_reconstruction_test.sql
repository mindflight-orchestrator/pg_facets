-- =============================================================================
-- TEST: ID Reconstruction Verification
-- This test specifically verifies that filter_documents_by_facets_bitmap
-- returns correctly reconstructed IDs (not raw in_chunk_id values)
-- 
-- This test will FAIL if the function returns raw in_chunk_id values
-- instead of reconstructed (chunk_id << chunk_bits) | in_chunk_id
-- =============================================================================

\echo '=== TEST: ID Reconstruction Verification ==='

-- Cleanup
DROP SCHEMA IF EXISTS test_id_reconstruction CASCADE;
CREATE SCHEMA test_id_reconstruction;

-- Create test table with HASH-based IDs (simulating production scenario)
-- Using ABS(HASHTEXT(...))::INTEGER to generate large, non-sequential IDs
CREATE TABLE test_id_reconstruction.documents (
    id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert test data with hash-based IDs (similar to production)
INSERT INTO test_id_reconstruction.documents (id, category, title) VALUES
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

-- Show ID range
\echo ''
\echo 'Table ID range:'
SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as total_count 
FROM test_id_reconstruction.documents;

-- Add faceting with chunk_bits=20 (like production)
\echo ''
\echo 'Adding faceting with chunk_bits=20...'
SELECT facets.add_faceting_to_table(
    'test_id_reconstruction.documents'::regclass,
    'id',
    ARRAY[
        ROW(NULL, NULL, 'category', 'plain', 'category', NULL, false, true)::facets.facet_definition
    ]::facets.facet_definition[],
    20,  -- chunk_bits=20 like production
    true,
    true
);

-- Verify chunk_bits
\echo ''
\echo 'Verifying chunk_bits configuration:'
SELECT chunk_bits FROM facets.faceted_table 
WHERE schemaname = 'test_id_reconstruction' AND tablename = 'documents';

-- =============================================================================
-- TEST 1: Verify bitmap IDs are in valid range
-- =============================================================================
\echo ''
\echo '--- TEST 1: Bitmap IDs must be within table ID range ---'

WITH filter_bitmap AS (
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_id_reconstruction',
        '{"category":"Electronics"}'::jsonb,
        'documents'
    ) AS bm
),
bitmap_ids AS (
    SELECT unnest(rb_to_array(bm)) AS doc_id
    FROM filter_bitmap
),
table_range AS (
    SELECT MIN(id) AS min_id, MAX(id) AS max_id
    FROM test_id_reconstruction.documents
)
SELECT 
    CASE 
        WHEN COUNT(*) FILTER (WHERE b.doc_id < tr.min_id OR b.doc_id > tr.max_id) = 0
        THEN 'PASS: All bitmap IDs are within table ID range'
        ELSE 'FAIL: ' || COUNT(*) FILTER (WHERE b.doc_id < tr.min_id OR b.doc_id > tr.max_id) || 
             ' bitmap IDs are OUT OF RANGE (expected between ' || tr.min_id || ' and ' || tr.max_id || ')'
    END AS test_result,
    COUNT(*) AS total_bitmap_ids,
    COUNT(*) FILTER (WHERE b.doc_id < tr.min_id OR b.doc_id > tr.max_id) AS out_of_range_count,
    MIN(b.doc_id) AS min_bitmap_id,
    MAX(b.doc_id) AS max_bitmap_id,
    tr.min_id AS table_min_id,
    tr.max_id AS table_max_id
FROM bitmap_ids b, table_range tr
GROUP BY tr.min_id, tr.max_id;

-- =============================================================================
-- TEST 2: Verify all bitmap IDs exist in the table
-- =============================================================================
\echo ''
\echo '--- TEST 2: All bitmap IDs must exist in the table ---'

WITH filter_bitmap AS (
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_id_reconstruction',
        '{"category":"Electronics"}'::jsonb,
        'documents'
    ) AS bm
),
bitmap_ids AS (
    SELECT unnest(rb_to_array(bm)) AS doc_id
    FROM filter_bitmap
),
existence_check AS (
    SELECT 
        b.doc_id,
        CASE WHEN d.id IS NOT NULL THEN 1 ELSE 0 END AS exists_in_table
    FROM bitmap_ids b
    LEFT JOIN test_id_reconstruction.documents d ON d.id = b.doc_id
)
SELECT 
    CASE 
        WHEN COUNT(*) FILTER (WHERE exists_in_table = 0) = 0
        THEN 'PASS: All bitmap IDs exist in the table'
        ELSE 'FAIL: ' || COUNT(*) FILTER (WHERE exists_in_table = 0) || 
             ' bitmap IDs do NOT exist in the table'
    END AS test_result,
    COUNT(*) AS total_bitmap_ids,
    COUNT(*) FILTER (WHERE exists_in_table = 1) AS existing_ids,
    COUNT(*) FILTER (WHERE exists_in_table = 0) AS missing_ids
FROM existence_check;

-- Show sample missing IDs if any
\echo ''
\echo 'Sample bitmap IDs and their existence status:'
WITH filter_bitmap AS (
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_id_reconstruction',
        '{"category":"Electronics"}'::jsonb,
        'documents'
    ) AS bm
),
bitmap_ids AS (
    SELECT unnest(rb_to_array(bm)) AS doc_id
    FROM filter_bitmap
    LIMIT 10
)
SELECT 
    b.doc_id,
    d.id AS table_id,
    d.title,
    CASE WHEN d.id IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS status
FROM bitmap_ids b
LEFT JOIN test_id_reconstruction.documents d ON d.id = b.doc_id
ORDER BY b.doc_id;

-- =============================================================================
-- TEST 3: Verify bitmap cardinality matches expected count
-- =============================================================================
\echo ''
\echo '--- TEST 3: Bitmap cardinality should match expected count ---'

WITH filter_bitmap AS (
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_id_reconstruction',
        '{"category":"Electronics"}'::jsonb,
        'documents'
    ) AS bm
),
expected_count AS (
    SELECT COUNT(*) AS cnt
    FROM test_id_reconstruction.documents
    WHERE category = 'Electronics'
)
SELECT 
    CASE 
        WHEN rb_cardinality(fb.bm) = ec.cnt
        THEN 'PASS: Bitmap cardinality matches expected count'
        ELSE 'FAIL: Bitmap cardinality (' || rb_cardinality(fb.bm) || 
             ') does not match expected count (' || ec.cnt || ')'
    END AS test_result,
    rb_cardinality(fb.bm) AS bitmap_cardinality,
    ec.cnt AS expected_count,
    rb_cardinality(fb.bm) - ec.cnt AS difference
FROM filter_bitmap fb, expected_count ec;

-- =============================================================================
-- TEST 4: Verify search_documents_with_facets returns results
-- =============================================================================
\echo ''
\echo '--- TEST 4: search_documents_with_facets should return results ---'

WITH search_result AS (
    SELECT * FROM facets.search_documents_with_facets(
        p_schema_name := 'test_id_reconstruction',
        p_table_name := 'documents',
        p_query := '',
        p_facets := '{"category":"Electronics"}'::jsonb,
        p_content_column := 'title',
        p_metadata_column := 'metadata',
        p_created_at_column := 'created_at',
        p_updated_at_column := 'updated_at',
        p_limit := 10,
        p_offset := 0
    )
)
SELECT 
    CASE 
        WHEN jsonb_array_length(results) > 0 AND total_found > 0
        THEN 'PASS: search_documents_with_facets returns results'
        ELSE 'FAIL: search_documents_with_facets returned ' || 
             COALESCE(jsonb_array_length(results)::text, 'NULL') || 
             ' results (expected > 0)'
    END AS test_result,
    total_found,
    jsonb_array_length(results) AS result_count
FROM search_result;

-- =============================================================================
-- TEST 5: Verify result IDs from search_documents_with_facets exist
-- =============================================================================
\echo ''
\echo '--- TEST 5: Result IDs from search_documents_with_facets must exist ---'

WITH search_result AS (
    SELECT results FROM facets.search_documents_with_facets(
        p_schema_name := 'test_id_reconstruction',
        p_table_name := 'documents',
        p_query := '',
        p_facets := '{"category":"Electronics"}'::jsonb,
        p_content_column := 'title',
        p_metadata_column := 'metadata',
        p_created_at_column := 'created_at',
        p_updated_at_column := 'updated_at',
        p_limit := 10,
        p_offset := 0
    )
),
result_ids AS (
    SELECT (jsonb_array_elements(results)->>'id')::integer AS result_id 
    FROM search_result
),
existence_check AS (
    SELECT 
        r.result_id,
        CASE WHEN d.id IS NOT NULL THEN 1 ELSE 0 END AS exists_in_table
    FROM result_ids r
    LEFT JOIN test_id_reconstruction.documents d ON d.id = r.result_id
)
SELECT 
    CASE 
        WHEN COUNT(*) FILTER (WHERE exists_in_table = 0) = 0
        THEN 'PASS: All result IDs exist in the table'
        ELSE 'FAIL: ' || COUNT(*) FILTER (WHERE exists_in_table = 0) || 
             ' result IDs do NOT exist in the table'
    END AS test_result,
    COUNT(*) AS total_result_ids,
    COUNT(*) FILTER (WHERE exists_in_table = 1) AS existing_ids,
    COUNT(*) FILTER (WHERE exists_in_table = 0) AS missing_ids
FROM existence_check;

-- =============================================================================
-- CLEANUP
-- =============================================================================
\echo ''
\echo 'Cleaning up...'
SELECT facets.drop_faceting('test_id_reconstruction.documents'::regclass);
DROP SCHEMA test_id_reconstruction CASCADE;

\echo ''
\echo '=== ID Reconstruction Test Complete ==='
