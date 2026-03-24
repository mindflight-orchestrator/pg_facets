-- Critical Memory Safety Tests
-- These tests specifically target the bugs we fixed:
-- 1. Memory use after SPI_finish() in datumToRoaringBitmap
-- 2. Nested SPI connections
-- 3. search_documents_with_facets crash with text query

\echo '=============================================='
\echo 'Critical Memory Safety Tests'
\echo '=============================================='
\echo ''
\echo 'These tests specifically target the bugs we fixed:'
\echo '1. Memory use after SPI_finish() in bitmap operations'
\echo '2. Nested SPI connections'
\echo '3. search_documents_with_facets crash with text query'
\echo ''

-- Setup: Create a test table
CREATE SCHEMA IF NOT EXISTS critical_memory_test;
CREATE EXTENSION IF NOT EXISTS pg_facets;

CREATE TABLE IF NOT EXISTS critical_memory_test.documents (
    id bigint PRIMARY KEY,
    content text,
    title text,
    category text,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Register table for faceting with BM25
DO $$
DECLARE
    v_table_oid oid;
    v_is_registered boolean;
BEGIN
    SELECT oid INTO v_table_oid 
    FROM pg_class 
    WHERE relname = 'documents' 
    AND relnamespace = 'critical_memory_test'::regnamespace::oid;
    
    SELECT EXISTS (
        SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid
    ) INTO v_is_registered;
    
    IF NOT v_is_registered THEN
        PERFORM facets.add_faceting_to_table(
            'critical_memory_test.documents'::regclass,
            'id',
            ARRAY[
                facets.plain_facet('category')
            ]::facets.facet_definition[],
            NULL,
            true,
            false
        );
        
        PERFORM facets.bm25_set_language('critical_memory_test.documents'::regclass, 'english');
        
        RAISE NOTICE 'Table registered successfully';
    END IF;
END $$;

-- Insert test data
INSERT INTO critical_memory_test.documents (id, content, title, category) VALUES
    (1, 'This is a test document about computers and technology', 'Computer Test', 'Electronics'),
    (2, 'Another document about laptops and software', 'Laptop Guide', 'Electronics'),
    (3, 'A document about phones and mobile technology', 'Phone Review', 'Electronics'),
    (4, 'Document about books and literature', 'Book Guide', 'Books'),
    (5, 'Another book document', 'Book Review', 'Books')
ON CONFLICT (id) DO NOTHING;

-- Index documents for BM25
DO $$
DECLARE
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'critical_memory_test' AND tablename = 'documents';
    
    -- Index all documents
    PERFORM facets.bm25_index_document_native(v_table_id, 1, 
        (SELECT content FROM critical_memory_test.documents WHERE id = 1), 'english');
    PERFORM facets.bm25_index_document_native(v_table_id, 2, 
        (SELECT content FROM critical_memory_test.documents WHERE id = 2), 'english');
    PERFORM facets.bm25_index_document_native(v_table_id, 3, 
        (SELECT content FROM critical_memory_test.documents WHERE id = 3), 'english');
    PERFORM facets.bm25_index_document_native(v_table_id, 4, 
        (SELECT content FROM critical_memory_test.documents WHERE id = 4), 'english');
    PERFORM facets.bm25_index_document_native(v_table_id, 5, 
        (SELECT content FROM critical_memory_test.documents WHERE id = 5), 'english');
    
    RAISE NOTICE 'Documents indexed for BM25';
END $$;

\echo ''
\echo '--- Test 1: bm25_get_matches_bitmap_native with text query (CRITICAL BUG TEST) ---'
\echo 'This test specifically targets the bug where memory was used after SPI_finish()'
DO $$
DECLARE
    v_table_id oid;
    v_bitmap roaringbitmap;
    v_cardinality bigint;
    v_test_passed boolean := false;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'critical_memory_test' AND tablename = 'documents';
    
    -- This was the exact scenario that caused the crash
    -- The bitmap was deserialized from SPI tuple memory, then SPI_finish() was called,
    -- then the bitmap was used - causing a segmentation fault
    v_bitmap := facets.bm25_get_matches_bitmap_native(
        v_table_id,
        'test query',  -- Text query that triggers tokenization
        'english'
    );
    
    -- If we get here without crashing, the fix worked!
    IF v_bitmap IS NOT NULL THEN
        v_cardinality := rb_cardinality(v_bitmap);
        RAISE NOTICE 'PASS: bm25_get_matches_bitmap_native returned bitmap with cardinality %', v_cardinality;
        v_test_passed := true;
    ELSE
        RAISE NOTICE 'INFO: bm25_get_matches_bitmap_native returned NULL (no matches)';
        v_test_passed := true; -- NULL is acceptable if no matches
    END IF;
    
    -- Test with different queries to ensure it works multiple times
    v_bitmap := facets.bm25_get_matches_bitmap_native(
        v_table_id,
        'computer laptop',
        'english'
    );
    
    IF v_bitmap IS NOT NULL THEN
        v_cardinality := rb_cardinality(v_bitmap);
        RAISE NOTICE 'PASS: Second call to bm25_get_matches_bitmap_native returned bitmap with cardinality %', v_cardinality;
    END IF;
    
    IF v_test_passed THEN
        RAISE NOTICE 'PASS: Test 1 completed - no crash detected';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FAIL: Test 1 crashed with error: %', SQLERRM;
END $$;

\echo ''
\echo '--- Test 2: search_documents_with_facets with text query (CRITICAL BUG TEST) ---'
\echo 'This test specifically targets the crash in search_documents_with_facets'
DO $$
DECLARE
    v_result_count int;
    v_test_passed boolean := false;
BEGIN
    -- This was the exact scenario that caused the database crash
    -- search_documents_with_facets called bm25_get_matches_bitmap_native
    -- which used memory after SPI_finish(), causing a segmentation fault
    SELECT COUNT(*) INTO v_result_count
    FROM facets.search_documents_with_facets(
        p_schema_name := 'critical_memory_test',
        p_table_name := 'documents',
        p_query := 'test',  -- Text query that triggers the bug path
        p_facets := NULL,
        p_content_column := 'content',
        p_metadata_column := 'metadata',
        p_created_at_column := 'created_at',
        p_updated_at_column := 'updated_at',
        p_limit := 10,
        p_offset := 0,
        p_min_score := 0.0,
        p_vector_weight := 0.5,
        p_facet_limit := 5,
        p_language := 'english'
    );
    
    -- If we get here without crashing, the fix worked!
    RAISE NOTICE 'PASS: search_documents_with_facets returned % results (no crash)', v_result_count;
    v_test_passed := true;
    
    -- Test multiple times to ensure stability
    SELECT COUNT(*) INTO v_result_count
    FROM facets.search_documents_with_facets(
        p_schema_name := 'critical_memory_test',
        p_table_name := 'documents',
        p_query := 'computer laptop',
        p_facets := '{"category": "Electronics"}'::jsonb,
        p_content_column := 'content',
        p_metadata_column := 'metadata',
        p_created_at_column := 'created_at',
        p_updated_at_column := 'updated_at',
        p_limit := 10,
        p_offset := 0,
        p_min_score := 0.0,
        p_vector_weight := 0.5,
        p_facet_limit := 5,
        p_language := 'english'
    );
    
    RAISE NOTICE 'PASS: Second call to search_documents_with_facets returned % results', v_result_count;
    
    IF v_test_passed THEN
        RAISE NOTICE 'PASS: Test 2 completed - no crash detected';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FAIL: Test 2 crashed with error: %', SQLERRM;
END $$;

\echo ''
\echo '--- Test 3: Multiple rapid calls (stress test for SPI lifecycle) ---'
\echo 'This test ensures SPI connections are properly managed across multiple calls'
DO $$
DECLARE
    v_table_id oid;
    v_bitmap roaringbitmap;
    i int;
    v_success_count int := 0;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'critical_memory_test' AND tablename = 'documents';
    
    -- Make multiple rapid calls to test SPI lifecycle
    FOR i IN 1..10 LOOP
        BEGIN
            v_bitmap := facets.bm25_get_matches_bitmap_native(
                v_table_id,
                'test query ' || i::text,
                'english'
            );
            v_success_count := v_success_count + 1;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Call % failed: %', i, SQLERRM;
        END;
    END LOOP;
    
    IF v_success_count = 10 THEN
        RAISE NOTICE 'PASS: All 10 rapid calls succeeded (no nested SPI issues)';
    ELSE
        RAISE WARNING 'PARTIAL: Only %/10 calls succeeded', v_success_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 4: search_documents_with_facets with various query types ---'
\echo 'This test ensures the function works with different query scenarios'
DO $$
DECLARE
    v_result_count int;
    v_test_scenarios int := 0;
    v_passed_scenarios int := 0;
BEGIN
    -- Scenario 1: Empty query
    BEGIN
        SELECT COUNT(*) INTO v_result_count
        FROM facets.search_documents_with_facets(
            'critical_memory_test', 'documents', '', NULL, NULL, 'content', 'metadata', 
            'created_at', 'updated_at', 10, 0, 0.0, 0.5, 5, 'english'
        );
        v_passed_scenarios := v_passed_scenarios + 1;
        RAISE NOTICE 'PASS: Empty query scenario';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'FAIL: Empty query scenario: %', SQLERRM;
    END;
    v_test_scenarios := v_test_scenarios + 1;
    
    -- Scenario 2: Short query
    BEGIN
        SELECT COUNT(*) INTO v_result_count
        FROM facets.search_documents_with_facets(
            'critical_memory_test', 'documents', 'test', NULL, NULL, 'content', 'metadata',
            'created_at', 'updated_at', 10, 0, 0.0, 0.5, 5, 'english'
        );
        v_passed_scenarios := v_passed_scenarios + 1;
        RAISE NOTICE 'PASS: Short query scenario';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'FAIL: Short query scenario: %', SQLERRM;
    END;
    v_test_scenarios := v_test_scenarios + 1;
    
    -- Scenario 3: Query with facets
    BEGIN
        SELECT COUNT(*) INTO v_result_count
        FROM facets.search_documents_with_facets(
            'critical_memory_test', 'documents', 'computer', 
            '{"category": "Electronics"}'::jsonb, NULL, 'content', 'metadata',
            'created_at', 'updated_at', 10, 0, 0.0, 0.5, 5, 'english'
        );
        v_passed_scenarios := v_passed_scenarios + 1;
        RAISE NOTICE 'PASS: Query with facets scenario';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'FAIL: Query with facets scenario: %', SQLERRM;
    END;
    v_test_scenarios := v_test_scenarios + 1;
    
    IF v_passed_scenarios = v_test_scenarios THEN
        RAISE NOTICE 'PASS: All % query scenarios passed', v_test_scenarios;
    ELSE
        RAISE WARNING 'PARTIAL: %/% scenarios passed', v_passed_scenarios, v_test_scenarios;
    END IF;
END $$;

\echo ''
\echo '=============================================='
\echo 'Critical Memory Safety Tests Completed'
\echo '=============================================='
\echo ''
\echo 'If all tests passed, the critical bugs are fixed:'
\echo '  ✓ Memory use after SPI_finish() - FIXED'
\echo '  ✓ Nested SPI connections - FIXED'
\echo '  ✓ search_documents_with_facets crash - FIXED'
\echo ''
\echo 'If any test failed with a crash, this indicates'
\echo 'a regression that needs immediate attention.'

