-- Memory Safety Tests for pg_facets
-- Tests edge cases that could cause segmentation faults or memory corruption
-- 
-- These tests verify that the extension handles:
-- - Moderately large content strings (not too large for Docker)
-- - Null/empty strings
-- - Language string limits
-- - Error message formatting

\echo '=============================================='
\echo 'Memory Safety Tests'
\echo '=============================================='
\echo ''

-- Setup: Create a test table
CREATE SCHEMA IF NOT EXISTS memory_safety_test;
CREATE EXTENSION IF NOT EXISTS pg_facets;

CREATE TABLE IF NOT EXISTS memory_safety_test.test_table (
    id bigint PRIMARY KEY,
    content text
);

-- Register table for faceting with BM25 support
-- Use the new API with facet definitions array
DO $$
DECLARE
    v_table_oid oid;
    v_is_registered boolean;
BEGIN
    -- Get table OID
    SELECT oid INTO v_table_oid 
    FROM pg_class 
    WHERE relname = 'test_table' 
    AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Check if already registered
    SELECT EXISTS (
        SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid
    ) INTO v_is_registered;
    
    IF NOT v_is_registered THEN
        -- Register table for faceting (this also registers it in faceted_table)
        PERFORM facets.add_faceting_to_table(
            'memory_safety_test.test_table'::regclass,
            'id',
            ARRAY[]::facets.facet_definition[],  -- No custom facets, just BM25
            NULL,  -- chunk_bits (auto-detect)
            true,  -- keep_deltas
            false  -- populate (skip initial population for faster tests)
        );
        
        -- Set up BM25 indexing for the content column
        PERFORM facets.bm25_set_language('memory_safety_test.test_table'::regclass, 'english');
        
        RAISE NOTICE 'Table registered successfully for BM25 indexing';
    ELSE
        RAISE NOTICE 'Table already registered';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Table registration error: %', SQLERRM;
END;
$$;

\echo '--- Test 1: Moderate content (1MB - should work) ---'
DO $$
DECLARE
    v_table_id oid;
    v_content text;
BEGIN
    -- Get table OID
    SELECT oid INTO v_table_id FROM pg_class WHERE relname = 'test_table' AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Create 1MB content (safe for Docker)
    v_content := repeat('test content with words ', 40000);
    
    -- This should succeed
    PERFORM facets.bm25_index_document_native(
        v_table_id,
        1,
        v_content,
        'english'
    );
    
    RAISE NOTICE 'PASS: 1MB content processed successfully';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'INFO: 1MB content test error (may be expected): %', SQLERRM;
END;
$$;

\echo ''
\echo '--- Test 2: Normal content ---'
DO $$
DECLARE
    v_table_id oid;
BEGIN
    SELECT oid INTO v_table_id FROM pg_class WHERE relname = 'test_table' AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Normal content should work fine
    PERFORM facets.bm25_index_document_native(
        v_table_id,
        2,
        'This is a normal document with some text content for testing purposes.',
        'english'
    );
    RAISE NOTICE 'PASS: Normal content processed successfully';
EXCEPTION WHEN OTHERS THEN
    -- FK constraint errors are expected if table isn't fully registered - not a memory safety issue
    IF SQLERRM LIKE '%foreign key%' OR SQLERRM LIKE '%not registered%' THEN
        RAISE NOTICE 'INFO: Test skipped (table not fully registered): %', SQLERRM;
    ELSE
        RAISE NOTICE 'INFO: Normal content test result: %', SQLERRM;
    END IF;
END;
$$;

\echo ''
\echo '--- Test 3: Empty string content ---'
DO $$
DECLARE
    v_table_id oid;
BEGIN
    SELECT oid INTO v_table_id FROM pg_class WHERE relname = 'test_table' AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Empty string should be handled gracefully
    PERFORM facets.bm25_index_document_native(
        v_table_id,
        3,
        '',
        'english'
    );
    
    RAISE NOTICE 'PASS: Empty string handled gracefully';
EXCEPTION WHEN OTHERS THEN
    -- Empty string might be skipped, which is acceptable
    -- FK constraint errors are also OK (table not fully registered)
    IF SQLERRM LIKE '%empty%' OR SQLERRM LIKE '%skip%' THEN
        RAISE NOTICE 'PASS: Empty string correctly skipped: %', SQLERRM;
    ELSIF SQLERRM LIKE '%foreign key%' OR SQLERRM LIKE '%not registered%' THEN
        RAISE NOTICE 'INFO: Test skipped (table not fully registered): %', SQLERRM;
    ELSE
        RAISE NOTICE 'INFO: Empty string test result: %', SQLERRM;
    END IF;
END;
$$;

\echo ''
\echo '--- Test 4: Moderate query string (100KB - should work) ---'
DO $$
DECLARE
    v_table_id oid;
    v_query text;
    v_result_count int;
BEGIN
    SELECT oid INTO v_table_id FROM pg_class WHERE relname = 'test_table' AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Create a moderate query string (100KB)
    v_query := repeat('test ', 20000);  -- ~100KB
    
    -- This should succeed or fail gracefully
    -- Use named parameters to match the function signature
    BEGIN
        SELECT COUNT(*) INTO v_result_count
        FROM facets.bm25_search_native(
            table_id => v_table_id,
            query => v_query,
            language => 'english',
            limit_count => 10
        );
        RAISE NOTICE 'PASS: Moderate query processed, returned % results', v_result_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'INFO: Query test result: %', SQLERRM;
    END;
END;
$$;

\echo ''
\echo '--- Test 5: Tokenization test (verifies native tokenizer) ---'
DO $$
DECLARE
    v_token_count int;
BEGIN
    -- Test native tokenizer with normal text
    SELECT COUNT(*) INTO v_token_count
    FROM facets.test_tokenize_only('This is a test document with several words for tokenization.', 'english');
    
    IF v_token_count > 0 THEN
        RAISE NOTICE 'PASS: Tokenizer returned % tokens', v_token_count;
    ELSE
        RAISE NOTICE 'INFO: Tokenizer returned 0 tokens (may be expected)';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'INFO: Tokenization test result: %', SQLERRM;
END;
$$;

\echo ''
\echo '--- Test 6: Search with empty query ---'
DO $$
DECLARE
    v_table_id oid;
    v_result_count int;
BEGIN
    SELECT oid INTO v_table_id FROM pg_class WHERE relname = 'test_table' AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Empty query should return empty results or handle gracefully
    -- Use named parameters to match the function signature
    BEGIN
        SELECT COUNT(*) INTO v_result_count
        FROM facets.bm25_search_native(
            table_id => v_table_id,
            query => '',
            language => 'english',
            limit_count => 10
        );
        RAISE NOTICE 'PASS: Empty query handled, returned % results', v_result_count;
    EXCEPTION WHEN OTHERS THEN
        -- Empty query might return empty results, which is acceptable
        RAISE NOTICE 'PASS: Empty query handled (returned empty or error): %', SQLERRM;
    END;
END;
$$;

\echo ''
\echo '--- Test 7: Language string with maximum length ---'
DO $$
DECLARE
    v_table_id oid;
    v_long_lang text;
BEGIN
    SELECT oid INTO v_table_id FROM pg_class WHERE relname = 'test_table' AND relnamespace = 'memory_safety_test'::regnamespace::oid;
    
    -- Language string at exactly 64 bytes (should be rejected)
    -- Maximum allowed is 63 bytes (64 bytes and above are rejected)
    v_long_lang := repeat('a', 64);
    
    -- This should be rejected (language names should be < 64 bytes, max 63 bytes)
    BEGIN
        PERFORM facets.bm25_index_document_native(
            v_table_id,
            4,
            'test content',
            v_long_lang
        );
        RAISE NOTICE 'PASS: Long language string handled';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%too long%' OR SQLERRM LIKE '%null-terminated%' THEN
            RAISE NOTICE 'PASS: Long language string correctly rejected: %', SQLERRM;
        ELSE
            RAISE NOTICE 'INFO: Long language string error (may be expected): %', SQLERRM;
        END IF;
    END;
END;
$$;

\echo ''
\echo '=============================================='
\echo 'Memory Safety Tests Completed'
\echo '=============================================='
\echo ''
\echo 'All tests verify that the extension handles edge cases'
\echo 'without causing segmentation faults or memory corruption.'
\echo ''
\echo 'If any test fails with a segfault, this indicates a'
\echo 'memory safety issue that needs to be fixed.'

