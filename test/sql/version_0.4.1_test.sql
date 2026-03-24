-- Test suite for pg_facets 0.4.1
-- Tests BM25 optimization and new native functions

\set ON_ERROR_STOP on
\timing on

\echo '================================================================================'
\echo 'pg_facets 0.4.1 Test Suite'
\echo '================================================================================'
\echo ''

-- Test 1: Verify version
\echo '--- Test 1: Version Check ---'
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    -- Note: _get_version() might need to be implemented or we check pg_extension
    SELECT extversion INTO v_version FROM pg_extension WHERE extname = 'pg_facets';
    
    IF v_version = '0.4.1' THEN
        RAISE NOTICE 'PASS: Version is 0.4.1';
    ELSE
        RAISE NOTICE 'FAIL: Expected version 0.4.1, got %', v_version;
    END IF;
END $$;

\echo ''
\echo '--- Test 2: Native bm25_get_matches_bitmap_native exists ---'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets'
        AND p.proname = 'bm25_get_matches_bitmap_native'
    ) THEN
        RAISE NOTICE 'PASS: bm25_get_matches_bitmap_native function exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_get_matches_bitmap_native function not found';
    END IF;
END $$;

\echo ''
\echo '--- Test 3: BM25 helper functions exist ---'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets'
        AND p.proname = 'bm25_is_table_registered'
    ) THEN
        RAISE NOTICE 'PASS: bm25_is_table_registered function exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_is_table_registered function not found';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets'
        AND p.proname = 'bm25_get_index_stats'
    ) THEN
        RAISE NOTICE 'PASS: bm25_get_index_stats function exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_get_index_stats function not found';
    END IF;
END $$;

\echo ''
\echo '--- Test 4: bm25_search uses native function ---'
DO $$
DECLARE
    v_func_body text;
BEGIN
    SELECT pg_get_functiondef(p.oid) INTO v_func_body
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'facets'
    AND p.proname = 'bm25_search'
    LIMIT 1;

    IF v_func_body LIKE '%bm25_search_native%' THEN
        RAISE NOTICE 'PASS: bm25_search uses native implementation';
    ELSE
        RAISE WARNING 'FAIL: bm25_search does not seem to call native implementation';
    END IF;
END $$;

\echo ''
\echo '--- Test 4: Verify search_documents_with_facets uses bm25_get_matches_bitmap_native ---'
DO $$
DECLARE
    v_func_body text;
BEGIN
    SELECT pg_get_functiondef(p.oid) INTO v_func_body
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'facets'
    AND p.proname = 'search_documents_with_facets'
    LIMIT 1;

    IF v_func_body LIKE '%bm25_get_matches_bitmap_native%' THEN
        RAISE NOTICE 'PASS: search_documents_with_facets calls bm25_get_matches_bitmap_native';
    ELSE
        RAISE WARNING 'FAIL: search_documents_with_facets does not seem to call bm25_get_matches_bitmap_native';
    END IF;
END $$;

\echo ''
\echo '--- Test 5: Verify bm25_score uses native function ---'
DO $$
DECLARE
    v_func_body text;
BEGIN
    SELECT pg_get_functiondef(p.oid) INTO v_func_body
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'facets'
    AND p.proname = 'bm25_score'
    LIMIT 1;

    IF v_func_body LIKE '%bm25_score_native%' THEN
        RAISE NOTICE 'PASS: bm25_score uses native implementation';
    ELSE
        RAISE WARNING 'FAIL: bm25_score does not seem to call native implementation';
    END IF;
END $$;

\echo ''
\echo '================================================================================'
\echo 'Test Suite Complete'
\echo '================================================================================'
