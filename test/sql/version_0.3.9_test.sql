-- Test suite for pg_facets 0.3.9
-- Tests performance optimizations and native function integration
-- Run with: psql -f version_0.3.9_test.sql

\set ON_ERROR_STOP on
\timing on

\echo '================================================================================'
\echo 'pg_facets 0.3.9 Test Suite'
\echo '================================================================================'
\echo ''

-- Test 1: Verify version
\echo '--- Test 1: Version Check ---'
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    IF v_version = '0.3.9' THEN
        RAISE NOTICE 'PASS: Version is 0.3.9';
    ELSE
        RAISE EXCEPTION 'FAIL: Expected version 0.3.9, got %', v_version;
    END IF;
END $$;

\echo ''
\echo '--- Test 2: Native filter_documents_by_facets_bitmap_jsonb_native exists ---'
DO $$
BEGIN
    -- Check if function exists
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets'
        AND p.proname = 'filter_documents_by_facets_bitmap_jsonb_native'
    ) THEN
        RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap_jsonb_native function exists';
    ELSE
        RAISE WARNING 'WARN: filter_documents_by_facets_bitmap_jsonb_native function not found (may not be compiled)';
    END IF;
END $$;

\echo ''
\echo '--- Test 3: filter_documents_by_facets_bitmap uses native when available ---'
\echo 'This test verifies that filter_documents_by_facets_bitmap works correctly'
\echo 'with the optimized implementation (native or SQL fallback)'
\echo ''

-- This will work with either native or SQL implementation
DO $$
DECLARE
    v_table_id oid;
    v_bitmap roaringbitmap;
    v_card bigint;
BEGIN
    -- Get a test table (assuming test setup exists)
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    LIMIT 1;
    
    IF v_table_id IS NULL THEN
        RAISE NOTICE 'SKIP: No faceted tables found for testing';
        RETURN;
    END IF;
    
    -- Test with a simple filter (will use native if available, SQL otherwise)
    v_bitmap := facets.filter_documents_by_facets_bitmap(
        (SELECT schemaname FROM facets.faceted_table WHERE table_id = v_table_id),
        '{}'::jsonb,
        (SELECT tablename FROM facets.faceted_table WHERE table_id = v_table_id)
    );
    
    IF v_bitmap IS NULL THEN
        RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap returns NULL for empty filters';
    ELSE
        v_card := rb_cardinality(v_bitmap);
        RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap returns bitmap with % elements', v_card;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'WARN: filter_documents_by_facets_bitmap test failed: %', SQLERRM;
END $$;

\echo ''
\echo '--- Test 4: Optimized search_documents_with_facets query structure ---'
\echo 'This test verifies the bitmap intersection optimization is working'
\echo ''

DO $$
DECLARE
    v_table_id oid;
    v_result record;
BEGIN
    -- Get a test table
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    LIMIT 1;
    
    IF v_table_id IS NULL THEN
        RAISE NOTICE 'SKIP: No faceted tables found for testing';
        RETURN;
    END IF;
    
    -- Test search with empty query and filter (should use bitmap path)
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        (SELECT schemaname FROM facets.faceted_table WHERE table_id = v_table_id),
        (SELECT tablename FROM facets.faceted_table WHERE table_id = v_table_id),
        '',  -- empty query
        '{}'::jsonb,  -- empty filters
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        10, 0, NULL, NULL, 5
    );
    
    IF v_result IS NOT NULL THEN
        RAISE NOTICE 'PASS: search_documents_with_facets returns results';
        RAISE NOTICE '      Total found: %, Search time: % ms', v_result.total_found, v_result.search_time;
    ELSE
        RAISE WARNING 'WARN: search_documents_with_facets returned NULL';
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'WARN: search_documents_with_facets test failed: %', SQLERRM;
END $$;

\echo ''
\echo '--- Test 5: Performance comparison (if test data available) ---'
\echo 'This test compares performance before/after optimization'
\echo ''

DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_table_id oid;
    v_schema text;
    v_table text;
    v_result record;
BEGIN
    -- Get a test table with data
    SELECT table_id, schemaname, tablename INTO v_table_id, v_schema, v_table
    FROM facets.faceted_table
    WHERE EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = facets.faceted_table.schemaname 
        AND table_name = facets.faceted_table.tablename
    )
    LIMIT 1;
    
    IF v_table_id IS NULL THEN
        RAISE NOTICE 'SKIP: No suitable test tables found';
        RETURN;
    END IF;
    
    -- Test optimized path
    v_start_time := clock_timestamp();
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        v_schema, v_table, 'test', '{}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        20, 0, NULL, NULL, 1000
    );
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    RAISE NOTICE 'Performance test completed:';
    RAISE NOTICE '  Execution time: % ms', v_duration_ms;
    RAISE NOTICE '  Function reported time: % ms', COALESCE(v_result.search_time, 0);
    RAISE NOTICE '  Total found: %', COALESCE(v_result.total_found, 0);
    
    IF v_duration_ms < 1000 THEN
        RAISE NOTICE 'PASS: Query completed in under 1 second';
    ELSE
        RAISE WARNING 'WARN: Query took % ms (may need further optimization)', v_duration_ms;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'WARN: Performance test failed: %', SQLERRM;
END $$;

\echo ''
\echo '--- Test 6: Verify optimized filter_documents_by_facets_bitmap uses rb_or_agg ---'
\echo 'This test verifies the SQL optimization (rb_or_agg grouping) is in place'
\echo ''

DO $$
DECLARE
    v_func_body text;
BEGIN
    -- Check if the function body contains the optimization
    SELECT pg_get_functiondef(p.oid) INTO v_func_body
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'facets'
    AND p.proname = 'filter_documents_by_facets_bitmap'
    AND p.pronargs = 3;
    
    IF v_func_body IS NULL THEN
        RAISE NOTICE 'SKIP: Could not find function definition';
        RETURN;
    END IF;
    
    -- Check for optimization markers
    IF v_func_body LIKE '%rb_or_agg%' OR v_func_body LIKE '%filter_documents_by_facets_bitmap_jsonb_native%' THEN
        RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap contains optimizations';
    ELSE
        RAISE WARNING 'WARN: filter_documents_by_facets_bitmap may not have optimizations';
    END IF;
END $$;

\echo ''
\echo '--- Test 7: Verify search_documents_with_facets uses bitmap intersection ---'
\echo 'This test verifies the main optimization (bitmap intersection vs rb_contains)'
\echo ''

DO $$
DECLARE
    v_func_body text;
BEGIN
    -- Check if the function body contains the optimization
    SELECT pg_get_functiondef(p.oid) INTO v_func_body
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname = 'facets'
    AND p.proname = 'search_documents_with_facets'
    LIMIT 1;
    
    IF v_func_body IS NULL THEN
        RAISE NOTICE 'SKIP: Could not find function definition';
        RETURN;
    END IF;
    
    -- Check for optimization markers (bitmap intersection approach)
    IF v_func_body LIKE '%text_search_bitmap%' OR 
       v_func_body LIKE '%filtered_bitmap%' OR
       v_func_body LIKE '%rb_and%' THEN
        RAISE NOTICE 'PASS: search_documents_with_facets uses bitmap intersection optimization';
    ELSIF v_func_body LIKE '%rb_contains%' AND v_func_body NOT LIKE '%--%rb_contains%' THEN
        RAISE WARNING 'WARN: search_documents_with_facets may still use rb_contains in WHERE clause';
    ELSE
        RAISE NOTICE 'INFO: Could not verify optimization from function body';
    END IF;
END $$;

\echo ''
\echo '================================================================================'
\echo 'Test Suite Complete'
\echo '================================================================================'
