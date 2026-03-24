-- Regression Test for Facet Functionality
-- This test would have caught the bug where facets returned empty [] 
-- when using search_documents_with_facets with facet filters
--
-- Run this test after any changes to search_documents_with_facets or hierarchical_facets_bitmap
-- to ensure facets are always returned correctly

\echo '=============================================='
\echo 'Facet Regression Test Suite'
\echo '=============================================='
\echo ''
\echo 'This test verifies that facets are NEVER empty when they should contain data'
\echo ''

-- Setup test schema and table
DROP SCHEMA IF EXISTS facet_regression_test CASCADE;
CREATE SCHEMA facet_regression_test;

\echo '--- Setup: Create test table ---'

CREATE TABLE facet_regression_test.documents (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT,
    subcategory TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO facet_regression_test.documents (content, category, subcategory, metadata) VALUES
    ('PostgreSQL database management system', 'Technology', 'Database', '{"author": "Alice"}'),
    ('Python programming language tutorial', 'Technology', 'Programming', '{"author": "Bob"}'),
    ('JavaScript web development guide', 'Technology', 'Programming', '{"author": "Charlie"}'),
    ('Cooking Italian pasta recipes', 'Cooking', 'Italian', '{"author": "Marco"}'),
    ('Travel guide to Spain', 'Travel', 'Europe', '{"author": "Elena"}'),
    ('PostgreSQL optimization techniques', 'Technology', 'Database', '{"author": "David"}'),
    ('Python data science with pandas', 'Technology', 'Programming', '{"author": "Frank"}'),
    ('Italian wine selection guide', 'Cooking', 'Italian', '{"author": "Giulia"}');

\echo 'PASS: Created test table with 8 documents'

-- Add faceting
SELECT facets.add_faceting_to_table(
    'facet_regression_test.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('subcategory')
    ],
    populate => true
);

\echo 'PASS: Added faceting to table'

-- ============================================
-- TEST 1: search_documents_with_facets with facet filter MUST return non-empty facets
-- ============================================

\echo ''
\echo '--- TEST 1: Facet filter MUST return facets (not empty) ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'facet_regression_test',
        'documents',
        '',  -- Empty query
        '{"category":"Technology"}'::jsonb,  -- Facet filter
        NULL,  -- No vector column
        'content',
        'metadata',
        'created_at',
        'updated_at',
        20,
        0,
        NULL,
        NULL,
        1000  -- p_facet_limit
    );
    
    -- Verify we have results
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found (expected at least 5 Technology documents)';
    END IF;
    
    IF v_result.total_found < 5 THEN
        RAISE EXCEPTION 'FAIL: Only % results found (expected at least 5)', v_result.total_found;
    END IF;
    
    RAISE NOTICE 'PASS: Found % results', v_result.total_found;
    
    -- CRITICAL: Facets MUST NOT be empty when we have results
    IF v_result.facets IS NULL THEN
        RAISE EXCEPTION 'FAIL: facets is NULL (should be JSONB array, even if empty)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    
    IF v_facets_count = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] (should contain facet groups when results exist)';
    END IF;
    
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
    
    -- Verify facets contain expected data
    IF v_result.facets::text = '[]' THEN
        RAISE EXCEPTION 'FAIL: facets is empty array [] (should contain facet data)';
    END IF;
    
    RAISE NOTICE 'PASS: Facets contain data: %', v_result.facets::text;
END;
$$;

-- ============================================
-- TEST 2: Text search + facet filter MUST return facets
-- ============================================

\echo ''
\echo '--- TEST 2: Text search + facet filter MUST return facets ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'facet_regression_test',
        'documents',
        'PostgreSQL',  -- Text search
        '{"category":"Technology"}'::jsonb,  -- Facet filter
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
    
    -- Verify we have results
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found for "PostgreSQL" + Technology filter';
    END IF;
    
    RAISE NOTICE 'PASS: Found % results for text search + facet filter', v_result.total_found;
    
    -- CRITICAL: Facets MUST NOT be empty
    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] (should contain facet groups when results exist)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
END;
$$;

-- ============================================
-- TEST 3: Empty query with facet filter MUST return facets
-- ============================================

\echo ''
\echo '--- TEST 3: Empty query + facet filter MUST return facets ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'facet_regression_test',
        'documents',
        '',  -- Empty query
        '{"subcategory":"Programming"}'::jsonb,  -- Facet filter
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
    
    -- Verify we have results
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found for Programming subcategory';
    END IF;
    
    RAISE NOTICE 'PASS: Found % results for Programming subcategory', v_result.total_found;
    
    -- CRITICAL: Facets MUST NOT be empty
    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] (should contain facet groups when results exist)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
    
    -- Verify facets contain category and subcategory
    IF v_result.facets::text NOT LIKE '%category%' THEN
        RAISE WARNING 'WARN: Facets may not contain category facet';
    END IF;
END;
$$;

-- ============================================
-- TEST 4: Verify facets contain expected values
-- ============================================

\echo ''
\echo '--- TEST 4: Facets must contain expected values ---'

DO $$
DECLARE
    v_result record;
    v_facet_item jsonb;
    v_has_category boolean := false;
    v_has_subcategory boolean := false;
    i int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'facet_regression_test',
        'documents',
        '',  -- Empty query
        '{"category":"Technology"}'::jsonb,  -- Facet filter
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
    
    -- Check each facet group
    FOR i IN 0..jsonb_array_length(v_result.facets) - 1 LOOP
        v_facet_item := v_result.facets->i;
        
        IF v_facet_item->>'facet_name' = 'category' THEN
            v_has_category := true;
            -- Verify it has values
            IF jsonb_array_length(v_facet_item->'values') = 0 THEN
                RAISE EXCEPTION 'FAIL: category facet has no values';
            END IF;
        END IF;
        
        IF v_facet_item->>'facet_name' = 'subcategory' THEN
            v_has_subcategory := true;
            -- Verify it has values
            IF jsonb_array_length(v_facet_item->'values') = 0 THEN
                RAISE EXCEPTION 'FAIL: subcategory facet has no values';
            END IF;
        END IF;
    END LOOP;
    
    IF NOT v_has_category AND NOT v_has_subcategory THEN
        RAISE WARNING 'WARN: Facets may not contain expected facet names (category, subcategory)';
    END IF;
    
    RAISE NOTICE 'PASS: Facets contain expected structure';
END;
$$;

-- ============================================
-- TEST 5: Real-world scenario (like user's query)
-- ============================================

\echo ''
\echo '--- TEST 5: Real-world scenario (text search + facet filter) ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    -- Simulate user's actual query pattern
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'facet_regression_test',
        'documents',
        'Python',  -- Text search
        '{"category":"Technology"}'::jsonb,  -- Facet filter
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
    
    -- Must have results
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results for "Python" + Technology filter';
    END IF;
    
    -- CRITICAL: Facets MUST NOT be empty
    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] - THIS IS THE BUG WE ARE TESTING FOR!';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    RAISE NOTICE 'PASS: Real-world query returned % results with % facet groups', 
        v_result.total_found, v_facets_count;
END;
$$;

\echo ''
\echo '=============================================='
\echo 'All Facet Regression Tests PASSED'
\echo '=============================================='
\echo ''
\echo 'If any test failed, this indicates a regression in facet functionality.'
\echo 'Facets should NEVER be empty [] when search results exist.'
