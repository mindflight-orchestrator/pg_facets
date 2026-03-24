-- ============================================================================
-- Test: Facets with Actors/Actresses Dataset
-- ============================================================================
-- This test creates a small dataset of 10 actors/actresses to verify that
-- facets are returned correctly when searching with queries and facet filters.
-- This specifically tests the bug fix where facets were empty when searching
-- with a query + facet filter combination.
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo 'TEST: Facets with Actors/Actresses Dataset (10 records)'
\echo '============================================================================'
\echo ''

-- ============================================================================
-- Setup: Create test schema and table
-- ============================================================================

DROP SCHEMA IF EXISTS test_actors CASCADE;
CREATE SCHEMA test_actors;

-- Create source table (similar to name_basics)
CREATE TABLE test_actors.actors_source (
    nconst TEXT PRIMARY KEY,
    primaryName TEXT NOT NULL,
    birthYear INTEGER,
    deathYear INTEGER,
    primaryProfession TEXT,
    knownForTitles TEXT
);

-- Insert 10 test actors/actresses with varied data
INSERT INTO test_actors.actors_source (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles) VALUES
    ('nm0000001', 'Ingrid Bergman', 1915, 1982, 'actress,producer,soundtrack', 'tt0034583,tt0038109,tt0036855'),
    ('nm0000002', 'Meryl Streep', 1949, NULL, 'actress,producer', 'tt0071562,tt0084434,tt0100157'),
    ('nm0000003', 'Tom Hanks', 1956, NULL, 'actor,producer,director', 'tt0109830,tt0112384,tt0120338'),
    ('nm0000004', 'Vera Bergman', 1920, 1971, 'actress,soundtrack', 'tt0030103,tt0033412'),
    ('nm0000005', 'Sandahl Bergman', 1951, NULL, 'actress,miscellaneous,soundtrack', 'tt0082198,tt0089893'),
    ('nm0000006', 'Robert De Niro', 1943, NULL, 'actor,producer', 'tt0071562,tt0073195,tt0099685'),
    ('nm0000007', 'Cate Blanchett', 1969, NULL, 'actress,producer', 'tt0120338,tt0167260,tt0372784'),
    ('nm0000008', 'Leonardo DiCaprio', 1974, NULL, 'actor,producer', 'tt0111161,tt0120338,tt1130884'),
    ('nm0000009', 'Emma Stone', 1988, NULL, 'actress,producer', 'tt1282140,tt2582782,tt4550098'),
    ('nm0000010', 'Denzel Washington', 1954, NULL, 'actor,producer,director', 'tt0105695,tt0115956,tt0167260');

\echo 'PASS: Created test_actors.actors_source with 10 actors/actresses'
\echo ''

-- ============================================================================
-- Create faceting table (similar to name_basics_faceting_mv)
-- ============================================================================

DROP TABLE IF EXISTS test_actors.actors_faceting_mv CASCADE;

CREATE TABLE test_actors.actors_faceting_mv AS
SELECT 
    -- Use stable hash-based ID from nconst
    ABS(HASHTEXT(a.nconst))::INTEGER AS id,
    
    -- document_id column alias
    ABS(HASHTEXT(a.nconst))::INTEGER AS document_id,
    
    -- Content for full-text search (primaryName and primaryProfession)
    COALESCE(
        a.primaryName || ' ' || 
        COALESCE(a.primaryProfession, ''),
        ''
    ) AS content,
    
    -- Metadata as JSONB
    jsonb_build_object(
        'nconst', a.nconst,
        'primaryName', a.primaryName,
        'birthYear', a.birthYear,
        'deathYear', a.deathYear,
        'primaryProfession', a.primaryProfession,
        'knownForTitles', a.knownForTitles
    ) AS metadata,
    
    -- Timestamps
    COALESCE(
        make_timestamp(COALESCE(a.birthYear, 1900), 1, 1, 0, 0, 0),
        CURRENT_TIMESTAMP
    ) AS created_at,
    
    CURRENT_TIMESTAMP AS updated_at,
    
    -- Reference to original table
    a.nconst AS nconst,
    
    -- Facet columns
    a.primaryName AS primary_name,
    a.birthYear AS birth_year,
    a.deathYear AS death_year,
    -- Store first profession for simple faceting
    CASE 
        WHEN a.primaryProfession IS NOT NULL AND a.primaryProfession != '' THEN 
            TRIM(SPLIT_PART(a.primaryProfession, ',', 1))
        ELSE NULL
    END AS primary_profession
    
FROM test_actors.actors_source a
WHERE a.nconst IS NOT NULL;

\echo 'PASS: Created test_actors.actors_faceting_mv table'
\echo ''

-- ============================================================================
-- Add faceting to the table
-- ============================================================================

SELECT facets.add_faceting_to_table(
    'test_actors.actors_faceting_mv',
    'id',
    ARRAY[
        -- Plain facets (simple column values)
        facets.plain_facet('primary_profession'),
        
        -- Bucket facet for birth year (group into decades)
        facets.bucket_facet('birth_year', ARRAY[
            1900::float8, 1920::float8, 1940::float8, 1960::float8, 1980::float8, 2000::float8
        ]),
        
        -- Bucket facet for death year
        facets.bucket_facet('death_year', ARRAY[
            1900::float8, 1920::float8, 1940::float8, 1960::float8, 1980::float8, 2000::float8
        ])
    ]::facets.facet_definition[],
    20,   -- chunk_bits
    TRUE, -- keep_deltas
    TRUE  -- populate (initial population)
);

\echo 'PASS: Added faceting to table'
\echo ''

-- ============================================================================
-- Create GIN index on content column for full-text search
-- ============================================================================

CREATE INDEX IF NOT EXISTS actors_faceting_mv_content_gin_idx 
ON test_actors.actors_faceting_mv 
USING GIN (to_tsvector('english', content));

\echo 'PASS: Created GIN index on content column'
\echo ''

-- ============================================================================
-- TEST 1: Empty query with facet filter - MUST return facets
-- ============================================================================

\echo '--- TEST 1: Empty query + facet filter (primary_profession=actress) ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'test_actors',  -- p_schema_name
        'actors_faceting_mv',  -- p_table_name
        '',  -- p_query (empty)
        '{"primary_profession":"actress"}'::jsonb,  -- p_facets
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
    
    -- Verify we have results
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found (expected at least 5 actresses)';
    END IF;
    
    RAISE NOTICE 'PASS: Found % results', v_result.total_found;
    
    -- CRITICAL: Facets MUST NOT be empty
    IF v_result.facets IS NULL THEN
        RAISE EXCEPTION 'FAIL: facets is NULL (should be JSONB array)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    
    IF v_facets_count = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] (should contain facet groups when results exist)';
    END IF;
    
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
    RAISE NOTICE 'Facets data: %', v_result.facets::text;
END;
$$;

\echo 'PASS: TEST 1 completed successfully'
\echo ''

-- ============================================================================
-- TEST 2: Text search + facet filter - MUST return facets (THIS IS THE BUG FIX)
-- ============================================================================

\echo '--- TEST 2: Text search "bergman" + facet filter (primary_profession=actress) ---'
\echo 'This test verifies the bug fix where facets were empty with query + filter'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
    v_results_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'test_actors',  -- p_schema_name
        'actors_faceting_mv',  -- p_table_name
        'bergman',  -- p_query (text search)
        '{"primary_profession":"actress"}'::jsonb,  -- p_facets
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
    
    -- Verify we have results
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found for "bergman" + actress filter (expected at least 2)';
    END IF;
    
    v_results_count := jsonb_array_length(v_result.results);
    RAISE NOTICE 'PASS: Found % results (total_found: %)', v_results_count, v_result.total_found;
    
    -- CRITICAL: Facets MUST NOT be empty (this was the bug!)
    IF v_result.facets IS NULL THEN
        RAISE EXCEPTION 'FAIL: facets is NULL (should be JSONB array, even if empty)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    
    IF v_facets_count = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] - THIS WAS THE BUG! Facets should be returned when results exist';
    END IF;
    
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
    RAISE NOTICE 'Facets data: %', v_result.facets::text;
    
    -- Verify that facets contain the expected data
    IF v_result.facets::text = '[]' THEN
        RAISE EXCEPTION 'FAIL: facets is empty array [] (should contain facet data)';
    END IF;
    
    RAISE NOTICE 'PASS: Facets contain valid data';
END;
$$;

\echo 'PASS: TEST 2 completed successfully (bug fix verified)'
\echo ''

-- ============================================================================
-- TEST 3: Text search without facet filter - should return facets
-- ============================================================================

\echo '--- TEST 3: Text search "bergman" without facet filter ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'test_actors',
        'actors_faceting_mv',
        'bergman',  -- p_query
        NULL,  -- p_facets (no filter)
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
    
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found for "bergman"';
    END IF;
    
    RAISE NOTICE 'PASS: Found % results', v_result.total_found;
    
    -- Facets should be returned (all facets, not filtered)
    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty (should show all facets when no filter applied)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
END;
$$;

\echo 'PASS: TEST 3 completed successfully'
\echo ''

-- ============================================================================
-- TEST 4: Text search "streep" + facet filter - verify facets
-- ============================================================================

\echo '--- TEST 4: Text search "streep" + facet filter (primary_profession=actress) ---'

DO $$
DECLARE
    v_result record;
    v_facets_count int;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'test_actors',
        'actors_faceting_mv',
        'streep',  -- p_query
        '{"primary_profession":"actress"}'::jsonb,  -- p_facets
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
    
    IF v_result.total_found = 0 THEN
        RAISE EXCEPTION 'FAIL: No results found for "streep" + actress filter';
    END IF;
    
    RAISE NOTICE 'PASS: Found % results', v_result.total_found;
    
    -- Facets MUST be returned
    IF v_result.facets IS NULL OR jsonb_array_length(v_result.facets) = 0 THEN
        RAISE EXCEPTION 'FAIL: facets is empty [] (should contain facets when results exist)';
    END IF;
    
    v_facets_count := jsonb_array_length(v_result.facets);
    RAISE NOTICE 'PASS: Facets returned with % facet groups', v_facets_count;
END;
$$;

\echo 'PASS: TEST 4 completed successfully'
\echo ''

-- ============================================================================
-- TEST 5: Verify facet values are correct
-- ============================================================================

\echo '--- TEST 5: Verify facet values contain expected data ---'

DO $$
DECLARE
    v_result record;
    v_facet jsonb;
    v_has_primary_profession boolean := false;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'test_actors',
        'actors_faceting_mv',
        'bergman',
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
    
    -- Check that facets contain primary_profession facet
    FOR v_facet IN SELECT jsonb_array_elements(v_result.facets)
    LOOP
        IF v_facet->>'facet_name' = 'primary_profession' THEN
            v_has_primary_profession := true;
            RAISE NOTICE 'PASS: Found primary_profession facet with % values', 
                jsonb_array_length(v_facet->'values');
            EXIT;
        END IF;
    END LOOP;
    
    IF NOT v_has_primary_profession THEN
        RAISE EXCEPTION 'FAIL: primary_profession facet not found in results';
    END IF;
END;
$$;

\echo 'PASS: TEST 5 completed successfully'
\echo ''

-- ============================================================================
-- Summary
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo 'ALL TESTS PASSED!'
\echo '============================================================================'
\echo ''
\echo 'Summary:'
\echo '  - TEST 1: Empty query + facet filter returns facets ✓'
\echo '  - TEST 2: Text search + facet filter returns facets ✓ (BUG FIX VERIFIED)'
\echo '  - TEST 3: Text search without filter returns facets ✓'
\echo '  - TEST 4: Another text search + filter combination ✓'
\echo '  - TEST 5: Facet values are correct ✓'
\echo ''
\echo 'The bug fix ensures that facets are always returned when results exist,'
\echo 'regardless of whether a text query is provided or not.'
\echo '============================================================================'
\echo ''
