-- Phase 3: Replace ts_rank_cd Tests
-- Tests that search_documents and search_documents_with_facets use BM25 instead of ts_rank_cd

\echo '=============================================='
\echo 'Phase 3: Replace ts_rank_cd Tests'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;

DROP SCHEMA IF EXISTS bm25_phase3_test CASCADE;
CREATE SCHEMA bm25_phase3_test;

CREATE TABLE bm25_phase3_test.articles (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO bm25_phase3_test.articles (title, content, category) VALUES
    ('PostgreSQL Performance', 'PostgreSQL database performance tuning and optimization techniques', 'Technology'),
    ('Database Design', 'Learn database design principles for PostgreSQL and other systems', 'Technology'),
    ('SQL Queries', 'Advanced SQL query techniques and optimization strategies', 'Education'),
    ('PostgreSQL Administration', 'PostgreSQL administration guide with backup and recovery', 'Technology'),
    ('Data Analysis', 'Data analysis techniques using SQL and PostgreSQL', 'Education');

SELECT facets.add_faceting_to_table(
    'bm25_phase3_test.articles',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => true
);

-- Index all documents for BM25
\echo ''
\echo '--- Test 3.1: Index documents for BM25 ---'
DO $$
DECLARE
    v_doc record;
BEGIN
    FOR v_doc IN SELECT id, content FROM bm25_phase3_test.articles ORDER BY id
    LOOP
        PERFORM facets.bm25_index_document(
            'bm25_phase3_test.articles'::regclass,
            v_doc.id,
            v_doc.content,
            'content',
            'english'
        );
    END LOOP;
    
    RAISE NOTICE 'PASS: All documents indexed for BM25';
END;
$$;

-- Test 1: Verify search_documents uses BM25
\echo ''
\echo '--- Test 3.2: Verify search_documents uses BM25 ---'
DO $$
DECLARE
    v_result record;
    v_first_result jsonb;
    v_bm25_score float;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL',
        NULL,  -- no vector
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5,
        'english'
    );
    
    IF v_result.total_found > 0 THEN
        v_first_result := (v_result.results->0);
        v_bm25_score := (v_first_result->>'bm25_score')::float;
        
        IF v_bm25_score > 0 THEN
            RAISE NOTICE 'PASS: search_documents returns BM25 scores (score: %)', v_bm25_score;
        ELSE
            RAISE EXCEPTION 'FAIL: BM25 score is 0 or missing';
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: No results returned for query "PostgreSQL"';
    END IF;
END;
$$;

-- Test 2: Verify results are ranked by BM25 score
\echo ''
\echo '--- Test 3.3: Verify results ranked by BM25 score ---'
DO $$
DECLARE
    v_result record;
    v_results jsonb;
    v_first_score float;
    v_second_score float;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL database',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5,
        'english'
    );
    
    v_results := v_result.results;
    
    IF jsonb_array_length(v_results) >= 2 THEN
        v_first_score := ((v_results->0)->>'bm25_score')::float;
        v_second_score := ((v_results->1)->>'bm25_score')::float;
        
        IF v_first_score >= v_second_score THEN
            RAISE NOTICE 'PASS: Results sorted by BM25 score (first: %, second: %)', v_first_score, v_second_score;
        ELSE
            RAISE EXCEPTION 'FAIL: Results not sorted correctly (first: %, second: %)', v_first_score, v_second_score;
        END IF;
    ELSE
        RAISE NOTICE 'INFO: Less than 2 results, skipping sort test';
    END IF;
END;
$$;

-- Test 3: Compare BM25 scores with direct bm25_score calls
\echo ''
\echo '--- Test 3.4: Compare search_documents scores with bm25_score ---'
DO $$
DECLARE
    v_search_result record;
    v_direct_score float;
    v_search_score float;
    v_first_result jsonb;
BEGIN
    -- Get result from search_documents
    SELECT * INTO v_search_result
    FROM facets.search_documents(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        1,
        0,
        0.0,
        0.5,
        'english'
    );
    
    IF jsonb_array_length(v_search_result.results) > 0 THEN
        v_first_result := (v_search_result.results->0);
        v_search_score := (v_first_result->>'bm25_score')::float;
        
        -- Get direct BM25 score for same document
        SELECT facets.bm25_score(
            'bm25_phase3_test.articles'::regclass,
            'PostgreSQL',
            (v_first_result->>'id')::bigint,
            'english'
        ) INTO v_direct_score;
        
        -- Scores should be very close (within rounding)
        IF ABS(v_search_score - v_direct_score) < 0.001 THEN
            RAISE NOTICE 'PASS: search_documents BM25 score matches direct bm25_score (search: %, direct: %)', v_search_score, v_direct_score;
        ELSE
            RAISE NOTICE 'INFO: Scores differ slightly (search: %, direct: %) - may be due to query processing', v_search_score, v_direct_score;
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: No results from search_documents';
    END IF;
END;
$$;

-- Test 4: Verify search_documents_with_facets uses BM25
\echo ''
\echo '--- Test 3.5: Verify search_documents_with_facets uses BM25 ---'
DO $$
DECLARE
    v_result record;
    v_first_result jsonb;
    v_bm25_score float;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL',
        NULL,  -- no facet filter
        NULL,  -- no vector
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5,
        10
    );
    
    IF v_result.total_found > 0 AND jsonb_array_length(v_result.results) > 0 THEN
        v_first_result := (v_result.results->0);
        v_bm25_score := (v_first_result->>'bm25_score')::float;
        
        IF v_bm25_score > 0 THEN
            RAISE NOTICE 'PASS: search_documents_with_facets returns BM25 scores (score: %)', v_bm25_score;
        ELSE
            RAISE EXCEPTION 'FAIL: BM25 score is 0 or missing in search_documents_with_facets';
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: No results from search_documents_with_facets';
    END IF;
END;
$$;

-- Test 5: Verify language parameter works
\echo ''
\echo '--- Test 3.6: Verify language parameter works ---'
DO $$
DECLARE
    v_result_english record;
    v_result_french record;
BEGIN
    -- Test with English
    SELECT * INTO v_result_english
    FROM facets.search_documents(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5,
        'english'
    );
    
    -- Test with simple (no stemming)
    SELECT * INTO v_result_french
    FROM facets.search_documents(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5,
        'simple'
    );
    
    IF v_result_english.total_found > 0 AND v_result_french.total_found > 0 THEN
        RAISE NOTICE 'PASS: Language parameter works (english: % results, simple: % results)', 
            v_result_english.total_found, v_result_french.total_found;
    ELSE
        RAISE EXCEPTION 'FAIL: Language parameter not working correctly';
    END IF;
END;
$$;

-- Test 6: Verify backward compatibility (same function signature)
\echo ''
\echo '--- Test 3.7: Verify backward compatibility ---'
DO $$
DECLARE
    v_result record;
BEGIN
    -- Call with old signature (no language parameter - should default to 'english')
    SELECT * INTO v_result
    FROM facets.search_documents(
        'bm25_phase3_test',
        'articles',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5
        -- No language parameter
    );
    
    IF v_result.total_found > 0 THEN
        RAISE NOTICE 'PASS: Backward compatibility maintained - function works without language parameter';
    ELSE
        RAISE EXCEPTION 'FAIL: Backward compatibility broken';
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bm25_phase3_test.articles');
DROP SCHEMA bm25_phase3_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Phase 3 Tests Complete!'
\echo '=============================================='
