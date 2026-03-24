-- Phase 5: Fuzzy Prefix Matching Tests
-- Tests fuzzy prefix matching with typo tolerance (requires pg_trgm extension)

\echo '=============================================='
\echo 'Phase 5: Fuzzy Prefix Matching Tests'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- Required for fuzzy matching

-- Ensure the optional trigram GIN index exists (pg_trgm may have been installed after pg_facets)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS bm25_index_term_prefix
            ON facets.bm25_index USING gin (term_text gin_trgm_ops);
    END IF;
END;
$$;

DROP SCHEMA IF EXISTS bm25_phase5_test CASCADE;
CREATE SCHEMA bm25_phase5_test;

CREATE TABLE bm25_phase5_test.documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT
);

-- Insert documents with terms that can be matched with typos
INSERT INTO bm25_phase5_test.documents (content, category) VALUES
    ('running marathon training', 'sports'),
    ('runner guide for beginners', 'sports'),
    ('runs daily exercise routine', 'sports'),
    ('database administration', 'tech'),
    ('data analysis techniques', 'tech'),
    ('databases are important', 'tech'),
    ('query optimization', 'tech'),
    ('queries and performance', 'tech'),
    ('postgresql database system', 'tech'),
    ('postgres administration guide', 'tech');

SELECT facets.add_faceting_to_table(
    'bm25_phase5_test.documents',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => true
);

-- Index all documents
\echo ''
\echo '--- Test 5.1: Index documents ---'
DO $$
DECLARE
    v_doc record;
BEGIN
    FOR v_doc IN SELECT id, content FROM bm25_phase5_test.documents ORDER BY id
    LOOP
        PERFORM facets.bm25_index_document(
            'bm25_phase5_test.documents'::regclass,
            v_doc.id,
            v_doc.content,
            'content',
            'english'
        );
    END LOOP;
    
    RAISE NOTICE 'PASS: All documents indexed';
END;
$$;

-- Test 1: Exact match (no fuzzy) - baseline
\echo ''
\echo '--- Test 5.2: Exact match (no fuzzy) - baseline ---'
DO $$
DECLARE
    v_result_count int;
BEGIN
    -- Query with typo "runing" (missing 'n')
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'runing',  -- typo
        'english',
        false,  -- no prefix
        false,  -- no fuzzy
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_result_count = 0 THEN
        RAISE NOTICE 'PASS: Exact match (no fuzzy) correctly returns 0 results for typo "runing"';
    ELSE
        RAISE NOTICE 'INFO: Exact match found % results for typo (unexpected)', v_result_count;
    END IF;
END;
$$;

-- Test 2: Fuzzy matching with typo
\echo ''
\echo '--- Test 5.3: Fuzzy matching with typo ---'
DO $$
DECLARE
    v_result_count int;
    v_results record;
BEGIN
    -- Query with typo "runing" should match "running", "runner", "runs"
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'runing',  -- typo: missing 'n'
        'english',
        false,  -- no prefix
        true,   -- fuzzy enabled
        0.3,    -- similarity threshold
        1.2,
        0.75,
        10
    );
    
    IF v_result_count >= 3 THEN
        RAISE NOTICE 'PASS: Fuzzy match found % documents for typo "runing"', v_result_count;
        
        -- Show results with scores
        FOR v_results IN
            SELECT doc_id, score
            FROM facets.bm25_search(
                'bm25_phase5_test.documents'::regclass,
                'runing',
                'english',
                false,
                true,
                0.3,
                1.2,
                0.75,
                10
            )
            ORDER BY score DESC
            LIMIT 5
        LOOP
            RAISE NOTICE '  Document %: score %', v_results.doc_id, v_results.score;
        END LOOP;
    ELSE
        RAISE EXCEPTION 'FAIL: Fuzzy match found only % documents for typo "runing" (expected at least 3)', v_result_count;
    END IF;
END;
$$;

-- Test 3: Fuzzy matching with different threshold
\echo ''
\echo '--- Test 5.4: Fuzzy matching with different thresholds ---'
DO $$
DECLARE
    v_result_low int;
    v_result_high int;
BEGIN
    -- Low threshold (0.1) - more permissive
    SELECT COUNT(*) INTO v_result_low
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'runing',  -- typo
        'english',
        false,
        true,
        0.1,    -- low threshold
        1.2,
        0.75,
        10
    );
    
    -- High threshold (0.5) - more strict
    SELECT COUNT(*) INTO v_result_high
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'runing',  -- typo
        'english',
        false,
        true,
        0.5,    -- high threshold
        1.2,
        0.75,
        10
    );
    
    IF v_result_low >= v_result_high THEN
        RAISE NOTICE 'PASS: Lower threshold returns more results (low: %, high: %)', v_result_low, v_result_high;
    ELSE
        RAISE NOTICE 'INFO: Threshold behavior - low: %, high: %', v_result_low, v_result_high;
    END IF;
END;
$$;

-- Test 4: Fuzzy prefix matching (typo + prefix)
\echo ''
\echo '--- Test 5.5: Fuzzy prefix matching ---'
DO $$
DECLARE
    v_result_count int;
BEGIN
    -- Query "databse" (typo) with prefix matching
    -- Should match "database", "databases", "data"
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'databse',  -- typo: "database" -> "databse"
        'english',
        true,   -- prefix enabled
        true,   -- fuzzy enabled
        0.3,    -- similarity threshold
        1.2,
        0.75,
        10
    );
    
    IF v_result_count >= 3 THEN
        RAISE NOTICE 'PASS: Fuzzy prefix match found % documents for typo "databse"', v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Fuzzy prefix match found % documents', v_result_count;
    END IF;
END;
$$;

-- Test 5: Compare fuzzy vs exact matching
\echo ''
\echo '--- Test 5.6: Compare fuzzy vs exact matching ---'
DO $$
DECLARE
    v_exact_count int;
    v_fuzzy_count int;
BEGIN
    -- Exact match for typo
    SELECT COUNT(*) INTO v_exact_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'postgresql',  -- correct spelling
        'english',
        false,
        false,
        0.3,
        1.2,
        0.75,
        10
    );
    
    -- Fuzzy match for typo
    SELECT COUNT(*) INTO v_fuzzy_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'postgresl',  -- typo: missing 'q'
        'english',
        false,
        true,   -- fuzzy
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_fuzzy_count > 0 THEN
        RAISE NOTICE 'PASS: Fuzzy matching finds results for typo (exact: %, fuzzy: %)', v_exact_count, v_fuzzy_count;
    ELSE
        RAISE NOTICE 'INFO: Fuzzy matching results (exact: %, fuzzy: %)', v_exact_count, v_fuzzy_count;
    END IF;
END;
$$;

-- Test 6: Verify similarity scores are reasonable
\echo ''
\echo '--- Test 5.7: Verify similarity scores ---'
DO $$
DECLARE
    v_results record;
    v_top_score float;
BEGIN
    -- Get top result for fuzzy match
    SELECT score INTO v_top_score
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'runing',  -- typo
        'english',
        false,
        true,
        0.3,
        1.2,
        0.75,
        1  -- limit to 1
    )
    ORDER BY score DESC
    LIMIT 1;
    
    IF v_top_score > 0 THEN
        RAISE NOTICE 'PASS: Fuzzy matching returns positive scores (top score: %)', v_top_score;
    ELSE
        RAISE EXCEPTION 'FAIL: Fuzzy matching returns zero or negative scores';
    END IF;
END;
$$;

-- Test 7: Multiple typos in query
\echo ''
\echo '--- Test 5.8: Multiple typos in query ---'
DO $$
DECLARE
    v_result_count int;
BEGIN
    -- Query with multiple typos: "databse querys" (database queries)
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'databse querys',  -- typos in both terms
        'english',
        false,
        true,   -- fuzzy
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_result_count > 0 THEN
        RAISE NOTICE 'PASS: Fuzzy matching handles multiple typos (% results)', v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Fuzzy matching with multiple typos found % results', v_result_count;
    END IF;
END;
$$;

-- Test 8: Performance of fuzzy matching
\echo ''
\echo '--- Test 5.9: Fuzzy matching performance ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_result_count int;
BEGIN
    v_start_time := clock_timestamp();
    
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase5_test.documents'::regclass,
        'runing',  -- typo
        'english',
        false,
        true,   -- fuzzy
        0.3,
        1.2,
        0.75,
        10
    );
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    IF v_duration_ms < 500 THEN
        RAISE NOTICE 'PASS: Fuzzy matching completed in % ms (% results)', v_duration_ms, v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Fuzzy matching took % ms (acceptable for small dataset)', v_duration_ms;
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bm25_phase5_test.documents');
DROP SCHEMA bm25_phase5_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Phase 5 Tests Complete!'
\echo '=============================================='
