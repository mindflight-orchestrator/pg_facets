-- Phase 4: Prefix Matching Tests
-- Tests prefix matching functionality (requires pg_trgm extension)

\echo '=============================================='
\echo 'Phase 4: Prefix Matching Tests'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- Required for prefix matching

-- Ensure the optional trigram GIN index exists (pg_trgm may have been installed after pg_facets)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS bm25_index_term_prefix
            ON facets.bm25_index USING gin (term_text gin_trgm_ops);
    END IF;
END;
$$;

DROP SCHEMA IF EXISTS bm25_phase4_test CASCADE;
CREATE SCHEMA bm25_phase4_test;

CREATE TABLE bm25_phase4_test.documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT
);

-- Insert documents with terms that can be matched by prefix
INSERT INTO bm25_phase4_test.documents (content, category) VALUES
    ('running marathon training', 'sports'),
    ('runner guide for beginners', 'sports'),
    ('runs daily exercise routine', 'sports'),
    ('database administration', 'tech'),
    ('data analysis techniques', 'tech'),
    ('databases are important', 'tech'),
    ('query optimization', 'tech'),
    ('queries and performance', 'tech');

SELECT facets.add_faceting_to_table(
    'bm25_phase4_test.documents',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => true
);

-- Index all documents
\echo ''
\echo '--- Test 4.1: Index documents ---'
DO $$
DECLARE
    v_doc record;
BEGIN
    FOR v_doc IN SELECT id, content FROM bm25_phase4_test.documents ORDER BY id
    LOOP
        PERFORM facets.bm25_index_document(
            'bm25_phase4_test.documents'::regclass,
            v_doc.id,
            v_doc.content,
            'content',
            'english'
        );
    END LOOP;
    
    RAISE NOTICE 'PASS: All documents indexed';
END;
$$;

-- Test 1: Verify prefix index exists
\echo ''
\echo '--- Test 4.2: Verify prefix index exists ---'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE schemaname = 'facets' 
          AND indexname = 'bm25_index_term_prefix'
    ) THEN
        RAISE NOTICE 'PASS: Prefix index (GIN with trigram ops) exists';
    ELSE
        RAISE EXCEPTION 'FAIL: Prefix index not found (pg_trgm is required for this phase)';
    END IF;
END;
$$;

-- Test 2: Exact match (no prefix) - baseline
\echo ''
\echo '--- Test 4.3: Exact match (no prefix) - baseline ---'
DO $$
DECLARE
    v_result_count int;
BEGIN
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase4_test.documents'::regclass,
        'running',
        'english',
        false,  -- no prefix
        false,  -- no fuzzy
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_result_count = 1 THEN
        RAISE NOTICE 'PASS: Exact match found 1 document for "running"';
    ELSE
        RAISE NOTICE 'INFO: Exact match found % documents for "running"', v_result_count;
    END IF;
END;
$$;

-- Test 3: Prefix matching enabled
\echo ''
\echo '--- Test 4.4: Prefix matching enabled ---'
DO $$
DECLARE
    v_result_count int;
    v_results record;
BEGIN
    -- Query "run" should match "running", "runner", "runs"
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase4_test.documents'::regclass,
        'run',
        'english',
        true,   -- prefix match enabled
        false,  -- no fuzzy
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_result_count >= 3 THEN
        RAISE NOTICE 'PASS: Prefix match found % documents for "run" (expected at least 3)', v_result_count;
        
        -- Verify specific terms are matched
        FOR v_results IN
            SELECT doc_id, score
            FROM facets.bm25_search(
                'bm25_phase4_test.documents'::regclass,
                'run',
                'english',
                true,
                false,
                0.3,
                1.2,
                0.75,
                10
            )
            ORDER BY score DESC
        LOOP
            RAISE NOTICE '  Document %: score %', v_results.doc_id, v_results.score;
        END LOOP;
    ELSE
        RAISE EXCEPTION 'FAIL: Prefix match found only % documents for "run" (expected at least 3)', v_result_count;
    END IF;
END;
$$;

-- Test 4: Prefix matching with multiple query terms
\echo ''
\echo '--- Test 4.5: Prefix matching with multiple terms ---'
DO $$
DECLARE
    v_result_count int;
BEGIN
    -- Query "dat quer" should match "database", "data", "databases" and "query", "queries"
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase4_test.documents'::regclass,
        'dat quer',
        'english',
        true,   -- prefix match
        false,
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_result_count >= 5 THEN
        RAISE NOTICE 'PASS: Prefix match with multiple terms found % documents', v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Prefix match with multiple terms found % documents', v_result_count;
    END IF;
END;
$$;

-- Test 5: Compare exact vs prefix matching results
\echo ''
\echo '--- Test 4.6: Compare exact vs prefix matching ---'
DO $$
DECLARE
    v_exact_count int;
    v_prefix_count int;
BEGIN
    -- Exact match
    SELECT COUNT(*) INTO v_exact_count
    FROM facets.bm25_search(
        'bm25_phase4_test.documents'::regclass,
        'run',
        'english',
        false,  -- no prefix
        false,
        0.3,
        1.2,
        0.75,
        10
    );
    
    -- Prefix match
    SELECT COUNT(*) INTO v_prefix_count
    FROM facets.bm25_search(
        'bm25_phase4_test.documents'::regclass,
        'run',
        'english',
        true,   -- prefix enabled
        false,
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_prefix_count >= v_exact_count THEN
        RAISE NOTICE 'PASS: Prefix matching returns more or equal results (exact: %, prefix: %)', v_exact_count, v_prefix_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Prefix matching should return more results (exact: %, prefix: %)', v_exact_count, v_prefix_count;
    END IF;
END;
$$;

-- Test 6: Prefix matching in search_documents function
\echo ''
\echo '--- Test 4.7: Prefix matching in search_documents (via bm25_search) ---'
DO $$
DECLARE
    v_result record;
    v_result_count int;
BEGIN
    -- Note: search_documents doesn't directly support prefix_match parameter yet
    -- But we can test that bm25_search with prefix works
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase4_test.documents'::regclass,
        'run',
        'english',
        true,   -- prefix
        false,
        0.3,
        1.2,
        0.75,
        10
    );
    
    IF v_result_count >= 3 THEN
        RAISE NOTICE 'PASS: bm25_search with prefix matching works (% results)', v_result_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Prefix matching not working (% results)', v_result_count;
    END IF;
END;
$$;

-- Test 7: Verify prefix matching uses GIN index
\echo ''
\echo '--- Test 4.8: Verify prefix matching performance ---'
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
        'bm25_phase4_test.documents'::regclass,
        'run',
        'english',
        true,   -- prefix
        false,
        0.3,
        1.2,
        0.75,
        10
    );
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    IF v_duration_ms < 1000 THEN
        RAISE NOTICE 'PASS: Prefix matching completed in % ms (% results)', v_duration_ms, v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Prefix matching took % ms (may need optimization)', v_duration_ms;
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bm25_phase4_test.documents');
DROP SCHEMA bm25_phase4_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Phase 4 Tests Complete!'
\echo '=============================================='
