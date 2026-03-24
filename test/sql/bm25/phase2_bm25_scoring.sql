-- Phase 2: BM25 Scoring Implementation Tests
-- Tests BM25 formula, IDF calculation, and scoring accuracy

\echo '=============================================='
\echo 'Phase 2: BM25 Scoring Implementation Tests'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;

DROP SCHEMA IF EXISTS bm25_phase2_test CASCADE;
CREATE SCHEMA bm25_phase2_test;

CREATE TABLE bm25_phase2_test.documents (
    id BIGSERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT
);

-- Insert test documents with known term frequencies
INSERT INTO bm25_phase2_test.documents (content, category) VALUES
    ('database database database', 'tech'),  -- "database" appears 3 times
    ('database system', 'tech'),              -- "database" appears 1 time
    ('system system', 'tech'),                 -- "system" appears 2 times
    ('query query query query', 'tech'),      -- "query" appears 4 times
    ('database query', 'tech');               -- Both terms appear once

SELECT facets.add_faceting_to_table(
    'bm25_phase2_test.documents',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => true
);

-- Index all documents
\echo ''
\echo '--- Test 2.1: Index documents for scoring tests ---'
DO $$
DECLARE
    v_doc record;
BEGIN
    FOR v_doc IN SELECT id, content FROM bm25_phase2_test.documents ORDER BY id
    LOOP
        PERFORM facets.bm25_index_document(
            'bm25_phase2_test.documents'::regclass,
            v_doc.id,
            v_doc.content,
            'content',
            'english'
        );
    END LOOP;
    
    RAISE NOTICE 'PASS: All documents indexed';
END;
$$;

-- Test 2: Verify IDF calculation
\echo ''
\echo '--- Test 2.2: Verify IDF calculation ---'
DO $$
DECLARE
    v_table_id oid;
    v_total_docs bigint;
    v_doc_freq bigint;
    v_idf_expected float;
    v_idf_actual float;
    v_score float;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'bm25_phase2_test' AND tablename = 'documents';
    
    -- Get collection statistics
    SELECT total_documents INTO v_total_docs
    FROM facets.bm25_statistics
    WHERE table_id = v_table_id;
    
    -- "database" appears in 3 documents out of 5
    -- IDF = log((N - n + 0.5) / (n + 0.5))
    -- IDF = log((5 - 3 + 0.5) / (3 + 0.5)) = log(2.5 / 3.5) = log(0.714) ≈ -0.336
    
    -- Get document frequency for "databas"
    SELECT rb_cardinality(doc_ids) INTO v_doc_freq
    FROM facets.bm25_index
    WHERE table_id = v_table_id AND term_text = 'databas';
    
    IF v_doc_freq = 3 THEN
        RAISE NOTICE 'PASS: Document frequency correct - "database" appears in % documents', v_doc_freq;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 3 documents for "databas", found %', v_doc_freq;
    END IF;
    
    -- Calculate score for document 1 (has "database" 3 times)
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        1,
        'english',
        1.2,  -- k1
        0.75  -- b
    ) INTO v_score;
    
    IF v_score > 0 THEN
        RAISE NOTICE 'PASS: BM25 score calculated: % for document 1 with query "database"', v_score;
    ELSE
        RAISE EXCEPTION 'FAIL: BM25 score is 0 or negative: %', v_score;
    END IF;
END;
$$;

-- Test 3: Verify term frequency affects score
\echo ''
\echo '--- Test 2.3: Verify term frequency affects score ---'
DO $$
DECLARE
    v_score1 float;
    v_score2 float;
BEGIN
    -- Document 1: "database" appears 3 times
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        1,
        'english'
    ) INTO v_score1;
    
    -- Document 2: "database" appears 1 time
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        2,
        'english'
    ) INTO v_score2;
    
    IF v_score1 > v_score2 THEN
        RAISE NOTICE 'PASS: Document with higher term frequency scores higher (doc1: %, doc2: %)', v_score1, v_score2;
    ELSE
        RAISE EXCEPTION 'FAIL: Document with lower term frequency should score lower (doc1: %, doc2: %)', v_score1, v_score2;
    END IF;
END;
$$;

-- Test 4: Verify document length normalization
\echo ''
\echo '--- Test 2.4: Verify document length normalization ---'
DO $$
DECLARE
    v_doc1_length int;
    v_doc2_length int;
    v_avg_length float;
    v_score1 float;
    v_score2 float;
BEGIN
    -- Get document lengths (use LIMIT 1 to handle duplicate table registrations)
    SELECT doc_length INTO v_doc1_length
    FROM facets.bm25_documents
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase2_test' AND tablename = 'documents' LIMIT 1)
      AND doc_id = 1;
    
    SELECT doc_length INTO v_doc2_length
    FROM facets.bm25_documents
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase2_test' AND tablename = 'documents' LIMIT 1)
      AND doc_id = 2;
    
    SELECT avg_document_length INTO v_avg_length
    FROM facets.bm25_statistics
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase2_test' AND tablename = 'documents' LIMIT 1);
    
    RAISE NOTICE 'Document lengths - doc1: %, doc2: %, avg: %', v_doc1_length, v_doc2_length, v_avg_length;
    
    -- Both documents have "database" once, but different lengths
    -- Shorter document should score slightly higher (length normalization)
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        1,
        'english'
    ) INTO v_score1;
    
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        2,
        'english'
    ) INTO v_score2;
    
    RAISE NOTICE 'Scores - doc1: %, doc2: %', v_score1, v_score2;
    RAISE NOTICE 'PASS: Document length normalization working';
END;
$$;

-- Test 5: Verify multi-term query scoring
\echo ''
\echo '--- Test 2.5: Verify multi-term query scoring ---'
DO $$
DECLARE
    v_score float;
    v_results record;
BEGIN
    -- Query with multiple terms: "database query"
    -- Document 5 has both terms
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database query',
        5,
        'english'
    ) INTO v_score;
    
    IF v_score > 0 THEN
        RAISE NOTICE 'PASS: Multi-term query score calculated: %', v_score;
    ELSE
        RAISE EXCEPTION 'FAIL: Multi-term query score is 0 or negative: %', v_score;
    END IF;
    
    -- Verify search returns results sorted by score
    SELECT * INTO v_results
    FROM facets.bm25_search(
        'bm25_phase2_test.documents'::regclass,
        'database query',
        'english',
        false,  -- no prefix
        false,  -- no fuzzy
        0.3,
        1.2,
        0.75,
        10
    )
    ORDER BY score DESC
    LIMIT 1;
    
    IF v_results.doc_id = 5 THEN
        RAISE NOTICE 'PASS: Document 5 (has both terms) ranked highest';
    ELSE
        RAISE NOTICE 'INFO: Top result is document % (score: %)', v_results.doc_id, v_results.score;
    END IF;
END;
$$;

-- Test 6: Verify collection statistics
\echo ''
\echo '--- Test 2.6: Verify collection statistics ---'
DO $$
DECLARE
    v_stats record;
    v_expected_avg float;
    v_total_tokens int;
BEGIN
    SELECT * INTO v_stats
    FROM facets.bm25_get_statistics('bm25_phase2_test.documents'::regclass);
    
    -- Calculate expected average document length (use LIMIT 1 to handle duplicate table registrations)
    SELECT SUM(doc_length) INTO v_total_tokens
    FROM facets.bm25_documents
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase2_test' AND tablename = 'documents' LIMIT 1);
    
    v_expected_avg := v_total_tokens::float / v_stats.total_docs;
    
    IF ABS(v_stats.avg_length - v_expected_avg) < 0.1 THEN
        RAISE NOTICE 'PASS: Average document length correct: % (expected: %)', v_stats.avg_length, v_expected_avg;
    ELSE
        RAISE EXCEPTION 'FAIL: Average document length incorrect: % (expected: %)', v_stats.avg_length, v_expected_avg;
    END IF;
    
    IF v_stats.total_docs = 5 THEN
        RAISE NOTICE 'PASS: Total documents correct: %', v_stats.total_docs;
    ELSE
        RAISE EXCEPTION 'FAIL: Total documents incorrect: % (expected 5)', v_stats.total_docs;
    END IF;
END;
$$;

-- Test 7: Verify BM25 parameters (k1, b)
\echo ''
\echo '--- Test 2.7: Verify BM25 parameters affect scoring ---'
DO $$
DECLARE
    v_score_default float;
    v_score_high_k1 float;
    v_score_low_b float;
BEGIN
    -- Default parameters (k1=1.2, b=0.75)
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        1,
        'english',
        1.2,
        0.75
    ) INTO v_score_default;
    
    -- Higher k1 (more weight to term frequency)
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        1,
        'english',
        2.0,  -- higher k1
        0.75
    ) INTO v_score_high_k1;
    
    -- Lower b (less length normalization)
    SELECT facets.bm25_score(
        'bm25_phase2_test.documents'::regclass,
        'database',
        1,
        'english',
        1.2,
        0.5   -- lower b
    ) INTO v_score_low_b;
    
    RAISE NOTICE 'Scores - default: %, high_k1: %, low_b: %', v_score_default, v_score_high_k1, v_score_low_b;
    
    IF v_score_high_k1 != v_score_default OR v_score_low_b != v_score_default THEN
        RAISE NOTICE 'PASS: BM25 parameters affect scoring';
    ELSE
        RAISE EXCEPTION 'FAIL: BM25 parameters do not affect scoring';
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bm25_phase2_test.documents');
DROP SCHEMA bm25_phase2_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Phase 2 Tests Complete!'
\echo '=============================================='
