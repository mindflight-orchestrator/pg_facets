-- ==============================================
-- Phase 3: Parallel Indexing Tests
-- ==============================================
\echo '=============================================='
\echo 'Phase 3: Parallel Indexing Tests'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;
CREATE EXTENSION IF NOT EXISTS dblink;

-- Clean up previous test data
DROP SCHEMA IF EXISTS bm25_phase3_test CASCADE;
CREATE SCHEMA bm25_phase3_test;

-- ==============================================
-- Test 3.1: Batch Indexing
-- ==============================================
\echo ''
\echo '--- Test 3.1: Batch Indexing ---'

CREATE TABLE bm25_phase3_test.batch_docs (
    id BIGSERIAL PRIMARY KEY,
    content TEXT,
    category TEXT
);

-- Insert test documents
INSERT INTO bm25_phase3_test.batch_docs (content, category)
SELECT 
    'Document ' || i || ' about ' || 
    CASE (i % 5) 
        WHEN 0 THEN 'database systems and SQL queries'
        WHEN 1 THEN 'web development and JavaScript'
        WHEN 2 THEN 'machine learning algorithms'
        WHEN 3 THEN 'cloud computing infrastructure'
        ELSE 'software engineering practices'
    END,
    'category_' || (i % 3)
FROM generate_series(1, 100) AS i;

SELECT facets.add_faceting_to_table(
    'bm25_phase3_test.batch_docs',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => false
);

DO $$
DECLARE
    v_result record;
    v_doc_count int;
    v_term_count int;
BEGIN
    -- Test batch indexing
    SELECT * INTO v_result
    FROM facets.bm25_index_documents_batch(
        'bm25_phase3_test.batch_docs'::regclass,
        (SELECT jsonb_agg(jsonb_build_object('doc_id', id, 'content', content)) 
         FROM bm25_phase3_test.batch_docs),
        'content',
        'english',
        25  -- batch_size
    );
    
    IF v_result.indexed_count = 100 THEN
        RAISE NOTICE 'PASS: Batch indexed 100 documents in % ms', v_result.elapsed_ms;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 100 indexed, got %', v_result.indexed_count;
    END IF;
    
    -- Verify documents in index
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents 
    WHERE table_id = 'bm25_phase3_test.batch_docs'::regclass::oid;
    
    IF v_doc_count = 100 THEN
        RAISE NOTICE 'PASS: 100 documents in bm25_documents table';
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 100 documents, found %', v_doc_count;
    END IF;
    
    -- Verify terms created
    SELECT COUNT(*) INTO v_term_count
    FROM facets.bm25_index 
    WHERE table_id = 'bm25_phase3_test.batch_docs'::regclass::oid;
    
    IF v_term_count > 0 THEN
        RAISE NOTICE 'PASS: % unique terms created', v_term_count;
    ELSE
        RAISE EXCEPTION 'FAIL: No terms created';
    END IF;
    
    -- Verify statistics
    PERFORM * FROM facets.bm25_get_statistics('bm25_phase3_test.batch_docs'::regclass)
    WHERE total_docs = 100;
    
    IF FOUND THEN
        RAISE NOTICE 'PASS: Statistics show 100 documents';
    ELSE
        RAISE EXCEPTION 'FAIL: Statistics incorrect';
    END IF;
END;
$$;

-- ==============================================
-- Test 3.2: Worker Function
-- ==============================================
\echo ''
\echo '--- Test 3.2: Worker Function ---'

-- Create fresh table for worker tests
CREATE TABLE bm25_phase3_test.worker_docs (
    id BIGSERIAL PRIMARY KEY,
    content TEXT,
    category TEXT
);

INSERT INTO bm25_phase3_test.worker_docs (content, category)
SELECT 
    'Worker doc ' || i || ' with content about technology',
    'cat_' || (i % 2)
FROM generate_series(1, 30) AS i;

SELECT facets.add_faceting_to_table(
    'bm25_phase3_test.worker_docs',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => false
);

DO $$
DECLARE
    v_result1 record;
    v_result2 record;
    v_result3 record;
    v_total_docs int;
BEGIN
    -- Test worker 1 of 3 (should index docs 1-10)
    SELECT * INTO v_result1
    FROM facets.bm25_index_worker(
        'bm25_phase3_test.worker_docs'::regclass,
        'SELECT id as doc_id, content FROM bm25_phase3_test.worker_docs',
        'content', 'english', 30, 3, 1
    );
    
    IF v_result1.docs_indexed = 10 THEN
        RAISE NOTICE 'PASS: Worker 1 indexed 10 documents in % ms', v_result1.elapsed_ms;
    ELSE
        RAISE EXCEPTION 'FAIL: Worker 1 expected 10, got %', v_result1.docs_indexed;
    END IF;
    
    -- Test worker 2 of 3 (should index docs 11-20)
    SELECT * INTO v_result2
    FROM facets.bm25_index_worker(
        'bm25_phase3_test.worker_docs'::regclass,
        'SELECT id as doc_id, content FROM bm25_phase3_test.worker_docs',
        'content', 'english', 30, 3, 2
    );
    
    IF v_result2.docs_indexed = 10 THEN
        RAISE NOTICE 'PASS: Worker 2 indexed 10 documents in % ms', v_result2.elapsed_ms;
    ELSE
        RAISE EXCEPTION 'FAIL: Worker 2 expected 10, got %', v_result2.docs_indexed;
    END IF;
    
    -- Test worker 3 of 3 (should index docs 21-30)
    SELECT * INTO v_result3
    FROM facets.bm25_index_worker(
        'bm25_phase3_test.worker_docs'::regclass,
        'SELECT id as doc_id, content FROM bm25_phase3_test.worker_docs',
        'content', 'english', 30, 3, 3
    );
    
    IF v_result3.docs_indexed = 10 THEN
        RAISE NOTICE 'PASS: Worker 3 indexed 10 documents in % ms', v_result3.elapsed_ms;
    ELSE
        RAISE EXCEPTION 'FAIL: Worker 3 expected 10, got %', v_result3.docs_indexed;
    END IF;
    
    -- Recalculate statistics
    PERFORM facets.bm25_recalculate_statistics('bm25_phase3_test.worker_docs'::regclass);
    
    -- Verify all documents indexed
    SELECT COUNT(*) INTO v_total_docs
    FROM facets.bm25_documents 
    WHERE table_id = 'bm25_phase3_test.worker_docs'::regclass::oid;
    
    IF v_total_docs = 30 THEN
        RAISE NOTICE 'PASS: Total 30 documents indexed by 3 workers';
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 30 documents, found %', v_total_docs;
    END IF;
END;
$$;

-- ==============================================
-- Test 3.3: Worker Range Calculation
-- ==============================================
\echo ''
\echo '--- Test 3.3: Worker Range Calculation ---'

DO $$
DECLARE
    v_range record;
BEGIN
    -- Test even distribution: 100 docs, 4 workers
    SELECT * INTO v_range FROM facets.bm25_get_worker_range(100, 4, 1);
    IF v_range.start_offset = 0 AND v_range.end_offset = 25 AND v_range.doc_count = 25 THEN
        RAISE NOTICE 'PASS: Worker 1 of 4 gets range 0-25 (25 docs)';
    ELSE
        RAISE EXCEPTION 'FAIL: Worker 1 range incorrect: %, %, %', v_range.start_offset, v_range.end_offset, v_range.doc_count;
    END IF;
    
    SELECT * INTO v_range FROM facets.bm25_get_worker_range(100, 4, 4);
    IF v_range.start_offset = 75 AND v_range.end_offset = 100 AND v_range.doc_count = 25 THEN
        RAISE NOTICE 'PASS: Worker 4 of 4 gets range 75-100 (25 docs)';
    ELSE
        RAISE EXCEPTION 'FAIL: Worker 4 range incorrect: %, %, %', v_range.start_offset, v_range.end_offset, v_range.doc_count;
    END IF;
    
    -- Test uneven distribution: 10 docs, 3 workers
    SELECT * INTO v_range FROM facets.bm25_get_worker_range(10, 3, 3);
    IF v_range.doc_count >= 0 AND v_range.end_offset <= 10 THEN
        RAISE NOTICE 'PASS: Worker 3 of 3 handles uneven distribution (% docs)', v_range.doc_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Worker 3 uneven distribution incorrect';
    END IF;
END;
$$;

-- ==============================================
-- Test 3.4: Recalculate Statistics
-- ==============================================
\echo ''
\echo '--- Test 3.4: Recalculate Statistics ---'

DO $$
DECLARE
    v_stats record;
BEGIN
    -- Get current statistics
    SELECT * INTO v_stats 
    FROM facets.bm25_get_statistics('bm25_phase3_test.batch_docs'::regclass);
    
    IF v_stats.total_docs = 100 THEN
        RAISE NOTICE 'PASS: Statistics show correct document count: %', v_stats.total_docs;
    ELSE
        RAISE EXCEPTION 'FAIL: Statistics incorrect, expected 100, got %', v_stats.total_docs;
    END IF;
    
    IF v_stats.avg_length > 0 THEN
        RAISE NOTICE 'PASS: Average document length: %', v_stats.avg_length;
    ELSE
        RAISE EXCEPTION 'FAIL: Average document length should be > 0';
    END IF;
    
    -- Force recalculation and verify
    PERFORM facets.bm25_recalculate_statistics('bm25_phase3_test.batch_docs'::regclass);
    
    SELECT * INTO v_stats 
    FROM facets.bm25_get_statistics('bm25_phase3_test.batch_docs'::regclass);
    
    IF v_stats.total_docs = 100 THEN
        RAISE NOTICE 'PASS: Statistics correct after recalculation';
    ELSE
        RAISE EXCEPTION 'FAIL: Statistics incorrect after recalculation';
    END IF;
END;
$$;

-- ==============================================
-- Test 3.5: Concurrent Term Updates (Atomic Upsert)
-- ==============================================
\echo ''
\echo '--- Test 3.5: Concurrent Term Updates ---'

CREATE TABLE bm25_phase3_test.concurrent_docs (
    id BIGSERIAL PRIMARY KEY,
    content TEXT,
    category TEXT DEFAULT 'test'
);

-- Insert documents with overlapping terms
INSERT INTO bm25_phase3_test.concurrent_docs (content) VALUES
    ('database query optimization'),
    ('database index performance'),
    ('database storage engine'),
    ('database replication setup'),
    ('database backup strategy');

SELECT facets.add_faceting_to_table(
    'bm25_phase3_test.concurrent_docs',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => false
);

DO $$
DECLARE
    v_term_count int;
    v_db_doc_count int;
BEGIN
    -- Index all documents (they all share the term "database")
    PERFORM facets.bm25_index_document(
        'bm25_phase3_test.concurrent_docs'::regclass,
        id, content, 'content', 'english'
    )
    FROM bm25_phase3_test.concurrent_docs;
    
    -- Verify "databas" (stemmed) term has all 5 documents
    SELECT rb_cardinality(doc_ids)::int INTO v_db_doc_count
    FROM facets.bm25_index
    WHERE table_id = 'bm25_phase3_test.concurrent_docs'::regclass::oid
      AND term_text = 'databas';
    
    IF v_db_doc_count = 5 THEN
        RAISE NOTICE 'PASS: Term "databas" correctly has 5 documents in bitmap';
    ELSE
        RAISE EXCEPTION 'FAIL: Term "databas" has % documents, expected 5', v_db_doc_count;
    END IF;
    
    -- Verify term_freqs has entries for all 5 documents
    SELECT COUNT(*) INTO v_term_count
    FROM facets.bm25_index,
         jsonb_each_text(term_freqs) AS kv
    WHERE table_id = 'bm25_phase3_test.concurrent_docs'::regclass::oid
      AND term_text = 'databas';
    
    IF v_term_count = 5 THEN
        RAISE NOTICE 'PASS: Term frequencies recorded for all 5 documents';
    ELSE
        RAISE EXCEPTION 'FAIL: Term frequencies count: %, expected 5', v_term_count;
    END IF;
END;
$$;

-- ==============================================
-- Test 3.6: Search After Batch Indexing
-- ==============================================
\echo ''
\echo '--- Test 3.6: Search After Batch Indexing ---'

DO $$
DECLARE
    v_result record;
    v_count int;
BEGIN
    -- Search for "database" in batch indexed table
    SELECT COUNT(*) INTO v_count
    FROM facets.bm25_search('bm25_phase3_test.batch_docs'::regclass, 'database');
    
    -- Should find documents with "database" in them (those with i % 5 = 0)
    IF v_count > 0 THEN
        RAISE NOTICE 'PASS: Search found % documents matching "database"', v_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Search found no results for "database"';
    END IF;
    
    -- Search for "machine learning"
    SELECT COUNT(*) INTO v_count
    FROM facets.bm25_search('bm25_phase3_test.batch_docs'::regclass, 'machine learning');
    
    IF v_count > 0 THEN
        RAISE NOTICE 'PASS: Search found % documents matching "machine learning"', v_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Search found no results for "machine learning"';
    END IF;
    
    -- Verify scores are ordered correctly
    SELECT * INTO v_result
    FROM facets.bm25_search('bm25_phase3_test.batch_docs'::regclass, 'database')
    ORDER BY score DESC
    LIMIT 1;
    
    IF v_result.score > 0 THEN
        RAISE NOTICE 'PASS: Top result has positive score: %', v_result.score;
    ELSE
        RAISE EXCEPTION 'FAIL: Top result has non-positive score: %', v_result.score;
    END IF;
END;
$$;

-- ==============================================
-- Test 3.7: Incremental Statistics Update
-- ==============================================
\echo ''
\echo '--- Test 3.7: Incremental Statistics Update ---'

CREATE TABLE bm25_phase3_test.incremental_docs (
    id BIGSERIAL PRIMARY KEY,
    content TEXT,
    category TEXT DEFAULT 'test'
);

SELECT facets.add_faceting_to_table(
    'bm25_phase3_test.incremental_docs',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => false
);

DO $$
DECLARE
    v_stats record;
BEGIN
    -- Index first document
    PERFORM facets.bm25_index_document(
        'bm25_phase3_test.incremental_docs'::regclass,
        1, 'first document with three tokens', 'content', 'english'
    );
    
    SELECT * INTO v_stats 
    FROM facets.bm25_get_statistics('bm25_phase3_test.incremental_docs'::regclass);
    
    IF v_stats.total_docs = 1 THEN
        RAISE NOTICE 'PASS: After 1 doc - total_docs: 1, avg_length: %', v_stats.avg_length;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 1 document after first insert';
    END IF;
    
    -- Index second document (longer)
    PERFORM facets.bm25_index_document(
        'bm25_phase3_test.incremental_docs'::regclass,
        2, 'second document with more tokens added here', 'content', 'english'
    );
    
    SELECT * INTO v_stats 
    FROM facets.bm25_get_statistics('bm25_phase3_test.incremental_docs'::regclass);
    
    IF v_stats.total_docs = 2 THEN
        RAISE NOTICE 'PASS: After 2 docs - total_docs: 2, avg_length: %', v_stats.avg_length;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 2 documents after second insert';
    END IF;
    
    -- Index third document
    PERFORM facets.bm25_index_document(
        'bm25_phase3_test.incremental_docs'::regclass,
        3, 'third doc', 'content', 'english'
    );
    
    SELECT * INTO v_stats 
    FROM facets.bm25_get_statistics('bm25_phase3_test.incremental_docs'::regclass);
    
    IF v_stats.total_docs = 3 THEN
        RAISE NOTICE 'PASS: Incremental stats working - total_docs: 3, avg_length: %', v_stats.avg_length;
    ELSE
        RAISE EXCEPTION 'FAIL: Incremental stats broken';
    END IF;
END;
$$;

-- ==============================================
-- Summary
-- ==============================================
\echo ''
\echo '=============================================='
\echo 'Phase 3 Tests Complete'
\echo '=============================================='
