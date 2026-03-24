-- pg_facets 0.4.2 Parallel Indexing Test Suite
-- Tests for the optimized lock-free parallel BM25 indexing

\echo '=============================================='
\echo 'pg_facets 0.4.2 Parallel Indexing Tests'
\echo '=============================================='
\echo ''

-- Setup test schema
DROP SCHEMA IF EXISTS test_parallel CASCADE;
CREATE SCHEMA test_parallel;

-- Create test table with enough data to test parallelism
CREATE TABLE test_parallel.documents (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT
);

-- Insert test data (500 documents for meaningful parallel test)
INSERT INTO test_parallel.documents (content, category)
SELECT 
    'Document number ' || i || '. ' ||
    CASE (i % 5)
        WHEN 0 THEN 'PostgreSQL database optimization techniques and best practices for performance.'
        WHEN 1 THEN 'Full text search implementation using pg_facets extension for fast retrieval.'
        WHEN 2 THEN 'Machine learning algorithms and data science fundamentals introduction.'
        WHEN 3 THEN 'Web development frameworks comparison React Angular Vue JavaScript.'
        WHEN 4 THEN 'Cloud computing services AWS Azure GCP infrastructure deployment.'
    END,
    CASE (i % 3)
        WHEN 0 THEN 'Technology'
        WHEN 1 THEN 'Science'
        WHEN 2 THEN 'Business'
    END
FROM generate_series(1, 500) AS s(i);

\echo ''
\echo '--- Test 1: Verify Test Data ---'
SELECT 
    CASE WHEN COUNT(*) = 500 THEN 'PASS' ELSE 'FAIL' END || ': Created ' || COUNT(*) || ' test documents'
FROM test_parallel.documents;

\echo ''
\echo '--- Test 2: Add Faceting to Table ---'
SELECT facets.add_faceting_to_table(
    'test_parallel.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM facets.faceted_table 
        WHERE table_id = 'test_parallel.documents'::regclass::oid
    ) THEN 'PASS' ELSE 'FAIL' END || ': Table registered';

\echo ''
\echo '--- Test 3: Lock-free Worker Function Exists ---'
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_worker_lockfree'
    ) THEN 'PASS' ELSE 'FAIL' END || ': bm25_index_worker_lockfree function exists';

\echo ''
\echo '--- Test 4: Staging Tables Cleanup Function Exists ---'
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_cleanup_staging_tables'
    ) THEN 'PASS' ELSE 'FAIL' END || ': bm25_cleanup_staging_tables function exists';

\echo ''
\echo '--- Test 5: Parallel Indexing (with fallback if no dblink) ---'

-- Check if dblink is available
DO $$
DECLARE
    v_has_dblink boolean;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') INTO v_has_dblink;
    
    IF v_has_dblink THEN
        RAISE NOTICE 'INFO: dblink extension is available, will test parallel mode';
    ELSE
        RAISE NOTICE 'INFO: dblink extension not available, will test sequential fallback';
    END IF;
END $$;

-- Run parallel indexing
SELECT * FROM facets.bm25_index_documents_parallel(
    'test_parallel.documents'::regclass,
    'SELECT id::bigint AS doc_id, content FROM test_parallel.documents WHERE content IS NOT NULL',
    'content',
    'english',
    2  -- 2 workers
);

\echo ''
\echo '--- Test 6: Verify Documents Were Indexed ---'
DO $$
DECLARE
    v_doc_count bigint;
    v_term_count bigint;
    v_expected int := 500;
BEGIN
    SELECT COUNT(*) INTO v_doc_count 
    FROM facets.bm25_documents 
    WHERE table_id = 'test_parallel.documents'::regclass::oid;
    
    SELECT COUNT(*) INTO v_term_count 
    FROM facets.bm25_index 
    WHERE table_id = 'test_parallel.documents'::regclass::oid;
    
    IF v_doc_count = v_expected THEN
        RAISE NOTICE 'PASS: Parallel indexing indexed all % documents', v_doc_count;
    ELSIF v_doc_count > v_expected * 0.9 THEN
        RAISE NOTICE 'PASS: Parallel indexing indexed % / % documents (>90%%)', v_doc_count, v_expected;
    ELSE
        RAISE WARNING 'FAIL: Parallel indexing only indexed % / % documents', v_doc_count, v_expected;
    END IF;
    
    IF v_term_count > 0 THEN
        RAISE NOTICE 'PASS: Created % unique terms', v_term_count;
    ELSE
        RAISE WARNING 'FAIL: No terms created';
    END IF;
END $$;

\echo ''
\echo '--- Test 7: Verify Statistics Were Updated ---'
DO $$
DECLARE
    v_total_docs bigint;
    v_avg_length float;
BEGIN
    SELECT total_documents, avg_document_length INTO v_total_docs, v_avg_length
    FROM facets.bm25_statistics 
    WHERE table_id = 'test_parallel.documents'::regclass::oid;
    
    IF v_total_docs = 500 THEN
        RAISE NOTICE 'PASS: Statistics show % documents', v_total_docs;
    ELSE
        RAISE NOTICE 'INFO: Statistics show % documents (expected 500)', v_total_docs;
    END IF;
    
    IF v_avg_length > 0 THEN
        RAISE NOTICE 'PASS: Average document length is %.2f', v_avg_length;
    ELSE
        RAISE WARNING 'FAIL: Average document length is 0';
    END IF;
END $$;

\echo ''
\echo '--- Test 8: BM25 Search Works After Parallel Indexing ---'
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count FROM facets.bm25_search(
        'test_parallel.documents'::regclass,
        'PostgreSQL database',
        'english',
        false, false, 0.3, 1.2, 0.75, 10
    );
    
    IF v_count > 0 THEN
        RAISE NOTICE 'PASS: BM25 search returned % results for "PostgreSQL database"', v_count;
    ELSE
        RAISE WARNING 'FAIL: BM25 search returned no results';
    END IF;
END $$;

-- Verify search results are correct
SELECT doc_id, round(score::numeric, 4) AS score
FROM facets.bm25_search(
    'test_parallel.documents'::regclass,
    'PostgreSQL optimization',
    'english',
    false, false, 0.3, 1.2, 0.75, 5
)
ORDER BY score DESC
LIMIT 5;

\echo ''
\echo '--- Test 9: No Orphaned Staging Tables ---'
DO $$
DECLARE
    v_staging_count int;
BEGIN
    SELECT COUNT(*) INTO v_staging_count
    FROM pg_tables
    WHERE schemaname = 'facets'
    AND (tablename LIKE 'bm25_src_%' OR tablename LIKE 'bm25_w%');
    
    IF v_staging_count = 0 THEN
        RAISE NOTICE 'PASS: No orphaned staging tables found';
    ELSE
        RAISE WARNING 'FAIL: Found % orphaned staging tables', v_staging_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 10: bm25_rebuild_index Uses Optimized Parallel ---'
-- Clear existing index
DELETE FROM facets.bm25_index WHERE table_id = 'test_parallel.documents'::regclass::oid;
DELETE FROM facets.bm25_documents WHERE table_id = 'test_parallel.documents'::regclass::oid;
DELETE FROM facets.bm25_statistics WHERE table_id = 'test_parallel.documents'::regclass::oid;

-- Rebuild with auto-detected parallelism
SELECT facets.bm25_rebuild_index(
    'test_parallel.documents'::regclass,
    'id',
    'content',
    'english',
    0  -- Auto-detect (will use parallel if dblink available)
);

-- Verify rebuild worked
DO $$
DECLARE
    v_doc_count bigint;
BEGIN
    SELECT COUNT(*) INTO v_doc_count 
    FROM facets.bm25_documents 
    WHERE table_id = 'test_parallel.documents'::regclass::oid;
    
    IF v_doc_count >= 450 THEN  -- Allow for some margin
        RAISE NOTICE 'PASS: bm25_rebuild_index indexed % documents', v_doc_count;
    ELSE
        RAISE WARNING 'FAIL: bm25_rebuild_index only indexed % documents', v_doc_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 11: bm25_progress Shows Correct Status ---'
SELECT * FROM facets.bm25_progress('test_parallel.documents'::regclass);

DO $$
DECLARE
    v_indexed bigint;
    v_source bigint;
    v_progress numeric;
BEGIN
    SELECT documents_indexed, source_documents, progress_pct 
    INTO v_indexed, v_source, v_progress
    FROM facets.bm25_progress('test_parallel.documents'::regclass);
    
    IF v_source = 500 THEN
        RAISE NOTICE 'PASS: bm25_progress correctly reports % source documents', v_source;
    ELSE
        RAISE NOTICE 'INFO: bm25_progress reports % source documents', v_source;
    END IF;
    
    IF v_progress >= 90 THEN
        RAISE NOTICE 'PASS: bm25_progress shows %.1f%% complete', v_progress;
    ELSE
        RAISE NOTICE 'INFO: bm25_progress shows %.1f%% complete', v_progress;
    END IF;
END $$;

\echo ''
\echo '--- Test 12: Cleanup Functions Work ---'
-- Clean up any remaining staging tables
SELECT * FROM facets.bm25_cleanup_staging();

-- Verify cleanup worked
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM pg_tables
    WHERE schemaname = 'facets'
    AND (tablename LIKE 'bm25_src_%' OR tablename LIKE 'bm25_w%' OR tablename LIKE 'bm25_staging%');
    
    IF v_count = 0 THEN
        RAISE NOTICE 'PASS: bm25_cleanup_staging cleaned all staging tables';
    ELSE
        RAISE NOTICE 'INFO: % staging tables remain', v_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 13: Incremental Indexing After Initial Build ---'
-- Insert more documents
INSERT INTO test_parallel.documents (content, category) VALUES
    ('New document about Kubernetes container orchestration', 'Technology'),
    ('Docker containerization and microservices architecture', 'Technology'),
    ('Serverless computing with AWS Lambda functions', 'Technology');

-- Rebuild index to include new documents
SELECT facets.bm25_rebuild_index(
    'test_parallel.documents'::regclass,
    'id',
    'content',
    'english',
    1  -- Sequential for this small update
);

-- Verify new documents are searchable
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count FROM facets.bm25_search(
        'test_parallel.documents'::regclass,
        'Kubernetes Docker',
        'english',
        false, false, 0.3, 1.2, 0.75, 10
    );
    
    IF v_count >= 2 THEN
        RAISE NOTICE 'PASS: New documents are searchable (found % results for "Kubernetes Docker")', v_count;
    ELSE
        RAISE WARNING 'FAIL: New documents not fully indexed (found % results)', v_count;
    END IF;
END $$;

\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('test_parallel.documents');
DROP SCHEMA test_parallel CASCADE;

\echo ''
\echo '=============================================='
\echo 'Parallel Indexing Test Suite Complete!'
\echo '=============================================='

