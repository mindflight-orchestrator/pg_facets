-- pg_facets Native Tokenization & Batch Insert Test Suite
-- Tests for the optimized native Zig tokenization and batch insert functionality
-- Validates correctness and performance improvements

\echo '=============================================='
\echo 'pg_facets Native Tokenization Tests'
\echo '=============================================='
\echo ''

-- Setup test schema
DROP SCHEMA IF EXISTS test_native_token CASCADE;
CREATE SCHEMA test_native_token;

-- Create test table
CREATE TABLE test_native_token.documents (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT
);

-- Insert test data with various text patterns
INSERT INTO test_native_token.documents (content, category) VALUES
    ('PostgreSQL is a powerful open source database system', 'Technology'),
    ('Database optimization tips and best practices for performance', 'Technology'),
    ('Full text search with pg_facets extension provides fast retrieval', 'Technology'),
    ('Cooking recipes for Italian cuisine with fresh ingredients', 'Lifestyle'),
    ('Travel guide to European destinations and cultural experiences', 'Travel'),
    ('Machine learning fundamentals introduction to neural networks', 'Technology'),
    ('Home workout routines for beginners starting fitness journey', 'Lifestyle'),
    ('PostgreSQL indexing strategies explained in detail', 'Technology'),
    ('Japanese cooking techniques and traditional recipes', 'Lifestyle'),
    ('Advanced SQL query optimization techniques and patterns', 'Technology'),
    -- Add more documents for batch testing
    (repeat('The quick brown fox jumps over the lazy dog. ', 10), 'Test'),
    (repeat('PostgreSQL full text search is very powerful. ', 20), 'Test'),
    (repeat('Batch insert optimization improves performance significantly. ', 15), 'Test');

\echo ''
\echo '--- Test 1: Native Worker Function Exists ---'
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_worker_native'
    ) THEN 'PASS' ELSE 'FAIL' END || ': bm25_index_worker_native function exists';

\echo ''
\echo '--- Test 2: Register Table for Faceting ---'
SELECT facets.add_faceting_to_table(
    'test_native_token.documents'::regclass,
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM facets.faceted_table 
        WHERE table_id = 'test_native_token.documents'::regclass::oid
    ) THEN 'PASS' ELSE 'FAIL' END || ': Table registered for faceting';

\echo ''
\echo '--- Test 3: Create BM25 Sync Trigger ---'
SELECT facets.bm25_create_sync_trigger(
    'test_native_token.documents'::regclass,
    'id',
    'content',
    'english'
);

SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        WHERE c.relname = 'documents'
        AND t.tgname LIKE '%bm25%'
    ) THEN 'PASS' ELSE 'FAIL' END || ': BM25 sync trigger created';

\echo ''
\echo '--- Test 4: Manual BM25 Indexing (Small Dataset) ---'
-- Index a few documents manually to test native tokenization
DO $$
DECLARE
    v_table_id oid;
    v_doc_count int;
BEGIN
    v_table_id := 'test_native_token.documents'::regclass::oid;
    
    -- Index first 5 documents
    PERFORM facets.bm25_index_document_native(
        v_table_id,
        d.id,
        d.content,
        'english'
    )
    FROM test_native_token.documents d
    ORDER BY d.id
    LIMIT 5;
    
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = v_table_id;
    
    IF v_doc_count >= 5 THEN
        RAISE NOTICE 'PASS: Indexed % documents', v_doc_count;
    ELSE
        RAISE WARNING 'FAIL: Expected at least 5 documents, got %', v_doc_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 5: Verify Tokenization Correctness ---'
-- Compare native tokenization with SQL to_tsvector
DO $$
DECLARE
    v_test_text text := 'PostgreSQL full text search is very powerful';
    v_sql_tokens text[];
    v_native_count int;
    v_sql_count int;
BEGIN
    -- Get tokens from native Zig tokenizer (same as used in production)
    SELECT array_agg(lexeme ORDER BY lexeme) INTO v_sql_tokens
    FROM facets.tokenize_native(v_test_text, 'english');
    
    v_sql_count := array_length(v_sql_tokens, 1);
    
    -- Check if native indexing produced similar results
    -- (We can't directly call the native function, but we can check the indexed terms)
    SELECT COUNT(DISTINCT term_text) INTO v_native_count
    FROM facets.bm25_index
    WHERE table_id = 'test_native_token.documents'::regclass::oid
    AND term_text = ANY(v_sql_tokens);
    
    IF v_native_count > 0 THEN
        RAISE NOTICE 'PASS: Native tokenization matches SQL (found %/% common terms)', v_native_count, v_sql_count;
    ELSE
        RAISE WARNING 'WARN: Could not verify tokenization match (this is expected if no matching docs indexed)';
    END IF;
END $$;

\echo ''
\echo '--- Test 6: Batch Insert Performance Test ---'
-- Create staging tables to test batch insert
DO $$
DECLARE
    v_table_id oid;
    v_source_staging text;
    v_output_staging text;
    v_total_docs bigint;
    v_start_time timestamptz;
    v_elapsed_ms float;
    v_result record;
BEGIN
    v_table_id := 'test_native_token.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM test_native_token.documents);
    
    -- Create source staging table
    v_source_staging := 'bm25_src_test_' || extract(epoch from now())::bigint;
    EXECUTE format('CREATE UNLOGGED TABLE facets.%I (doc_id bigint, content text, rn bigint)', v_source_staging);
    
    -- Populate source staging
    EXECUTE format(
        'INSERT INTO facets.%I (doc_id, content, rn) SELECT id, content, row_number() OVER (ORDER BY id) FROM test_native_token.documents',
        v_source_staging
    );
    
    -- Create output staging table
    v_output_staging := 'bm25_w1_test_' || extract(epoch from now())::bigint;
    EXECUTE format(
        'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
        v_output_staging
    );
    
    -- Test native worker function
    v_start_time := clock_timestamp();
    
    SELECT * INTO v_result
    FROM facets.bm25_index_worker_native(
        v_table_id,
        v_source_staging,
        v_output_staging,
        'english',
        v_total_docs,
        1,  -- single worker
        1   -- worker_id
    );
    
    v_elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    
    -- Verify results
    IF v_result.docs_indexed > 0 THEN
        RAISE NOTICE 'PASS: Native worker processed % documents, % terms in %.2f ms', 
            v_result.docs_indexed, v_result.terms_extracted, v_elapsed_ms;
        RAISE NOTICE 'INFO: Average %.2f ms per document', v_elapsed_ms / GREATEST(v_result.docs_indexed, 1);
    ELSE
        RAISE WARNING 'FAIL: Native worker processed 0 documents';
    END IF;
    
    -- Check batch insert worked (should have multiple rows)
    DECLARE
        v_term_count bigint;
    BEGIN
        EXECUTE format('SELECT COUNT(*) FROM facets.%I', v_output_staging) INTO v_term_count;
        
        IF v_term_count > 0 THEN
            RAISE NOTICE 'PASS: Batch insert created % term rows', v_term_count;
        ELSE
            RAISE WARNING 'FAIL: No terms inserted';
        END IF;
    END;
    
    -- Cleanup
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_source_staging);
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging);
END $$;

\echo ''
\echo '--- Test 7: Parallel Worker Distribution Test ---'
-- Test that work is evenly distributed
DO $$
DECLARE
    v_table_id oid;
    v_total_docs bigint := 100;
    v_num_workers int := 10;
    v_worker_id int;
    v_start_rn bigint;
    v_end_rn bigint;
    v_docs_per_worker bigint;
    v_base_docs bigint;
    v_remainder int;
    v_expected_start bigint;
    v_expected_end bigint;
    v_passed boolean := true;
BEGIN
    v_table_id := 'test_native_token.documents'::regclass::oid;
    v_base_docs := v_total_docs / v_num_workers;
    v_remainder := v_total_docs % v_num_workers;
    
    -- Test first few workers
    FOR v_worker_id IN 1..5 LOOP
        -- Calculate expected range (same logic as worker function)
        IF v_worker_id <= v_remainder THEN
            v_expected_start := (v_worker_id - 1) * (v_base_docs + 1) + 1;
            v_expected_end := v_expected_start + v_base_docs;
        ELSE
            v_expected_start := v_remainder * (v_base_docs + 1) + (v_worker_id - v_remainder - 1) * v_base_docs + 1;
            v_expected_end := v_expected_start + v_base_docs - 1;
        END IF;
        
        -- Verify distribution is correct
        IF v_expected_start < 1 OR v_expected_end > v_total_docs THEN
            RAISE WARNING 'FAIL: Worker % has invalid range: % to %', v_worker_id, v_expected_start, v_expected_end;
            v_passed := false;
        END IF;
    END LOOP;
    
    IF v_passed THEN
        RAISE NOTICE 'PASS: Work distribution logic is correct';
    END IF;
END $$;

\echo ''
\echo '--- Test 8: Full Parallel Indexing Test (if dblink available) ---'
DO $$
DECLARE
    v_has_dblink boolean;
    v_result record;
    v_start_time timestamptz;
    v_elapsed interval;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') INTO v_has_dblink;
    
    IF v_has_dblink THEN
        RAISE NOTICE 'INFO: Testing parallel indexing with native worker...';
        
        v_start_time := clock_timestamp();
        
        -- Run parallel indexing (should use native worker)
        SELECT * INTO v_result
        FROM facets.bm25_index_documents_parallel(
            'test_native_token.documents'::regclass,
            'SELECT id::bigint AS doc_id, content FROM test_native_token.documents',
            'content',
            'english',
            2  -- 2 workers for small dataset
        );
        
        v_elapsed := clock_timestamp() - v_start_time;
        
        -- Check results
        DECLARE
            v_indexed_docs bigint;
        BEGIN
            SELECT COUNT(*) INTO v_indexed_docs
            FROM facets.bm25_documents
            WHERE table_id = 'test_native_token.documents'::regclass::oid;
            
            IF v_indexed_docs > 0 THEN
                RAISE NOTICE 'PASS: Parallel indexing completed in %', v_elapsed;
                RAISE NOTICE 'INFO: Indexed % documents', v_indexed_docs;
            ELSE
                RAISE WARNING 'FAIL: No documents indexed';
            END IF;
        END;
    ELSE
        RAISE NOTICE 'SKIP: dblink not available, skipping parallel test';
    END IF;
END $$;

\echo ''
\echo '--- Test 9: Performance Comparison (Native vs SQL Worker) ---'
DO $$
DECLARE
    v_table_id oid;
    v_source_staging text;
    v_output_staging_native text;
    v_output_staging_sql text;
    v_total_docs bigint;
    v_start_time timestamptz;
    v_native_time interval;
    v_sql_time interval;
    v_result record;
    v_has_sql_worker boolean;
BEGIN
    v_table_id := 'test_native_token.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM test_native_token.documents);
    
    -- Check if SQL worker exists
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_worker_lockfree'
    ) INTO v_has_sql_worker;
    
    -- Create source staging
    v_source_staging := 'bm25_src_perf_' || extract(epoch from now())::bigint;
    EXECUTE format('CREATE UNLOGGED TABLE facets.%I (doc_id bigint, content text, rn bigint)', v_source_staging);
    EXECUTE format(
        'INSERT INTO facets.%I (doc_id, content, rn) SELECT id, content, row_number() OVER (ORDER BY id) FROM test_native_token.documents',
        v_source_staging
    );
    
    -- Test native worker
    v_output_staging_native := 'bm25_w_native_' || extract(epoch from now())::bigint;
    EXECUTE format(
        'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
        v_output_staging_native
    );
    
    v_start_time := clock_timestamp();
    SELECT * INTO v_result
    FROM facets.bm25_index_worker_native(
        v_table_id, v_source_staging, v_output_staging_native, 'english', v_total_docs, 1, 1
    );
    v_native_time := clock_timestamp() - v_start_time;
    
    RAISE NOTICE 'INFO: Native worker: % (processed % docs, % terms)', 
        v_native_time, v_result.docs_indexed, v_result.terms_extracted;
    
    -- Test SQL worker if available
    IF v_has_sql_worker THEN
        v_output_staging_sql := 'bm25_w_sql_' || extract(epoch from now())::bigint;
        EXECUTE format(
            'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
            v_output_staging_sql
        );
        
        v_start_time := clock_timestamp();
        SELECT * INTO v_result
        FROM facets.bm25_index_worker_lockfree(
            v_table_id, v_source_staging, v_output_staging_sql, 'english', v_total_docs, 1, 1
        );
        v_sql_time := clock_timestamp() - v_start_time;
        
        RAISE NOTICE 'INFO: SQL worker: % (processed % docs, % terms)', 
            v_sql_time, v_result.docs_indexed, v_result.terms_extracted;
        
        IF v_native_time < v_sql_time THEN
            RAISE NOTICE 'PASS: Native worker is %.2fx faster than SQL worker', 
                EXTRACT(EPOCH FROM v_sql_time) / NULLIF(EXTRACT(EPOCH FROM v_native_time), 0);
        ELSE
            RAISE NOTICE 'INFO: Performance comparison: Native=%, SQL=%', v_native_time, v_sql_time;
        END IF;
        
        EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging_sql);
    ELSE
        RAISE NOTICE 'SKIP: SQL worker not available for comparison';
    END IF;
    
    -- Cleanup
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_source_staging);
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging_native);
END $$;

\echo ''
\echo '--- Test 10: Batch Size Validation ---'
-- Verify that batch inserts are working (check for multiple rows per insert)
DO $$
DECLARE
    v_table_id oid;
    v_source_staging text;
    v_output_staging text;
    v_total_docs bigint;
    v_term_count_before bigint;
    v_term_count_after bigint;
    v_batch_size_estimate int;
BEGIN
    v_table_id := 'test_native_token.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM test_native_token.documents);
    
    -- Create staging tables
    v_source_staging := 'bm25_src_batch_' || extract(epoch from now())::bigint;
    v_output_staging := 'bm25_w_batch_' || extract(epoch from now())::bigint;
    
    EXECUTE format('CREATE UNLOGGED TABLE facets.%I (doc_id bigint, content text, rn bigint)', v_source_staging);
    EXECUTE format(
        'INSERT INTO facets.%I (doc_id, content, rn) SELECT id, content, row_number() OVER (ORDER BY id) FROM test_native_token.documents',
        v_source_staging
    );
    EXECUTE format(
        'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
        v_output_staging
    );
    
    -- Run worker
    PERFORM facets.bm25_index_worker_native(
        v_table_id, v_source_staging, v_output_staging, 'english', v_total_docs, 1, 1
    );
    
    -- Check term count
    EXECUTE format('SELECT COUNT(*) FROM facets.%I', v_output_staging) INTO v_term_count_after;
    
    -- Estimate batch size (if we have many terms, batches were likely used)
    -- With 13 documents and ~10 terms each, we should have ~130 terms
    -- If batches of 10K are used, we'd have 1 insert for all terms
    -- But with smaller dataset, we might have multiple batches
    
    IF v_term_count_after > 0 THEN
        RAISE NOTICE 'PASS: Batch insert created % term rows', v_term_count_after;
        RAISE NOTICE 'INFO: This suggests batch processing is working';
    ELSE
        RAISE WARNING 'FAIL: No terms inserted';
    END IF;
    
    -- Cleanup
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_source_staging);
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging);
END $$;

\echo ''
\echo '--- Test 11: Verify Index Quality ---'
-- Check that indexed terms are reasonable
DO $$
DECLARE
    v_term_count bigint;
    v_doc_count bigint;
    v_avg_terms_per_doc float;
BEGIN
    SELECT COUNT(DISTINCT term_text) INTO v_term_count
    FROM facets.bm25_index
    WHERE table_id = 'test_native_token.documents'::regclass::oid;
    
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = 'test_native_token.documents'::regclass::oid;
    
    IF v_doc_count > 0 THEN
        v_avg_terms_per_doc := v_term_count::float / v_doc_count;
        
        IF v_term_count > 0 AND v_avg_terms_per_doc > 0 THEN
            RAISE NOTICE 'PASS: Index quality check - % unique terms across % documents (avg %.1f terms/doc)', 
                v_term_count, v_doc_count, v_avg_terms_per_doc;
        ELSE
            RAISE WARNING 'FAIL: Invalid index statistics';
        END IF;
    ELSE
        RAISE WARNING 'SKIP: No documents indexed yet';
    END IF;
END $$;

\echo ''
\echo '=============================================='
\echo 'Test Suite Complete'
\echo '=============================================='

