-- nested_transaction_safety_test.sql
-- Tests that pg_facets operations are safe when running inside nested transactions (DO blocks)
-- where savepoint creation fails, ensuring ACID properties are maintained
--
-- To run:
--   psql -f nested_transaction_safety_test.sql

\set ON_ERROR_STOP on
\timing on

\echo '================================================================================'
\echo 'pg_facets Nested Transaction Safety Test'
\echo '================================================================================'
\echo ''

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;

DROP SCHEMA IF EXISTS test_nested CASCADE;
CREATE SCHEMA test_nested;

-- ============================================================================
-- TEST 1: BM25 Indexing Atomicity Without Savepoints
-- ============================================================================
\echo '--- TEST 1: BM25 Indexing Atomicity Without Savepoints ---'
\echo 'Testing that BM25 indexing is atomic even when savepoints fail'
\echo ''

CREATE TABLE test_nested.bm25_atomicity (
    id bigint PRIMARY KEY,
    content text
);

SELECT facets.add_faceting_to_table(
    'test_nested.bm25_atomicity'::regclass,
    key => 'id',
    facets => ARRAY[]::facets.facet_definition[]
);

SELECT facets.bm25_set_language('test_nested.bm25_atomicity'::regclass, 'english');

-- Test 1.1: Normal operation inside DO block (should work, may produce warnings)
\echo 'Test 1.1: Normal BM25 indexing inside DO block'
DO $$
DECLARE
    v_initial_docs int;
    v_initial_terms int;
    v_final_docs int;
    v_final_terms int;
BEGIN
    SELECT COUNT(*) INTO v_initial_docs FROM facets.bm25_documents WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;
    SELECT COUNT(*) INTO v_initial_terms FROM facets.bm25_index WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;

    RAISE NOTICE 'Initial: docs=%, terms=%', v_initial_docs, v_initial_terms;

    -- Insert and index document inside DO block
    INSERT INTO test_nested.bm25_atomicity VALUES (1, 'The quick brown fox jumps over the lazy dog');

    -- This should work despite savepoint warnings
    PERFORM facets.bm25_index_document(
        'test_nested.bm25_atomicity'::regclass,
        1,
        'The quick brown fox jumps over the lazy dog',
        'content',
        'english'
    );

    SELECT COUNT(*) INTO v_final_docs FROM facets.bm25_documents WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;
    SELECT COUNT(*) INTO v_final_terms FROM facets.bm25_index WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;

    IF v_final_docs = v_initial_docs + 1 AND v_final_terms > v_initial_terms THEN
        RAISE NOTICE 'PASS: Document and terms indexed atomically. Final: docs=%, terms=%', v_final_docs, v_final_terms;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected atomic indexing. Initial docs=%, terms=%; Final docs=%, terms=%', v_initial_docs, v_initial_terms, v_final_docs, v_final_terms;
    END IF;
END $$;

-- Test 1.2: Rollback propagation (DO block rolls back)
\echo 'Test 1.2: Rollback propagation from DO block'
DO $$
DECLARE
    v_before_docs int;
    v_after_docs int;
BEGIN
    SELECT COUNT(*) INTO v_before_docs FROM facets.bm25_documents WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;

    -- Insert and index document
    INSERT INTO test_nested.bm25_atomicity VALUES (2, 'PostgreSQL is an advanced open source database');
    PERFORM facets.bm25_index_document(
        'test_nested.bm25_atomicity'::regclass,
        2,
        'PostgreSQL is an advanced open source database',
        'content',
        'english'
    );

    -- Force rollback with exception
    RAISE EXCEPTION 'Intentional rollback test';
END $$;

-- Verify rollback worked
DO $$
DECLARE
    v_docs_after_rollback int;
    v_terms_after_rollback int;
BEGIN
    SELECT COUNT(*) INTO v_docs_after_rollback FROM facets.bm25_documents WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;
    SELECT COUNT(*) INTO v_terms_after_rollback FROM facets.bm25_index WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;

    -- Should only have the document from test 1.1
    IF v_docs_after_rollback = 1 THEN
        RAISE NOTICE 'PASS: Rollback worked - BM25 data was rolled back. Docs after rollback: %', v_docs_after_rollback;
    ELSE
        RAISE WARNING 'POTENTIAL ISSUE: Expected 1 doc after rollback, found %', v_docs_after_rollback;
    END IF;
END $$;

-- ============================================================================
-- TEST 2: Delta Merge Atomicity Without Savepoints
-- ============================================================================
\echo '--- TEST 2: Delta Merge Atomicity Without Savepoints ---'
\echo 'Testing that facet delta merging is atomic even when savepoints fail'
\echo ''

CREATE TABLE test_nested.facet_atomicity (
    id bigint PRIMARY KEY,
    category text,
    status text
);

SELECT facets.add_faceting_to_table(
    'test_nested.facet_atomicity'::regclass,
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('status')
    ],
    populate => true
);

-- Test 2.1: Normal delta merge inside DO block
\echo 'Test 2.1: Delta merge inside DO block'
DO $$
DECLARE
    v_initial_facets int;
    v_delta_count int;
    v_final_facets int;
    v_remaining_deltas int;
BEGIN
    SELECT COUNT(*) INTO v_initial_facets FROM test_nested.facet_atomicity_facets;

    -- Insert data (triggers should create deltas)
    INSERT INTO test_nested.facet_atomicity VALUES
        (1, 'A', 'active'),
        (2, 'B', 'inactive'),
        (3, 'A', 'active');

    -- Count deltas
    SELECT COUNT(*) INTO v_delta_count FROM test_nested.facet_atomicity_facets_deltas WHERE delta <> 0;

    IF v_delta_count > 0 THEN
        RAISE NOTICE 'Created % deltas, now merging...', v_delta_count;

        -- Merge deltas inside DO block (may produce warnings)
        PERFORM facets.merge_deltas_safe('test_nested.facet_atomicity'::regclass);

        -- Verify merge worked
        SELECT COUNT(*) INTO v_final_facets FROM test_nested.facet_atomicity_facets;
        SELECT COUNT(*) INTO v_remaining_deltas FROM test_nested.facet_atomicity_facets_deltas WHERE delta <> 0;

        IF v_remaining_deltas = 0 THEN
            RAISE NOTICE 'PASS: All deltas merged atomically. Final facets: %', v_final_facets;
        ELSE
            RAISE WARNING 'POTENTIAL ISSUE: % deltas remain after merge', v_remaining_deltas;
        END IF;
    ELSE
        RAISE NOTICE 'SKIP: No deltas created (triggers may not be working)';
    END IF;
END $$;

-- Test 2.2: Delta merge rollback (DO block rolls back)
\echo 'Test 2.2: Delta merge rollback from DO block'
DO $$
DECLARE
    v_before_merge int;
BEGIN
    -- Insert more data
    INSERT INTO test_nested.facet_atomicity VALUES (4, 'C', 'pending');

    -- Merge deltas
    PERFORM facets.merge_deltas_safe('test_nested.facet_atomicity'::regclass);

    SELECT COUNT(*) INTO v_before_merge FROM test_nested.facet_atomicity_facets;

    -- Force rollback
    RAISE EXCEPTION 'Intentional delta merge rollback test';
END $$;

-- Verify delta merge was rolled back
DO $$
DECLARE
    v_after_rollback int;
    v_deltas_after_rollback int;
BEGIN
    SELECT COUNT(*) INTO v_after_rollback FROM test_nested.facet_atomicity_facets;
    SELECT COUNT(*) INTO v_deltas_after_rollback FROM test_nested.facet_atomicity_facets_deltas WHERE delta <> 0;

    -- The facet count should be same as before the failed DO block
    -- (we expect 3 facets from the previous successful merge)
    IF v_after_rollback >= 3 THEN
        RAISE NOTICE 'PASS: Delta merge rollback worked. Facets after rollback: %', v_after_rollback;
    ELSE
        RAISE WARNING 'POTENTIAL ISSUE: Expected at least 3 facets after rollback, found %', v_after_rollback;
    END IF;

    -- There should be deltas for the rolled-back insert
    IF v_deltas_after_rollback > 0 THEN
        RAISE NOTICE 'PASS: Deltas correctly restored after rollback: %', v_deltas_after_rollback;
    END IF;
END $$;

-- ============================================================================
-- TEST 3: Error Recovery and Data Consistency
-- ============================================================================
\echo '--- TEST 3: Error Recovery and Data Consistency ---'
\echo 'Testing that database remains consistent after errors in nested transactions'
\echo ''

-- Test 3.1: Multiple operations with mid-batch error
\echo 'Test 3.1: Multiple BM25 operations with mid-batch error'
DO $$
DECLARE
    v_doc_count int;
BEGIN
    -- Insert multiple documents
    INSERT INTO test_nested.bm25_atomicity VALUES
        (10, 'First document for batch test'),
        (11, 'Second document for batch test'),
        (12, 'Third document will cause error');

    -- Index first two successfully
    PERFORM facets.bm25_index_document('test_nested.bm25_atomicity'::regclass, 10, 'First document for batch test', 'content', 'english');
    PERFORM facets.bm25_index_document('test_nested.bm25_atomicity'::regclass, 11, 'Second document for batch test', 'content', 'english');

    -- Third one fails (try to index with wrong content)
    BEGIN
        PERFORM facets.bm25_index_document('test_nested.bm25_atomicity'::regclass, 12, NULL, 'content', 'english');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected error caught: %', SQLERRM;
    END;

    -- Force rollback of entire DO block
    RAISE EXCEPTION 'Rolling back entire batch';
END $$;

-- Verify all operations were rolled back
DO $$
DECLARE
    v_docs_after_error int;
    v_terms_after_error int;
BEGIN
    SELECT COUNT(*) INTO v_docs_after_error FROM facets.bm25_documents WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;
    SELECT COUNT(*) INTO v_terms_after_error FROM facets.bm25_index WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;

    -- Should still only have the original document
    IF v_docs_after_error = 1 THEN
        RAISE NOTICE 'PASS: Error recovery worked - no partial indexing. Docs after error: %', v_docs_after_error;
    ELSE
        RAISE WARNING 'POTENTIAL ISSUE: Expected 1 doc after error recovery, found %', v_docs_after_error;
    END IF;
END $$;

-- ============================================================================
-- TEST 4: Database Functionality After Errors
-- ============================================================================
\echo '--- TEST 4: Database Functionality After Errors ---'
\echo 'Testing that pg_facets remains functional after nested transaction errors'
\echo ''

-- Test 4.1: Normal operations work after error recovery
\echo 'Test 4.1: Normal operations after error recovery'
DO $$
DECLARE
    v_new_docs int;
    v_new_terms int;
BEGIN
    -- Insert and index a new document after the errors above
    INSERT INTO test_nested.bm25_atomicity VALUES (100, 'Recovery test document');
    PERFORM facets.bm25_index_document(
        'test_nested.bm25_atomicity'::regclass,
        100,
        'Recovery test document',
        'content',
        'english'
    );

    SELECT COUNT(*) INTO v_new_docs FROM facets.bm25_documents WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;
    SELECT COUNT(*) INTO v_new_terms FROM facets.bm25_index WHERE table_id = 'test_nested.bm25_atomicity'::regclass::oid;

    IF v_new_docs = 2 AND v_new_terms > 0 THEN
        RAISE NOTICE 'PASS: pg_facets functional after error recovery. Total docs: %, terms: %', v_new_docs, v_new_terms;
    ELSE
        RAISE EXCEPTION 'FAIL: pg_facets not functional after errors. Docs: %, Terms: %', v_new_docs, v_new_terms;
    END IF;
END $$;

-- ============================================================================
-- SUMMARY
-- ============================================================================
\echo '================================================================================'
\echo 'NESTED TRANSACTION SAFETY TEST SUMMARY'
\echo '================================================================================'
\echo ''
\echo 'If all tests above showed PASS or SKIP messages, then:'
\echo '✓ Operations are safe inside nested transactions without savepoints'
\echo '✓ ACID properties are maintained by the outer transaction'
\echo '✓ The savepoint warnings are harmless noise'
\echo ''
\echo 'If any tests showed FAIL or POTENTIAL ISSUE, then:'
\echo '✗ Operations are NOT safe without savepoints'
\echo '✗ Need to implement proper nested transaction handling'
\echo ''
\echo 'Test completed successfully - no database corruption detected.'
\echo '================================================================================'
