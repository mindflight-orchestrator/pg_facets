-- Phase 1: Core Infrastructure Tests
-- Tests BM25 tables, indexes, and basic indexing functionality

\echo '=============================================='
\echo 'Phase 1: Core Infrastructure Tests'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;

DROP SCHEMA IF EXISTS bm25_phase1_test CASCADE;
CREATE SCHEMA bm25_phase1_test;

-- Test 1: Verify BM25 tables exist
\echo ''
\echo '--- Test 1.1: Verify BM25 tables exist ---'
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'facets' AND table_name = 'bm25_index') THEN
        RAISE NOTICE 'PASS: facets.bm25_index table exists';
    ELSE
        RAISE EXCEPTION 'FAIL: facets.bm25_index table does not exist';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'facets' AND table_name = 'bm25_documents') THEN
        RAISE NOTICE 'PASS: facets.bm25_documents table exists';
    ELSE
        RAISE EXCEPTION 'FAIL: facets.bm25_documents table does not exist';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'facets' AND table_name = 'bm25_statistics') THEN
        RAISE NOTICE 'PASS: facets.bm25_statistics table exists';
    ELSE
        RAISE EXCEPTION 'FAIL: facets.bm25_statistics table does not exist';
    END IF;
END;
$$;

-- Test 2: Verify indexes exist
\echo ''
\echo '--- Test 1.2: Verify BM25 indexes exist ---'
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'facets' AND indexname = 'bm25_index_term_btree') THEN
        RAISE NOTICE 'PASS: bm25_index_term_btree index exists';
    ELSE
        RAISE EXCEPTION 'FAIL: bm25_index_term_btree index does not exist';
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'facets' AND indexname = 'bm25_documents_table_idx') THEN
        RAISE NOTICE 'PASS: bm25_documents_table_idx index exists';
    ELSE
        RAISE EXCEPTION 'FAIL: bm25_documents_table_idx index does not exist';
    END IF;
END;
$$;

-- Test 3: Create test table and register with faceting
\echo ''
\echo '--- Test 1.3: Create test table and register ---'
CREATE TABLE bm25_phase1_test.documents (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO bm25_phase1_test.documents (title, content, category) VALUES
    ('PostgreSQL Guide', 'PostgreSQL is a powerful database system with advanced features', 'Technology'),
    ('Database Design', 'Learn database design principles and best practices', 'Technology'),
    ('SQL Tutorial', 'Introduction to SQL queries and database operations', 'Education');

SELECT facets.add_faceting_to_table(
    'bm25_phase1_test.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

-- Test 4: Index documents for BM25
\echo ''
\echo '--- Test 1.4: Index documents for BM25 ---'
DO $$
DECLARE
    v_table_id oid;
    v_doc_count int;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'bm25_phase1_test' AND tablename = 'documents';
    
    -- Index first document
    PERFORM facets.bm25_index_document(
        'bm25_phase1_test.documents'::regclass,
        1,
        'PostgreSQL is a powerful database system with advanced features',
        'content',
        'english'
    );
    
    -- Index second document
    PERFORM facets.bm25_index_document(
        'bm25_phase1_test.documents'::regclass,
        2,
        'Learn database design principles and best practices',
        'content',
        'english'
    );
    
    -- Index third document
    PERFORM facets.bm25_index_document(
        'bm25_phase1_test.documents'::regclass,
        3,
        'Introduction to SQL queries and database operations',
        'content',
        'english'
    );
    
    -- Verify documents are indexed
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = v_table_id;
    
    IF v_doc_count = 3 THEN
        RAISE NOTICE 'PASS: All 3 documents indexed successfully';
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 3 indexed documents, found %', v_doc_count;
    END IF;
    
    -- Verify terms are in index
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_index
    WHERE table_id = v_table_id;
    
    IF v_doc_count > 0 THEN
        RAISE NOTICE 'PASS: Found % unique terms in index', v_doc_count;
    ELSE
        RAISE EXCEPTION 'FAIL: No terms found in index';
    END IF;
END;
$$;

-- Test 5: Verify term frequencies are stored
\echo ''
\echo '--- Test 1.5: Verify term frequencies stored ---'
DO $$
DECLARE
    v_term_freqs jsonb;
    v_doc_id bigint;
BEGIN
    SELECT term_freqs INTO v_term_freqs
    FROM facets.bm25_index
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase1_test' AND tablename = 'documents')
      AND term_text = 'postgresql'
    LIMIT 1;
    
    IF v_term_freqs IS NOT NULL THEN
        -- Check if document 1 is in term_freqs
        v_doc_id := (v_term_freqs->>'1')::bigint;
        IF v_doc_id IS NOT NULL THEN
            RAISE NOTICE 'PASS: Term frequencies stored correctly for document 1';
        ELSE
            RAISE EXCEPTION 'FAIL: Document 1 not found in term frequencies';
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: No term frequencies found for "postgresql"';
    END IF;
END;
$$;

-- Test 6: Verify document metadata
\echo ''
\echo '--- Test 1.6: Verify document metadata ---'
DO $$
DECLARE
    v_doc_length int;
    v_total_docs bigint;
    v_avg_length float;
BEGIN
    SELECT doc_length INTO v_doc_length
    FROM facets.bm25_documents
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase1_test' AND tablename = 'documents')
      AND doc_id = 1;
    
    IF v_doc_length > 0 THEN
        RAISE NOTICE 'PASS: Document length stored: % tokens', v_doc_length;
    ELSE
        RAISE EXCEPTION 'FAIL: Document length not stored or is 0';
    END IF;
    
    -- Verify statistics
    SELECT total_documents, avg_document_length INTO v_total_docs, v_avg_length
    FROM facets.bm25_statistics
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase1_test' AND tablename = 'documents');
    
    IF v_total_docs = 3 AND v_avg_length > 0 THEN
        RAISE NOTICE 'PASS: Statistics correct - % documents, avg length: %', v_total_docs, v_avg_length;
    ELSE
        RAISE EXCEPTION 'FAIL: Statistics incorrect - documents: %, avg length: %', v_total_docs, v_avg_length;
    END IF;
END;
$$;

-- Test 7: Verify roaring bitmap storage
\echo ''
\echo '--- Test 1.7: Verify roaring bitmap storage ---'
DO $$
DECLARE
    v_doc_ids roaringbitmap;
    v_cardinality bigint;
BEGIN
    SELECT doc_ids INTO v_doc_ids
    FROM facets.bm25_index
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase1_test' AND tablename = 'documents')
      AND term_text = 'databas'  -- Stemmed form of 'database'
    LIMIT 1;
    
    IF v_doc_ids IS NOT NULL THEN
        v_cardinality := rb_cardinality(v_doc_ids);
        IF v_cardinality >= 2 THEN
            RAISE NOTICE 'PASS: Roaring bitmap contains % documents for term "databas"', v_cardinality;
        ELSE
            RAISE EXCEPTION 'FAIL: Expected at least 2 documents for "databas", found %', v_cardinality;
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: No bitmap found for term "databas"';
    END IF;
END;
$$;

-- Test 8: Delete document from index
\echo ''
\echo '--- Test 1.8: Delete document from index ---'
DO $$
DECLARE
    v_doc_count int;
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'bm25_phase1_test' AND tablename = 'documents';
    
    -- Delete document 3
    PERFORM facets.bm25_delete_document(
        'bm25_phase1_test.documents'::regclass,
        3
    );
    
    -- Verify document is removed
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = v_table_id AND doc_id = 3;
    
    IF v_doc_count = 0 THEN
        RAISE NOTICE 'PASS: Document 3 deleted from index';
    ELSE
        RAISE EXCEPTION 'FAIL: Document 3 still exists in index';
    END IF;
    
    -- Verify statistics updated
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = v_table_id;
    
    IF v_doc_count = 2 THEN
        RAISE NOTICE 'PASS: Statistics updated - % documents remaining', v_doc_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 2 documents, found %', v_doc_count;
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bm25_phase1_test.documents');
DROP SCHEMA bm25_phase1_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Phase 1 Tests Complete!'
\echo '=============================================='
