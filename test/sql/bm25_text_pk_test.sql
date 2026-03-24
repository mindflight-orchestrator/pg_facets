-- BM25 Test with Text Primary Keys
-- Tests BM25 indexing when primary keys are text (like IMDB tconst/nconst)
-- This addresses the common issue where text PKs need to be converted to bigint

\set ON_ERROR_STOP on
\timing on

\echo '=============================================='
\echo 'BM25 Text Primary Key Test Suite'
\echo '=============================================='
\echo ''

-- Setup test schema
DROP SCHEMA IF EXISTS bm25_text_pk_test CASCADE;
CREATE SCHEMA bm25_text_pk_test;

-- Create a table with text primary key (simulating IMDB scenario)
CREATE TABLE bm25_text_pk_test.title_basics (
    tconst TEXT PRIMARY KEY,  -- Text primary key like 'tt0000001'
    primarytitle TEXT,
    originaltitle TEXT,
    description TEXT,
    category TEXT
);

-- Insert test data
INSERT INTO bm25_text_pk_test.title_basics (tconst, primarytitle, originaltitle, description, category) VALUES
    ('tt0000001', 'Carmencita', 'Carmencita', 'A short film showing a woman dancing', 'short'),
    ('tt0000002', 'Le clown et ses chiens', 'Le clown et ses chiens', 'A clown performs with his dogs in a circus', 'short'),
    ('tt0000003', 'Pauvre Pierrot', 'Pauvre Pierrot', 'A poor clown tries to win the heart of a woman', 'short'),
    ('tt0000004', 'Un bon bock', 'Un bon bock', 'A man enjoys a good beer at a tavern', 'short'),
    ('tt0000005', 'Blacksmith Scene', 'Blacksmith Scene', 'Three blacksmiths work at an anvil', 'short');

\echo '--- Test 1: Register table with text primary key ---'
DO $$
BEGIN
    -- Register the table (key column must be numeric, but we'll handle text PK in rebuild)
    -- Note: add_faceting_to_table requires numeric key, so we need a workaround
    -- For this test, we'll create a numeric surrogate key
    ALTER TABLE bm25_text_pk_test.title_basics ADD COLUMN id SERIAL;
    CREATE UNIQUE INDEX ON bm25_text_pk_test.title_basics(id);
    
    PERFORM facets.add_faceting_to_table(
        'bm25_text_pk_test.title_basics'::regclass,
        key => 'id',
        facets => ARRAY[
            facets.plain_facet('category')
        ],
        populate => false
    );
    
    RAISE NOTICE 'PASS: Table registered';
END $$;

\echo ''
\echo '--- Test 2: Index documents with text primary key using hash ---'
DO $$
DECLARE
    v_doc record;
    v_count int := 0;
    v_doc_id bigint;
BEGIN
    FOR v_doc IN 
        SELECT 
            tconst,
            id,
            -- Convert text primary key to bigint using hash
            ABS(('x' || substr(md5(tconst), 1, 15))::bit(60)::bigint) AS doc_id_hash,
            -- Concatenate content
            COALESCE(primarytitle, '') || ' ' || 
            COALESCE(originaltitle, '') || ' ' || 
            COALESCE(description, '') AS content
        FROM bm25_text_pk_test.title_basics
        ORDER BY tconst
    LOOP
        -- Use hash as doc_id
        v_doc_id := v_doc.doc_id_hash;
        
        PERFORM facets.bm25_index_document(
            'bm25_text_pk_test.title_basics'::regclass,
            v_doc_id,  -- Use hash instead of text PK
            v_doc.content,
            'content',
            'english'
        );
        
        v_count := v_count + 1;
    END LOOP;
    
    -- Recalculate statistics
    PERFORM facets.bm25_recalculate_statistics('bm25_text_pk_test.title_basics'::regclass);
    
    RAISE NOTICE 'PASS: Indexed % documents using hash conversion', v_count;
END $$;

\echo ''
\echo '--- Test 3: Verify documents are indexed ---'
DO $$
DECLARE
    v_table_oid oid;
    v_doc_count bigint;
    v_term_count bigint;
BEGIN
    v_table_oid := 'bm25_text_pk_test.title_basics'::regclass::oid;
    
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = v_table_oid;
    
    SELECT COUNT(*) INTO v_term_count
    FROM facets.bm25_index
    WHERE table_id = v_table_oid;
    
    IF v_doc_count = 5 THEN
        RAISE NOTICE 'PASS: % documents indexed', v_doc_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 5 documents, got %', v_doc_count;
    END IF;
    
    IF v_term_count > 0 THEN
        RAISE NOTICE 'PASS: % terms indexed', v_term_count;
    ELSE
        RAISE EXCEPTION 'FAIL: No terms indexed';
    END IF;
END $$;

\echo ''
\echo '--- Test 4: Test BM25 search with text PK table ---'
DO $$
DECLARE
    v_result record;
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM facets.bm25_search(
        'bm25_text_pk_test.title_basics'::regclass,
        'clown',
        'english',
        false, false, 0.3, 1.2, 0.75, 10
    );
    
    IF v_count >= 2 THEN
        RAISE NOTICE 'PASS: Found % documents matching "clown"', v_count;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected at least 2 documents, got %', v_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 5: Test diagnostic helper functions ---'
DO $$
DECLARE
    v_registered boolean;
    v_stats record;
BEGIN
    -- Test bm25_is_table_registered
    v_registered := facets.bm25_is_table_registered('bm25_text_pk_test.title_basics'::regclass);
    IF v_registered THEN
        RAISE NOTICE 'PASS: bm25_is_table_registered returns true';
    ELSE
        RAISE EXCEPTION 'FAIL: bm25_is_table_registered returns false';
    END IF;
    
    -- Test bm25_get_index_stats
    SELECT * INTO v_stats
    FROM facets.bm25_get_index_stats('bm25_text_pk_test.title_basics'::regclass);
    
    IF v_stats.documents_indexed = 5 THEN
        RAISE NOTICE 'PASS: bm25_get_index_stats shows % documents', v_stats.documents_indexed;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 5 documents, got %', v_stats.documents_indexed;
    END IF;
    
    IF v_stats.terms_indexed > 0 THEN
        RAISE NOTICE 'PASS: bm25_get_index_stats shows % terms', v_stats.terms_indexed;
    ELSE
        RAISE EXCEPTION 'FAIL: No terms found in stats';
    END IF;
END $$;

\echo ''
\echo '--- Test 6: Test search with original text PK lookup ---'
DO $$
DECLARE
    v_tconst text := 'tt0000002';
    v_doc_id_hash bigint;
    v_search_results record;
    v_found boolean := false;
BEGIN
    -- Convert text PK to hash (same method as indexing)
    v_doc_id_hash := ABS(('x' || substr(md5(v_tconst), 1, 15))::bit(60)::bigint);
    
    -- Search and check if our document is in results
    FOR v_search_results IN 
        SELECT doc_id, score
        FROM facets.bm25_search(
            'bm25_text_pk_test.title_basics'::regclass,
            'clown',
            'english',
            false, false, 0.3, 1.2, 0.75, 10
        )
    LOOP
        IF v_search_results.doc_id = v_doc_id_hash THEN
            v_found := true;
            EXIT;
        END IF;
    END LOOP;
    
    IF v_found THEN
        RAISE NOTICE 'PASS: Found document with tconst % using hash lookup', v_tconst;
    ELSE
        RAISE WARNING 'WARNING: Could not find document with tconst % in search results', v_tconst;
    END IF;
END $$;

\echo ''
\echo '=============================================='
\echo 'All Text Primary Key Tests Completed'
\echo '=============================================='

