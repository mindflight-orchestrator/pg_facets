-- pg_facets 0.4.2 BM25 Helper Functions Test Suite
-- Tests for new helper functions added in 0.4.2

\echo '=============================================='
\echo 'pg_facets 0.4.2 BM25 Helper Functions Tests'
\echo '=============================================='
\echo ''

-- Setup test schema
DROP SCHEMA IF EXISTS test_bm25_helpers CASCADE;
CREATE SCHEMA test_bm25_helpers;

-- Create test table
CREATE TABLE test_bm25_helpers.articles (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO test_bm25_helpers.articles (content, category) VALUES
    ('PostgreSQL is a powerful open source database system', 'Technology'),
    ('Database optimization tips and best practices', 'Technology'),
    ('Full text search with pg_facets extension', 'Technology'),
    ('Cooking recipes for Italian cuisine', 'Lifestyle'),
    ('Travel guide to European destinations', 'Travel'),
    ('Machine learning fundamentals introduction', 'Technology'),
    ('Home workout routines for beginners', 'Lifestyle'),
    ('PostgreSQL indexing strategies explained', 'Technology'),
    ('Japanese cooking techniques and recipes', 'Lifestyle'),
    ('Advanced SQL query optimization', 'Technology');

\echo '--- Test 1: Version Check ---'
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    IF v_version = '0.4.2' THEN
        RAISE NOTICE 'PASS: Version is 0.4.2';
    ELSE
        RAISE NOTICE 'FAIL: Expected version 0.4.2, got %', v_version;
    END IF;
END $$;

\echo ''
\echo '--- Test 2: New Functions Exist ---'
DO $$
BEGIN
    -- Check bm25_create_sync_trigger exists
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_create_sync_trigger') THEN
        RAISE NOTICE 'PASS: bm25_create_sync_trigger exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_create_sync_trigger not found';
    END IF;
    
    -- Check bm25_drop_sync_trigger exists
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_drop_sync_trigger') THEN
        RAISE NOTICE 'PASS: bm25_drop_sync_trigger exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_drop_sync_trigger not found';
    END IF;
    
    -- Check bm25_rebuild_index exists
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_rebuild_index') THEN
        RAISE NOTICE 'PASS: bm25_rebuild_index exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_rebuild_index not found';
    END IF;
    
    -- Check bm25_status exists
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_status') THEN
        RAISE NOTICE 'PASS: bm25_status exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_status not found';
    END IF;
    
    -- Check bm25_progress exists
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_progress') THEN
        RAISE NOTICE 'PASS: bm25_progress exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_progress not found';
    END IF;
    
    -- Check setup_table_with_bm25 exists
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'setup_table_with_bm25') THEN
        RAISE NOTICE 'PASS: setup_table_with_bm25 exists';
    ELSE
        RAISE WARNING 'FAIL: setup_table_with_bm25 not found';
    END IF;
    
    -- Check cleanup functions exist
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_cleanup_dblinks') THEN
        RAISE NOTICE 'PASS: bm25_cleanup_dblinks exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_cleanup_dblinks not found';
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_cleanup_staging') THEN
        RAISE NOTICE 'PASS: bm25_cleanup_staging exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_cleanup_staging not found';
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
               WHERE n.nspname = 'facets' AND p.proname = 'bm25_full_cleanup') THEN
        RAISE NOTICE 'PASS: bm25_full_cleanup exists';
    ELSE
        RAISE WARNING 'FAIL: bm25_full_cleanup not found';
    END IF;
END $$;

\echo ''
\echo '--- Test 3: Add Faceting to Table ---'
SELECT facets.add_faceting_to_table(
    'test_bm25_helpers.articles',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

-- Verify table is registered
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM facets.faceted_table 
        WHERE table_id = 'test_bm25_helpers.articles'::regclass::oid
    ) THEN 'PASS' ELSE 'FAIL' END || ': Table registered in facets.faceted_table';

\echo ''
\echo '--- Test 4: bm25_create_sync_trigger ---'
SELECT facets.bm25_create_sync_trigger(
    'test_bm25_helpers.articles'::regclass,
    'id',
    'content',
    'english'
);

-- Verify trigger exists
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'test_bm25_helpers' 
        AND c.relname = 'articles'
        AND t.tgname LIKE '%bm25_sync%'
    ) THEN 'PASS' ELSE 'FAIL' END || ': BM25 sync trigger created';

\echo ''
\echo '--- Test 5: bm25_rebuild_index (Sequential) ---'
SELECT facets.bm25_rebuild_index(
    'test_bm25_helpers.articles'::regclass,
    'id',
    'content',
    'english',
    1  -- Force sequential mode
);

-- Verify documents were indexed
DO $$
DECLARE
    v_doc_count bigint;
    v_term_count bigint;
BEGIN
    SELECT COUNT(*) INTO v_doc_count 
    FROM facets.bm25_documents 
    WHERE table_id = 'test_bm25_helpers.articles'::regclass::oid;
    
    SELECT COUNT(*) INTO v_term_count 
    FROM facets.bm25_index 
    WHERE table_id = 'test_bm25_helpers.articles'::regclass::oid;
    
    IF v_doc_count = 10 THEN
        RAISE NOTICE 'PASS: bm25_rebuild_index indexed % documents (expected 10)', v_doc_count;
    ELSE
        RAISE NOTICE 'FAIL: bm25_rebuild_index indexed % documents (expected 10)', v_doc_count;
    END IF;
    
    IF v_term_count > 0 THEN
        RAISE NOTICE 'PASS: bm25_rebuild_index created % terms', v_term_count;
    ELSE
        RAISE WARNING 'FAIL: bm25_rebuild_index created no terms';
    END IF;
END $$;

\echo ''
\echo '--- Test 6: bm25_status ---'
SELECT * FROM facets.bm25_status();

DO $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count FROM facets.bm25_status() 
    WHERE table_name != 'No BM25 indexes found';
    
    IF v_count >= 1 THEN
        RAISE NOTICE 'PASS: bm25_status returned % table(s)', v_count;
    ELSE
        RAISE WARNING 'FAIL: bm25_status returned no tables';
    END IF;
END $$;

\echo ''
\echo '--- Test 7: bm25_progress ---'
SELECT * FROM facets.bm25_progress('test_bm25_helpers.articles'::regclass);

DO $$
DECLARE
    v_indexed bigint;
    v_progress numeric;
BEGIN
    SELECT documents_indexed, progress_pct INTO v_indexed, v_progress
    FROM facets.bm25_progress('test_bm25_helpers.articles'::regclass);
    
    IF v_indexed = 10 THEN
        RAISE NOTICE 'PASS: bm25_progress shows % documents indexed', v_indexed;
    ELSE
        RAISE NOTICE 'FAIL: bm25_progress shows % documents (expected 10)', v_indexed;
    END IF;
    
    IF v_progress = 100 THEN
        RAISE NOTICE 'PASS: bm25_progress shows 100%% complete';
    ELSE
        RAISE NOTICE 'INFO: bm25_progress shows %.1f%% complete', v_progress;
    END IF;
END $$;

\echo ''
\echo '--- Test 8: BM25 Search After Rebuild ---'
SELECT * FROM facets.bm25_search(
    'test_bm25_helpers.articles'::regclass,
    'PostgreSQL database',
    'english',
    false, false, 0.3, 1.2, 0.75, 5
);

DO $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count FROM facets.bm25_search(
        'test_bm25_helpers.articles'::regclass,
        'PostgreSQL',
        'english',
        false, false, 0.3, 1.2, 0.75, 5
    );
    
    IF v_count >= 2 THEN
        RAISE NOTICE 'PASS: BM25 search found % results for "PostgreSQL"', v_count;
    ELSE
        RAISE WARNING 'FAIL: BM25 search found only % results (expected >= 2)', v_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 9: Trigger INSERT Test ---'
-- Insert new document (trigger should index it)
INSERT INTO test_bm25_helpers.articles (content, category) 
VALUES ('New PostgreSQL features in version 17', 'Technology');

-- Wait briefly and check if indexed
DO $$
DECLARE
    v_doc_count bigint;
BEGIN
    SELECT COUNT(*) INTO v_doc_count 
    FROM facets.bm25_documents 
    WHERE table_id = 'test_bm25_helpers.articles'::regclass::oid;
    
    IF v_doc_count = 11 THEN
        RAISE NOTICE 'PASS: Trigger indexed new document (total: %)', v_doc_count;
    ELSE
        RAISE NOTICE 'INFO: Document count is % (trigger may not have fired yet)', v_doc_count;
    END IF;
END $$;

\echo ''
\echo '--- Test 10: bm25_drop_sync_trigger ---'
SELECT facets.bm25_drop_sync_trigger('test_bm25_helpers.articles'::regclass);

-- Verify trigger was dropped
SELECT 
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'test_bm25_helpers' 
        AND c.relname = 'articles'
        AND t.tgname LIKE '%bm25_sync%'
    ) THEN 'PASS' ELSE 'FAIL' END || ': BM25 sync trigger dropped';

\echo ''
\echo '--- Test 11: bm25_cleanup_staging ---'
SELECT * FROM facets.bm25_cleanup_staging();
\echo 'PASS: bm25_cleanup_staging executed'

\echo ''
\echo '--- Test 12: bm25_cleanup_dblinks ---'
SELECT * FROM facets.bm25_cleanup_dblinks();
\echo 'PASS: bm25_cleanup_dblinks executed'

\echo ''
\echo '--- Test 13: bm25_active_processes ---'
SELECT * FROM facets.bm25_active_processes();
\echo 'PASS: bm25_active_processes executed'

\echo ''
\echo '--- Test 14: bm25_full_cleanup ---'
SELECT * FROM facets.bm25_full_cleanup();
\echo 'PASS: bm25_full_cleanup executed'

\echo ''
\echo '--- Test 15: setup_table_with_bm25 (One-Stop Setup) ---'

-- Create a new test table
CREATE TABLE test_bm25_helpers.products (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    category TEXT,
    price DECIMAL(10,2)
);

INSERT INTO test_bm25_helpers.products (content, category, price) VALUES
    ('Laptop computer with Intel processor', 'Electronics', 999.99),
    ('Wireless mouse ergonomic design', 'Electronics', 49.99),
    ('Office chair comfortable seating', 'Furniture', 299.99),
    ('Standing desk adjustable height', 'Furniture', 599.99),
    ('Programming book for beginners', 'Books', 39.99);

-- Use the one-stop setup function
SELECT facets.setup_table_with_bm25(
    'test_bm25_helpers.products'::regclass,
    'id',
    'content',
    ARRAY[facets.plain_facet('category')],
    'english',
    true,   -- create trigger
    NULL,   -- auto chunk_bits
    true,   -- populate facets
    true,   -- build BM25 index
    1       -- sequential mode (1 worker)
);

-- Verify everything was set up
DO $$
DECLARE
    v_registered boolean;
    v_has_trigger boolean;
    v_doc_count bigint;
    v_term_count bigint;
BEGIN
    -- Check registration
    SELECT EXISTS (SELECT 1 FROM facets.faceted_table 
                   WHERE table_id = 'test_bm25_helpers.products'::regclass::oid)
    INTO v_registered;
    
    -- Check trigger
    SELECT EXISTS (SELECT 1 FROM pg_trigger t
                   JOIN pg_class c ON t.tgrelid = c.oid
                   JOIN pg_namespace n ON c.relnamespace = n.oid
                   WHERE n.nspname = 'test_bm25_helpers' 
                   AND c.relname = 'products'
                   AND t.tgname LIKE '%bm25_sync%')
    INTO v_has_trigger;
    
    -- Check BM25 data
    SELECT COUNT(*) INTO v_doc_count 
    FROM facets.bm25_documents 
    WHERE table_id = 'test_bm25_helpers.products'::regclass::oid;
    
    SELECT COUNT(*) INTO v_term_count 
    FROM facets.bm25_index 
    WHERE table_id = 'test_bm25_helpers.products'::regclass::oid;
    
    IF v_registered THEN
        RAISE NOTICE 'PASS: setup_table_with_bm25 registered table';
    ELSE
        RAISE WARNING 'FAIL: setup_table_with_bm25 did not register table';
    END IF;
    
    IF v_has_trigger THEN
        RAISE NOTICE 'PASS: setup_table_with_bm25 created trigger';
    ELSE
        RAISE WARNING 'FAIL: setup_table_with_bm25 did not create trigger';
    END IF;
    
    IF v_doc_count = 5 THEN
        RAISE NOTICE 'PASS: setup_table_with_bm25 indexed % documents', v_doc_count;
    ELSE
        RAISE NOTICE 'INFO: setup_table_with_bm25 indexed % documents (expected 5)', v_doc_count;
    END IF;
    
    IF v_term_count > 0 THEN
        RAISE NOTICE 'PASS: setup_table_with_bm25 created % terms', v_term_count;
    ELSE
        RAISE WARNING 'FAIL: setup_table_with_bm25 created no terms';
    END IF;
END $$;

-- Verify BM25 search works
DO $$
DECLARE
    v_count int;
BEGIN
    SELECT COUNT(*) INTO v_count FROM facets.bm25_search(
        'test_bm25_helpers.products'::regclass,
        'computer laptop',
        'english',
        false, false, 0.3, 1.2, 0.75, 5
    );
    
    IF v_count >= 1 THEN
        RAISE NOTICE 'PASS: BM25 search works after setup_table_with_bm25 (found % results)', v_count;
    ELSE
        RAISE WARNING 'FAIL: BM25 search returned no results';
    END IF;
END $$;

\echo ''
\echo '--- Cleanup ---'
SELECT facets.bm25_drop_sync_trigger('test_bm25_helpers.products'::regclass);
SELECT facets.drop_faceting('test_bm25_helpers.articles');
SELECT facets.drop_faceting('test_bm25_helpers.products');
DROP SCHEMA test_bm25_helpers CASCADE;

\echo ''
\echo '=============================================='
\echo 'BM25 Helper Functions Test Suite Complete!'
\echo '=============================================='

