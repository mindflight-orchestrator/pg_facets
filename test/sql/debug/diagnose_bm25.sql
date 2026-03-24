-- BM25 Indexing Diagnostic Script
-- Run this to diagnose why your BM25 indexes are empty
-- Compatible with both psql and standard SQL clients

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'BM25 Indexing Diagnostic Report';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
END $$;

-- 1. Check if tables exist
DO $$
BEGIN
    RAISE NOTICE '1. Checking if BM25 tables exist...';
END $$;
SELECT 
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'facets' AND table_name = 'bm25_index') 
         THEN '✓ bm25_index table exists'
         ELSE '✗ bm25_index table MISSING'
    END as status
UNION ALL
SELECT 
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'facets' AND table_name = 'bm25_documents') 
         THEN '✓ bm25_documents table exists'
         ELSE '✗ bm25_documents table MISSING'
    END
UNION ALL
SELECT 
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'facets' AND table_name = 'bm25_statistics') 
         THEN '✓ bm25_statistics table exists'
         ELSE '✗ bm25_statistics table MISSING'
    END;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '2. Checking registered tables...';
END $$;
SELECT 
    table_id::regclass::text as table_name,
    key as key_column,
    schemaname,
    tablename
FROM facets.faceted_table
ORDER BY table_id DESC
LIMIT 10;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '3. Checking for title_basics registration...';
END $$;
DO $$
DECLARE
    v_table_oid oid;
    v_registered boolean;
BEGIN
    BEGIN
        v_table_oid := 'providers_imdb.title_basics'::regclass::oid;
        SELECT EXISTS (
            SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid
        ) INTO v_registered;
        
        IF v_registered THEN
            RAISE NOTICE '✓ Table providers_imdb.title_basics is REGISTERED (OID: %)', v_table_oid;
        ELSE
            RAISE WARNING '✗ Table providers_imdb.title_basics is NOT REGISTERED (OID: %)', v_table_oid;
            RAISE NOTICE '  → Run: SELECT facets.add_faceting_to_table(''providers_imdb.title_basics''::regclass, key => ''tconst'', facets => ARRAY[], populate => false);';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '✗ Error checking table: %', SQLERRM;
    END;
END $$;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '4. Checking for name_basics registration...';
END $$;
DO $$
DECLARE
    v_table_oid oid;
    v_registered boolean;
BEGIN
    BEGIN
        v_table_oid := 'providers_imdb.name_basics'::regclass::oid;
        SELECT EXISTS (
            SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid
        ) INTO v_registered;
        
        IF v_registered THEN
            RAISE NOTICE '✓ Table providers_imdb.name_basics is REGISTERED (OID: %)', v_table_oid;
        ELSE
            RAISE WARNING '✗ Table providers_imdb.name_basics is NOT REGISTERED (OID: %)', v_table_oid;
            RAISE NOTICE '  → Run: SELECT facets.add_faceting_to_table(''providers_imdb.name_basics''::regclass, key => ''nconst'', facets => ARRAY[], populate => false);';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '✗ Error checking table: %', SQLERRM;
    END;
END $$;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '5. Checking BM25 index data for title_basics...';
END $$;
DO $$
DECLARE
    v_table_oid oid;
    v_doc_count bigint;
    v_term_count bigint;
    v_stats_count int;
BEGIN
    BEGIN
        v_table_oid := 'providers_imdb.title_basics'::regclass::oid;
        
        SELECT COUNT(*) INTO v_doc_count
        FROM facets.bm25_documents
        WHERE table_id = v_table_oid;
        
        SELECT COUNT(*) INTO v_term_count
        FROM facets.bm25_index
        WHERE table_id = v_table_oid;
        
        SELECT COUNT(*) INTO v_stats_count
        FROM facets.bm25_statistics
        WHERE table_id = v_table_oid;
        
        RAISE NOTICE 'Documents indexed: %', v_doc_count;
        RAISE NOTICE 'Terms indexed: %', v_term_count;
        RAISE NOTICE 'Statistics records: %', v_stats_count;
        
        IF v_doc_count = 0 THEN
            RAISE WARNING '✗ NO DOCUMENTS INDEXED - Your rebuild function may not be working!';
        ELSE
            RAISE NOTICE '✓ Documents are indexed';
        END IF;
        
        IF v_term_count = 0 THEN
            RAISE WARNING '✗ NO TERMS INDEXED - Indexing may have failed!';
        ELSE
            RAISE NOTICE '✓ Terms are indexed';
        END IF;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '✗ Error checking index data: %', SQLERRM;
    END;
END $$;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '6. Checking BM25 index data for name_basics...';
END $$;
DO $$
DECLARE
    v_table_oid oid;
    v_doc_count bigint;
    v_term_count bigint;
    v_stats_count int;
BEGIN
    BEGIN
        v_table_oid := 'providers_imdb.name_basics'::regclass::oid;
        
        SELECT COUNT(*) INTO v_doc_count
        FROM facets.bm25_documents
        WHERE table_id = v_table_oid;
        
        SELECT COUNT(*) INTO v_term_count
        FROM facets.bm25_index
        WHERE table_id = v_table_oid;
        
        SELECT COUNT(*) INTO v_stats_count
        FROM facets.bm25_statistics
        WHERE table_id = v_table_oid;
        
        RAISE NOTICE 'Documents indexed: %', v_doc_count;
        RAISE NOTICE 'Terms indexed: %', v_term_count;
        RAISE NOTICE 'Statistics records: %', v_stats_count;
        
        IF v_doc_count = 0 THEN
            RAISE WARNING '✗ NO DOCUMENTS INDEXED - Your rebuild function may not be working!';
        ELSE
            RAISE NOTICE '✓ Documents are indexed';
        END IF;
        
        IF v_term_count = 0 THEN
            RAISE WARNING '✗ NO TERMS INDEXED - Indexing may have failed!';
        ELSE
            RAISE NOTICE '✓ Terms are indexed';
        END IF;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '✗ Error checking index data: %', SQLERRM;
    END;
END $$;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '7. Checking if rebuild functions exist...';
END $$;
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'providers_imdb' 
        AND p.proname = 'rebuild_title_basics_bm25'
    ) THEN '✓ rebuild_title_basics_bm25() exists'
    ELSE '✗ rebuild_title_basics_bm25() NOT FOUND'
    END as status
UNION ALL
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'providers_imdb' 
        AND p.proname = 'rebuild_name_basics_bm25'
    ) THEN '✓ rebuild_name_basics_bm25() exists'
    ELSE '✗ rebuild_name_basics_bm25() NOT FOUND'
    END;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '8. Checking if bm25_index_document function exists...';
END $$;
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_document'
    ) THEN '✓ facets.bm25_index_document() exists'
    ELSE '✗ facets.bm25_index_document() MISSING - Extension may not be installed correctly'
    END as status;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '9. Sample test: Try indexing a single document...';
END $$;
DO $$
DECLARE
    v_table_oid oid;
    v_test_doc_id bigint;
    v_test_content text;
    v_doc_count_before bigint;
    v_doc_count_after bigint;
BEGIN
    BEGIN
        v_table_oid := 'providers_imdb.title_basics'::regclass::oid;
        
        -- Get a test document
        SELECT tconst, COALESCE(primarytitle, 'test') 
        INTO v_test_doc_id, v_test_content
        FROM providers_imdb.title_basics
        LIMIT 1;
        
        IF v_test_doc_id IS NULL THEN
            RAISE NOTICE 'No documents in title_basics table to test with';
            RETURN;
        END IF;
        
        -- Count before
        SELECT COUNT(*) INTO v_doc_count_before
        FROM facets.bm25_documents
        WHERE table_id = v_table_oid AND doc_id = v_test_doc_id;
        
        -- Try to index
        BEGIN
            PERFORM facets.bm25_index_document(
                'providers_imdb.title_basics'::regclass,
                v_test_doc_id,
                v_test_content,
                'content',
                'english'
            );
            
            -- Count after
            SELECT COUNT(*) INTO v_doc_count_after
            FROM facets.bm25_documents
            WHERE table_id = v_table_oid AND doc_id = v_test_doc_id;
            
            IF v_doc_count_after > v_doc_count_before THEN
                RAISE NOTICE '✓ Test indexing SUCCESSFUL - Document % indexed', v_test_doc_id;
            ELSE
                RAISE WARNING '✗ Test indexing FAILED - Document count did not increase';
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '✗ Test indexing ERROR: %', SQLERRM;
        END;
        
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '✗ Error during test: %', SQLERRM;
    END;
END $$;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Diagnostic Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. If tables are not registered, run facets.add_faceting_to_table()';
    RAISE NOTICE '2. Review your rebuild functions - they must call facets.bm25_index_document() for each document';
    RAISE NOTICE '3. Check PostgreSQL logs for errors during rebuild';
    RAISE NOTICE '4. See BM25_INDEXING_REVIEW.md for detailed documentation';
END $$;

