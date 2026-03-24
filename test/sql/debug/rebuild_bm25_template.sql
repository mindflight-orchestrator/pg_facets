-- Template for BM25 Rebuild Functions
-- Use this as a reference when creating your own rebuild functions

-- ============================================================================
-- TEMPLATE 1: Basic Rebuild Function
-- ============================================================================

CREATE OR REPLACE FUNCTION providers_imdb.rebuild_title_basics_bm25()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_doc record;
    v_count bigint := 0;
    v_total bigint;
    v_table_oid oid;
    v_start_time timestamptz;
    v_end_time timestamptz;
BEGIN
    v_start_time := clock_timestamp();
    v_table_oid := 'providers_imdb.title_basics'::regclass::oid;
    
    -- Verify table is registered
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table providers_imdb.title_basics is not registered. Run: SELECT facets.add_faceting_to_table(''providers_imdb.title_basics''::regclass, key => ''tconst'', facets => ARRAY[], populate => false);';
    END IF;
    
    -- Get total count
    SELECT COUNT(*) INTO v_total FROM providers_imdb.title_basics;
    RAISE NOTICE 'Starting BM25 rebuild for title_basics. Total documents: %', v_total;
    
    -- Optional: Clear existing index (uncomment if you want a fresh start)
    -- DELETE FROM facets.bm25_index WHERE table_id = v_table_oid;
    -- DELETE FROM facets.bm25_documents WHERE table_id = v_table_oid;
    -- DELETE FROM facets.bm25_statistics WHERE table_id = v_table_oid;
    -- RAISE NOTICE 'Cleared existing index';
    
    -- Index each document
    FOR v_doc IN 
        SELECT 
            tconst,  -- Primary key
            -- Concatenate relevant text columns for indexing
            COALESCE(primarytitle, '') || ' ' || 
            COALESCE(originaltitle, '') || ' ' || 
            COALESCE(description, '') AS content
        FROM providers_imdb.title_basics
        ORDER BY tconst
    LOOP
        BEGIN
            -- Index the document
            PERFORM facets.bm25_index_document(
                'providers_imdb.title_basics'::regclass,
                v_doc.tconst::bigint,  -- Document ID (primary key)
                v_doc.content,         -- Text content to index
                'content',             -- Column name (for reference)
                'english'              -- Language config
            );
            
            v_count := v_count + 1;
            
            -- Progress update every 10000 documents
            IF v_count % 10000 = 0 THEN
                RAISE NOTICE 'Progress: % / % documents (%.1f%%)', 
                    v_count, v_total, (v_count::float / NULLIF(v_total, 0)::float * 100);
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error indexing document %: %', v_doc.tconst, SQLERRM;
            -- Continue with next document
        END;
    END LOOP;
    
    -- Final statistics recalculation
    RAISE NOTICE 'Recalculating statistics...';
    PERFORM facets.bm25_recalculate_statistics('providers_imdb.title_basics'::regclass);
    
    v_end_time := clock_timestamp();
    RAISE NOTICE 'BM25 rebuild complete. Indexed % documents in %', 
        v_count, 
        v_end_time - v_start_time;
    
    -- Verify the index
    SELECT COUNT(*) INTO v_count
    FROM facets.bm25_documents
    WHERE table_id = v_table_oid;
    
    RAISE NOTICE 'Verification: % documents in bm25_documents table', v_count;
END;
$$;

-- ============================================================================
-- TEMPLATE 2: Rebuild Function for name_basics
-- ============================================================================

CREATE OR REPLACE FUNCTION providers_imdb.rebuild_name_basics_bm25()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_doc record;
    v_count bigint := 0;
    v_total bigint;
    v_table_oid oid;
    v_start_time timestamptz;
    v_end_time timestamptz;
BEGIN
    v_start_time := clock_timestamp();
    v_table_oid := 'providers_imdb.name_basics'::regclass::oid;
    
    -- Verify table is registered
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table providers_imdb.name_basics is not registered. Run: SELECT facets.add_faceting_to_table(''providers_imdb.name_basics''::regclass, key => ''nconst'', facets => ARRAY[], populate => false);';
    END IF;
    
    -- Get total count
    SELECT COUNT(*) INTO v_total FROM providers_imdb.name_basics;
    RAISE NOTICE 'Starting BM25 rebuild for name_basics. Total documents: %', v_total;
    
    -- Index each document
    FOR v_doc IN 
        SELECT 
            nconst,  -- Primary key
            -- Concatenate relevant text columns
            COALESCE(primaryname, '') || ' ' || 
            COALESCE(knownfortitles, '') AS content
        FROM providers_imdb.name_basics
        ORDER BY nconst
    LOOP
        BEGIN
            PERFORM facets.bm25_index_document(
                'providers_imdb.name_basics'::regclass,
                v_doc.nconst::bigint,
                v_doc.content,
                'content',
                'english'
            );
            
            v_count := v_count + 1;
            
            IF v_count % 10000 = 0 THEN
                RAISE NOTICE 'Progress: % / % documents (%.1f%%)', 
                    v_count, v_total, (v_count::float / NULLIF(v_total, 0)::float * 100);
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error indexing document %: %', v_doc.nconst, SQLERRM;
        END;
    END LOOP;
    
    -- Final statistics recalculation
    PERFORM facets.bm25_recalculate_statistics('providers_imdb.name_basics'::regclass);
    
    v_end_time := clock_timestamp();
    RAISE NOTICE 'BM25 rebuild complete. Indexed % documents in %', 
        v_count, 
        v_end_time - v_start_time;
END;
$$;

-- ============================================================================
-- TEMPLATE 3: Fast Batch Rebuild (for 0.4.1+)
-- ============================================================================
-- This is much faster than the loop-based approach above

CREATE OR REPLACE FUNCTION providers_imdb.rebuild_title_basics_bm25_batch()
RETURNS TABLE(indexed_count int, elapsed_ms float)
LANGUAGE plpgsql
AS $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.bm25_index_documents_batch(
        'providers_imdb.title_basics'::regclass,
        (
            SELECT jsonb_agg(jsonb_build_object(
                'doc_id', tconst,
                'content', COALESCE(primarytitle, '') || ' ' || 
                          COALESCE(originaltitle, '') || ' ' || 
                          COALESCE(description, '')
            ))
            FROM providers_imdb.title_basics
        ),
        'content',
        'english',
        1000  -- batch size
    );
    
    RETURN QUERY SELECT v_result.indexed_count, v_result.elapsed_ms;
END;
$$;

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- 1. Register the table first (if not already registered):
-- SELECT facets.add_faceting_to_table(
--     'providers_imdb.title_basics'::regclass,
--     key => 'tconst',
--     facets => ARRAY[],  -- Empty if you only need BM25
--     populate => false
-- );

-- 2. Run the rebuild:
-- SELECT providers_imdb.rebuild_title_basics_bm25();

-- 3. Verify the index:
-- SELECT COUNT(*) FROM facets.bm25_documents 
-- WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;

-- 4. Test a search:
-- SELECT * FROM facets.bm25_search(
--     'providers_imdb.title_basics'::regclass,
--     'action movie',
--     'english',
--     false, false, 0.3, 1.2, 0.75, 10
-- );

-- ============================================================================
-- COMMON MISTAKES TO AVOID
-- ============================================================================

-- ❌ WRONG: Not calling bm25_index_document()
-- CREATE FUNCTION rebuild_bm25() RETURNS void AS $$
-- BEGIN
--     -- This does nothing!
--     SELECT COUNT(*) FROM my_table;
-- END $$;

-- ✅ CORRECT: Calling bm25_index_document() for each document
-- CREATE FUNCTION rebuild_bm25() RETURNS void AS $$
-- DECLARE v_doc record;
-- BEGIN
--     FOR v_doc IN SELECT id, content FROM my_table LOOP
--         PERFORM facets.bm25_index_document('my_table'::regclass, v_doc.id, v_doc.content, 'content', 'english');
--     END LOOP;
--     PERFORM facets.bm25_recalculate_statistics('my_table'::regclass);
-- END $$;

-- ❌ WRONG: Using wrong table name or not using ::regclass
-- PERFORM facets.bm25_index_document('title_basics', ...);  -- Missing schema and ::regclass

-- ✅ CORRECT: Using full table name with ::regclass
-- PERFORM facets.bm25_index_document('providers_imdb.title_basics'::regclass, ...);

-- ❌ WRONG: Not registering the table first
-- -- Table must be registered in facets.faceted_table before indexing

-- ✅ CORRECT: Register the table first
-- SELECT facets.add_faceting_to_table('providers_imdb.title_basics'::regclass, key => 'tconst', facets => ARRAY[], populate => false);

