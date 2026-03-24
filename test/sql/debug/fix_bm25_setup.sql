-- Fix BM25 Setup for providers_imdb tables
-- This script registers the tables and provides corrected rebuild functions

-- ============================================================================
-- STEP 1: Register the tables
-- ============================================================================

-- Register title_basics table
-- Note: If your primary key is text (like 'tt0000001'), we'll need to handle it differently
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM facets.faceted_table 
        WHERE table_id = 'providers_imdb.title_basics'::regclass::oid
    ) THEN
        PERFORM facets.add_faceting_to_table(
            'providers_imdb.title_basics'::regclass,
            key => 'tconst',  -- Your primary key column
            facets => ARRAY[],  -- Empty if you only need BM25
            populate => false
        );
        RAISE NOTICE '✓ Registered providers_imdb.title_basics';
    ELSE
        RAISE NOTICE 'Table providers_imdb.title_basics is already registered';
    END IF;
END $$;

-- Register name_basics table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM facets.faceted_table 
        WHERE table_id = 'providers_imdb.name_basics'::regclass::oid
    ) THEN
        PERFORM facets.add_faceting_to_table(
            'providers_imdb.name_basics'::regclass,
            key => 'nconst',  -- Your primary key column
            facets => ARRAY[],  -- Empty if you only need BM25
            populate => false
        );
        RAISE NOTICE '✓ Registered providers_imdb.name_basics';
    ELSE
        RAISE NOTICE 'Table providers_imdb.name_basics is already registered';
    END IF;
END $$;

-- ============================================================================
-- STEP 2: Check primary key types
-- ============================================================================

DO $$
DECLARE
    v_title_pk_type text;
    v_name_pk_type text;
BEGIN
    -- Check title_basics primary key type
    SELECT data_type INTO v_title_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'providers_imdb'
      AND table_name = 'title_basics'
      AND column_name = 'tconst';
    
    -- Check name_basics primary key type
    SELECT data_type INTO v_name_pk_type
    FROM information_schema.columns
    WHERE table_schema = 'providers_imdb'
      AND table_name = 'name_basics'
      AND column_name = 'nconst';
    
    RAISE NOTICE 'title_basics.tconst type: %', v_title_pk_type;
    RAISE NOTICE 'name_basics.nconst type: %', v_name_pk_type;
    
    IF v_title_pk_type NOT IN ('bigint', 'integer', 'smallint') THEN
        RAISE WARNING '⚠ title_basics.tconst is % (not numeric). BM25 doc_id must be bigint. You may need to use a hash or sequence.';
    END IF;
    
    IF v_name_pk_type NOT IN ('bigint', 'integer', 'smallint') THEN
        RAISE WARNING '⚠ name_basics.nconst is % (not numeric). BM25 doc_id must be bigint. You may need to use a hash or sequence.';
    END IF;
END $$;

-- ============================================================================
-- STEP 3: Create corrected rebuild functions
-- ============================================================================

-- Option A: If your primary keys are text (like 'tt0000001'), use hash
-- Option B: If your primary keys are numeric, use them directly

-- For title_basics with TEXT primary key (using hash)
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
    v_doc_id bigint;
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
    
    -- Index each document
    -- Using hash of tconst to convert text primary key to bigint
    FOR v_doc IN 
        SELECT 
            tconst,
            -- Convert text primary key to bigint using hash
            ('x' || substr(md5(tconst), 1, 15))::bit(60)::bigint AS doc_id_hash,
            -- Concatenate relevant text columns for indexing
            COALESCE(primarytitle, '') || ' ' || 
            COALESCE(originaltitle, '') || ' ' || 
            COALESCE(description, '') AS content
        FROM providers_imdb.title_basics
        ORDER BY tconst
    LOOP
        BEGIN
            -- Use hash as doc_id (or use row_number() if you prefer sequential IDs)
            v_doc_id := ABS(v_doc.doc_id_hash);  -- Use absolute value to ensure positive
            
            -- Index the document
            PERFORM facets.bm25_index_document(
                'providers_imdb.title_basics'::regclass,
                v_doc_id,
                v_doc.content,
                'content',
                'english'
            );
            
            v_count := v_count + 1;
            
            -- Progress update every 10000 documents
            IF v_count % 10000 = 0 THEN
                RAISE NOTICE 'Progress: % / % documents (%.1f%%)', 
                    v_count, v_total, (v_count::float / NULLIF(v_total, 0)::float * 100);
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Error indexing document %: %', v_doc.tconst, SQLERRM;
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

-- For name_basics with TEXT primary key (using hash)
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
    v_doc_id bigint;
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
            nconst,
            -- Convert text primary key to bigint using hash
            ('x' || substr(md5(nconst), 1, 15))::bit(60)::bigint AS doc_id_hash,
            -- Concatenate relevant text columns
            COALESCE(primaryname, '') || ' ' || 
            COALESCE(knownfortitles, '') AS content
        FROM providers_imdb.name_basics
        ORDER BY nconst
    LOOP
        BEGIN
            v_doc_id := ABS(v_doc.doc_id_hash);
            
            PERFORM facets.bm25_index_document(
                'providers_imdb.name_basics'::regclass,
                v_doc_id,
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
-- ALTERNATIVE: If your primary keys are already numeric (bigint/integer)
-- ============================================================================

-- Uncomment and modify these if your primary keys are numeric:

/*
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
    
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table not registered';
    END IF;
    
    SELECT COUNT(*) INTO v_total FROM providers_imdb.title_basics;
    RAISE NOTICE 'Starting BM25 rebuild. Total documents: %', v_total;
    
    FOR v_doc IN 
        SELECT 
            tconst::bigint AS doc_id,  -- Direct cast if numeric
            COALESCE(primarytitle, '') || ' ' || 
            COALESCE(originaltitle, '') || ' ' || 
            COALESCE(description, '') AS content
        FROM providers_imdb.title_basics
        ORDER BY tconst
    LOOP
        BEGIN
            PERFORM facets.bm25_index_document(
                'providers_imdb.title_basics'::regclass,
                v_doc.doc_id,
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
            RAISE WARNING 'Error indexing document %: %', v_doc.doc_id, SQLERRM;
        END;
    END LOOP;
    
    PERFORM facets.bm25_recalculate_statistics('providers_imdb.title_basics'::regclass);
    
    v_end_time := clock_timestamp();
    RAISE NOTICE 'BM25 rebuild complete. Indexed % documents in %', 
        v_count, 
        v_end_time - v_start_time;
END;
$$;
*/

-- ============================================================================
-- STEP 4: Verify setup
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Setup Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Run: SELECT providers_imdb.rebuild_title_basics_bm25();';
    RAISE NOTICE '2. Run: SELECT providers_imdb.rebuild_name_basics_bm25();';
    RAISE NOTICE '3. Verify with: SELECT COUNT(*) FROM facets.bm25_documents WHERE table_id = ''providers_imdb.title_basics''::regclass::oid;';
END $$;

