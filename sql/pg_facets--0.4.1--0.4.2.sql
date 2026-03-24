-- pg_facets 0.4.1 to 0.4.2 Migration
-- Adds generic BM25 helper functions, optimized parallel indexing, and monitoring tools
--
-- Key additions:
-- - BM25 sync trigger helpers (create/drop)
-- - Lock-free parallel indexing (replaces slow OFFSET-based approach)
-- - Generic BM25 rebuild function
-- - Monitoring and cleanup functions
-- - Simplified setup function

-- Update version
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.4.2';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- SECTION: BM25 SYNC TRIGGER HELPERS
-- Automatically create/drop triggers to keep BM25 index in sync with table changes
-- ============================================================================

-- Create a trigger to keep BM25 index in sync with table changes
-- This trigger handles INSERT, UPDATE, and DELETE operations
CREATE OR REPLACE FUNCTION facets.bm25_create_sync_trigger(
    p_table regclass,
    p_id_column text DEFAULT 'id',
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT NULL  -- NULL = use table's bm25_language
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_schema_name text;
    v_table_name text;
    v_trigger_func_name text;
    v_trigger_name text;
    v_effective_language text;
BEGIN
    v_table_oid := p_table::oid;
    
    -- Get schema and table name
    SELECT n.nspname, c.relname INTO v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = v_table_oid;
    
    -- Check if table is registered
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table % is not registered in facets.faceted_table. Run facets.add_faceting_to_table() first.', p_table;
    END IF;
    
    -- Determine effective language
    v_effective_language := COALESCE(p_language, facets.bm25_get_language(p_table), 'english');
    
    -- Generate names
    v_trigger_func_name := format('%I.%I_bm25_sync_func', v_schema_name, v_table_name);
    v_trigger_name := format('%I_bm25_sync', v_table_name);
    
    -- Create the trigger function
    EXECUTE format($func$
        CREATE OR REPLACE FUNCTION %s()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $trigger$
        DECLARE
            v_table regclass := TG_RELID::regclass;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                -- Remove from BM25 index
                PERFORM facets.bm25_delete_document(v_table, (OLD.%I)::bigint);
                RETURN OLD;
            ELSIF TG_OP = 'UPDATE' THEN
                -- Re-index if content changed
                PERFORM facets.bm25_delete_document(v_table, (OLD.%I)::bigint);
                IF NEW.%I IS NOT NULL AND NEW.%I <> '' THEN
                    PERFORM facets.bm25_index_document(v_table, (NEW.%I)::bigint, NEW.%I, %L, %L);
                END IF;
                RETURN NEW;
            ELSE
                -- INSERT
                IF NEW.%I IS NOT NULL AND NEW.%I <> '' THEN
                    PERFORM facets.bm25_index_document(v_table, (NEW.%I)::bigint, NEW.%I, %L, %L);
                END IF;
                RETURN NEW;
            END IF;
        END;
        $trigger$
    $func$,
        v_trigger_func_name,
        p_id_column,  -- DELETE: OLD.id
        p_id_column,  -- UPDATE: OLD.id for delete
        p_content_column, p_content_column,  -- UPDATE: content check
        p_id_column, p_content_column, p_content_column, v_effective_language,  -- UPDATE: index
        p_content_column, p_content_column,  -- INSERT: content check
        p_id_column, p_content_column, p_content_column, v_effective_language  -- INSERT: index
    );
    
    -- Drop existing trigger if exists
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, p_table);
    
    -- Create the trigger
    EXECUTE format($trig$
        CREATE TRIGGER %I
        AFTER INSERT OR UPDATE OR DELETE ON %s
        FOR EACH ROW
        EXECUTE FUNCTION %s()
    $trig$, v_trigger_name, p_table, v_trigger_func_name);
    
    RAISE NOTICE 'Created BM25 sync trigger % on %', v_trigger_name, p_table;
END;
$$;

-- Drop BM25 sync trigger from a table
CREATE OR REPLACE FUNCTION facets.bm25_drop_sync_trigger(
    p_table regclass
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_schema_name text;
    v_table_name text;
    v_trigger_func_name text;
    v_trigger_name text;
BEGIN
    v_table_oid := p_table::oid;
    
    -- Get schema and table name
    SELECT n.nspname, c.relname INTO v_schema_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = v_table_oid;
    
    -- Generate names
    v_trigger_func_name := format('%I.%I_bm25_sync_func', v_schema_name, v_table_name);
    v_trigger_name := format('%I_bm25_sync', v_table_name);
    
    -- Drop trigger
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, p_table);
    
    -- Drop trigger function
    EXECUTE format('DROP FUNCTION IF EXISTS %s()', v_trigger_func_name);
    
    RAISE NOTICE 'Dropped BM25 sync trigger % from %', v_trigger_name, p_table;
END;
$$;

-- ============================================================================
-- SECTION: LOCK-FREE PARALLEL BM25 INDEXING
-- Optimized parallel indexing using per-worker staging tables (no lock contention)
-- This replaces the slower OFFSET-based approach
-- ============================================================================

-- Worker function that writes to a PRIVATE staging table (no lock contention)
-- Each worker tokenizes documents and writes term data to its own table
CREATE OR REPLACE FUNCTION facets.bm25_index_worker_lockfree(
    p_table_id oid,
    p_source_staging text,  -- Source document staging table (facets schema)
    p_output_staging text,  -- Output term staging table for THIS worker (facets schema)
    p_language text,
    p_total_docs bigint,
    p_num_workers int,
    p_worker_id int  -- 1-based worker ID
) RETURNS TABLE(docs_indexed int, terms_extracted bigint, elapsed_ms float)
LANGUAGE plpgsql AS $$
DECLARE
    v_base_docs bigint;
    v_remainder int;
    v_docs_for_this_worker bigint;
    v_start_rn bigint;
    v_end_rn bigint;
    v_doc record;
    v_count int := 0;
    v_term_count bigint := 0;
    v_start_time timestamptz;
    v_lexeme record;
    v_doc_length int;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Calculate row number range for this worker (even distribution)
    -- Use integer division with remainder handling for fair distribution
    v_base_docs := p_total_docs / p_num_workers;
    v_remainder := p_total_docs % p_num_workers;

    -- First v_remainder workers get v_base_docs + 1 documents
    -- Remaining workers get v_base_docs documents
    IF p_worker_id <= v_remainder THEN
        v_docs_for_this_worker := v_base_docs + 1;
        v_start_rn := (p_worker_id - 1) * (v_base_docs + 1) + 1;
    ELSE
        v_docs_for_this_worker := v_base_docs;
        v_start_rn := v_remainder * (v_base_docs + 1) + (p_worker_id - v_remainder - 1) * v_base_docs + 1;
    END IF;

    v_end_rn := v_start_rn + v_docs_for_this_worker - 1;
    
    -- Skip if no documents for this worker
    IF v_start_rn > p_total_docs OR v_end_rn < v_start_rn THEN
        docs_indexed := 0;
        terms_extracted := 0;
        elapsed_ms := 0;
        RETURN NEXT;
        RETURN;
    END IF;
    
    -- Process documents and write terms to worker's private staging table
    FOR v_doc IN EXECUTE format(
        'SELECT doc_id, content FROM facets.%I WHERE rn BETWEEN %s AND %s ORDER BY rn',
        p_source_staging, v_start_rn, v_end_rn
    )
    LOOP
        -- Skip empty content
        IF v_doc.content IS NULL OR v_doc.content = '' THEN
            CONTINUE;
        END IF;
        
        -- Tokenize using native Zig tokenizer (production function)
        -- This uses the same tokenization logic as bm25_index_worker_native
        -- facets.tokenize_native() uses the Zig tokenizer_native.tokenizeNative() function
        v_doc_length := 0;

        -- Extract each lexeme and frequency using native Zig tokenizer
        -- facets.tokenize_native() returns (lexeme text, freq int)
        FOR v_lexeme IN
            SELECT
                lexeme as word,
                freq as nentry
            FROM facets.tokenize_native(v_doc.content, p_language)
        LOOP
            -- Insert term data into worker's staging table
            EXECUTE format(
                'INSERT INTO facets.%I (term_hash, term_text, doc_id, term_freq, doc_length) VALUES ($1, $2, $3, $4, 0)',
                p_output_staging
            ) USING 
                hashtext(v_lexeme.word)::bigint,
                v_lexeme.word,
                v_doc.doc_id,
                v_lexeme.nentry;
            
            v_doc_length := v_doc_length + v_lexeme.nentry;
            v_term_count := v_term_count + 1;
        END LOOP;
        
        -- Update doc_length for all terms of this document
        IF v_doc_length > 0 THEN
            EXECUTE format(
                'UPDATE facets.%I SET doc_length = $1 WHERE doc_id = $2',
                p_output_staging
            ) USING v_doc_length, v_doc.doc_id;
        END IF;
        
        v_count := v_count + 1;
    END LOOP;
    
    docs_indexed := v_count;
    terms_extracted := v_term_count;
    elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    RETURN NEXT;
END;
$$;

-- Helper function to cleanup staging tables
CREATE OR REPLACE FUNCTION facets.bm25_cleanup_staging_tables(
    p_source_staging text,
    p_worker_stagings text[],
    p_conn_string text
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_conn_name text := 'bm25_cleanup';
    v_table text;
BEGIN
    BEGIN
        PERFORM dblink_connect(v_conn_name, p_conn_string);
        
        -- Drop source staging table
        PERFORM dblink_exec(v_conn_name, format('DROP TABLE IF EXISTS facets.%I', p_source_staging));
        
        -- Drop all worker staging tables
        FOREACH v_table IN ARRAY p_worker_stagings LOOP
            PERFORM dblink_exec(v_conn_name, format('DROP TABLE IF EXISTS facets.%I', v_table));
        END LOOP;
        
        PERFORM dblink_disconnect(v_conn_name);
    EXCEPTION WHEN OTHERS THEN
        BEGIN PERFORM dblink_disconnect(v_conn_name); EXCEPTION WHEN OTHERS THEN END;
        RAISE WARNING '[BM25 CLEANUP] Failed to drop some staging tables: %', SQLERRM;
    END;
END;
$$;

-- OPTIMIZED parallel indexing function using lock-free staging tables
-- This REPLACES the slower OFFSET-based facets.bm25_index_documents_parallel()
CREATE OR REPLACE FUNCTION facets.bm25_index_documents_parallel(
    p_table_id regclass,
    p_source_query text,  -- Query that returns (doc_id bigint, content text)
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT 'english',
    p_num_workers int DEFAULT 4,
    p_connection_string text DEFAULT NULL  -- If NULL, uses current database
) RETURNS TABLE(
    worker_id int,
    docs_indexed int,
    elapsed_ms float,
    status text
)
LANGUAGE plpgsql AS $$
DECLARE
    v_total_docs bigint;
    v_source_staging text;
    v_worker_staging text;
    v_worker_stagings text[] := ARRAY[]::text[];
    v_conn_name text;
    v_conn_string text;
    v_worker_query text;
    v_start_time timestamptz;
    v_phase_start timestamptz;
    v_phase_elapsed numeric;
    v_result record;
    v_table_oid oid;
    v_merge_sql text;
    v_total_terms bigint;
    v_total_docs_indexed bigint;
    v_has_dblink boolean;
    i int;
BEGIN
    v_start_time := clock_timestamp();
    v_table_oid := p_table_id::oid;
    
    -- Check if dblink is available
    v_has_dblink := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink');
    
    -- Use current database connection if not specified
    IF p_connection_string IS NULL THEN
        v_conn_string := format('dbname=%s user=%s', current_database(), current_user);
    ELSE
        v_conn_string := p_connection_string;
    END IF;
    
    -- Count total documents
    EXECUTE format('SELECT COUNT(*) FROM (%s) AS src', p_source_query) INTO v_total_docs;
    
    IF v_total_docs = 0 THEN
        worker_id := 0;
        docs_indexed := 0;
        elapsed_ms := 0;
        status := 'No documents to index';
        RETURN NEXT;
        RETURN;
    END IF;
    
    -- Fall back to sequential if dblink not available
    IF NOT v_has_dblink THEN
        RAISE NOTICE '[BM25 PARALLEL] dblink not available, falling back to sequential indexing';
        
        -- Sequential fallback
        FOR v_result IN 
            SELECT * FROM facets.bm25_index_worker(
                p_table_id, p_source_query, p_content_column, p_language,
                v_total_docs, 1, 1
            )
        LOOP
            worker_id := 1;
            docs_indexed := v_result.docs_indexed;
            elapsed_ms := v_result.elapsed_ms;
            status := 'sequential (no dblink)';
            RETURN NEXT;
        END LOOP;
        
        PERFORM facets.bm25_recalculate_statistics(p_table_id);
        RETURN;
    END IF;
    
    RAISE NOTICE '[BM25 PARALLEL] Starting lock-free parallel indexing with % workers for % documents', p_num_workers, v_total_docs;
    
    -- Generate unique base name for staging tables
    v_source_staging := format('bm25_src_%s_%s', v_table_oid, EXTRACT(EPOCH FROM clock_timestamp())::bigint);
    
    -- =======================================================================
    -- PHASE 1: Create source document staging table with ROW_NUMBER
    -- =======================================================================
    RAISE NOTICE '[BM25 PARALLEL] Phase 1: Creating source staging table...';
    v_phase_start := clock_timestamp();
    
    v_conn_name := 'bm25_setup_conn';
    BEGIN
        PERFORM dblink_connect(v_conn_name, v_conn_string);
        
        -- Create source staging table with ROW_NUMBER()
        PERFORM dblink_exec(v_conn_name, format(
            'CREATE UNLOGGED TABLE facets.%I AS '
            'SELECT doc_id, content, ROW_NUMBER() OVER (ORDER BY doc_id) as rn '
            'FROM (%s) AS src',
            v_source_staging, p_source_query
        ));
        
        -- Create index on row number for fast range queries
        PERFORM dblink_exec(v_conn_name, format('CREATE INDEX ON facets.%I (rn)', v_source_staging));
        
        -- Create per-worker output staging tables (UNLOGGED for speed)
        FOR i IN 1..p_num_workers LOOP
            v_worker_staging := format('bm25_w%s_%s_%s', i, v_table_oid, EXTRACT(EPOCH FROM clock_timestamp())::bigint);
            v_worker_stagings := array_append(v_worker_stagings, v_worker_staging);
            
            PERFORM dblink_exec(v_conn_name, format(
                'CREATE UNLOGGED TABLE facets.%I ('
                '    term_hash bigint NOT NULL,'
                '    term_text text NOT NULL,'
                '    doc_id bigint NOT NULL,'
                '    term_freq int NOT NULL,'
                '    doc_length int NOT NULL DEFAULT 0'
                ')',
                v_worker_staging
            ));
        END LOOP;
        
        PERFORM dblink_disconnect(v_conn_name);
    EXCEPTION WHEN OTHERS THEN
        BEGIN PERFORM dblink_disconnect(v_conn_name); EXCEPTION WHEN OTHERS THEN END;
        RAISE EXCEPTION '[BM25 PARALLEL] Failed to create staging tables: %', SQLERRM;
    END;
    
    v_phase_elapsed := EXTRACT(EPOCH FROM (clock_timestamp() - v_phase_start));
    RAISE NOTICE '[BM25 PARALLEL] Phase 1 complete in %s seconds', round(v_phase_elapsed::numeric, 1);
    
    -- =======================================================================
    -- PHASE 2: Spawn parallel workers (each writes to its own staging table)
    -- =======================================================================
    RAISE NOTICE '[BM25 PARALLEL] Phase 2: Spawning % parallel workers...', p_num_workers;
    v_phase_start := clock_timestamp();
    
    FOR i IN 1..p_num_workers LOOP
        v_conn_name := 'bm25_worker_' || i;
        v_worker_staging := v_worker_stagings[i];
        
        -- Build worker query using the lock-free worker function
        v_worker_query := format(
            'SELECT * FROM facets.bm25_index_worker_lockfree(%s, %L, %L, %L, %s, %s, %s)',
            v_table_oid, v_source_staging, v_worker_staging, p_language,
            v_total_docs, p_num_workers, i
        );
        
        BEGIN
            PERFORM dblink_connect(v_conn_name, v_conn_string);
            PERFORM dblink_send_query(v_conn_name, v_worker_query);
        EXCEPTION WHEN OTHERS THEN
            worker_id := i;
            docs_indexed := 0;
            elapsed_ms := 0;
            status := 'Failed to spawn worker: ' || SQLERRM;
            RETURN NEXT;
            -- Cleanup and exit
            PERFORM facets.bm25_cleanup_staging_tables(v_source_staging, v_worker_stagings, v_conn_string);
            RETURN;
        END;
    END LOOP;
    
    -- Wait for all workers to complete
    v_total_docs_indexed := 0;
    v_total_terms := 0;
    FOR i IN 1..p_num_workers LOOP
        v_conn_name := 'bm25_worker_' || i;
        
        BEGIN
            FOR v_result IN SELECT * FROM dblink_get_result(v_conn_name) AS t(docs_indexed int, terms_extracted bigint, elapsed_ms float)
            LOOP
                worker_id := i;
                docs_indexed := v_result.docs_indexed;
                elapsed_ms := v_result.elapsed_ms;
                status := format('completed (%s terms)', v_result.terms_extracted);
                v_total_docs_indexed := v_total_docs_indexed + v_result.docs_indexed;
                v_total_terms := v_total_terms + v_result.terms_extracted;
                RETURN NEXT;
            END LOOP;
            PERFORM dblink_disconnect(v_conn_name);
        EXCEPTION WHEN OTHERS THEN
            worker_id := i;
            docs_indexed := 0;
            elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
            status := 'error: ' || SQLERRM;
            RETURN NEXT;
            BEGIN PERFORM dblink_disconnect(v_conn_name); EXCEPTION WHEN OTHERS THEN END;
        END;
    END LOOP;
    
    v_phase_elapsed := EXTRACT(EPOCH FROM (clock_timestamp() - v_phase_start));
    RAISE NOTICE '[BM25 PARALLEL] Phase 2 complete in %s seconds. Workers extracted % terms from % documents.', 
        round(v_phase_elapsed::numeric, 1), v_total_terms, v_total_docs_indexed;
    
    -- =======================================================================
    -- PHASE 3: Merge all worker staging tables into bm25_index and bm25_documents
    -- =======================================================================
    RAISE NOTICE '[BM25 PARALLEL] Phase 3: Merging worker results into BM25 tables...';
    v_phase_start := clock_timestamp();
    
    v_conn_name := 'bm25_merge_conn';
    BEGIN
        PERFORM dblink_connect(v_conn_name, v_conn_string);
        
        -- Build UNION ALL of all worker staging tables
        v_merge_sql := '';
        FOR i IN 1..p_num_workers LOOP
            IF i > 1 THEN v_merge_sql := v_merge_sql || ' UNION ALL '; END IF;
            v_merge_sql := v_merge_sql || format('SELECT * FROM facets.%I', v_worker_stagings[i]);
        END LOOP;
        
        -- Merge into bm25_index using aggregation (single INSERT, no lock contention)
        PERFORM dblink_exec(v_conn_name, format(
            'INSERT INTO facets.bm25_index (table_id, term_hash, term_text, doc_ids, term_freqs, language) '
            'SELECT '
            '    %s as table_id, '
            '    term_hash, '
            '    term_text, '
            '    rb_build_agg(doc_id::int) as doc_ids, '
            '    jsonb_object_agg(doc_id::text, term_freq) as term_freqs, '
            '    %L as language '
            'FROM (%s) all_terms '
            'GROUP BY term_hash, term_text '
            'ON CONFLICT (table_id, term_hash) DO UPDATE SET '
            '    doc_ids = rb_or(facets.bm25_index.doc_ids, EXCLUDED.doc_ids), '
            '    term_freqs = facets.bm25_index.term_freqs || EXCLUDED.term_freqs',
            v_table_oid, p_language, v_merge_sql
        ));
        
        -- Merge into bm25_documents (one row per document)
        PERFORM dblink_exec(v_conn_name, format(
            'INSERT INTO facets.bm25_documents (table_id, doc_id, doc_length, language) '
            'SELECT DISTINCT ON (doc_id) '
            '    %s as table_id, '
            '    doc_id, '
            '    doc_length, '
            '    %L as language '
            'FROM (%s) all_terms '
            'WHERE doc_length > 0 '
            'ORDER BY doc_id '
            'ON CONFLICT (table_id, doc_id) DO UPDATE SET '
            '    doc_length = EXCLUDED.doc_length, '
            '    updated_at = now()',
            v_table_oid, p_language, v_merge_sql
        ));
        
        PERFORM dblink_disconnect(v_conn_name);
    EXCEPTION WHEN OTHERS THEN
        BEGIN PERFORM dblink_disconnect(v_conn_name); EXCEPTION WHEN OTHERS THEN END;
        RAISE WARNING '[BM25 PARALLEL] Merge failed: %. Cleaning up...', SQLERRM;
        PERFORM facets.bm25_cleanup_staging_tables(v_source_staging, v_worker_stagings, v_conn_string);
        RAISE;
    END;
    
    v_phase_elapsed := EXTRACT(EPOCH FROM (clock_timestamp() - v_phase_start));
    RAISE NOTICE '[BM25 PARALLEL] Phase 3 complete in %s seconds', round(v_phase_elapsed::numeric, 1);
    
    -- =======================================================================
    -- PHASE 4: Cleanup staging tables and recalculate statistics
    -- =======================================================================
    RAISE NOTICE '[BM25 PARALLEL] Phase 4: Cleanup and statistics...';
    
    PERFORM facets.bm25_cleanup_staging_tables(v_source_staging, v_worker_stagings, v_conn_string);
    PERFORM facets.bm25_recalculate_statistics(p_table_id);
    
    v_phase_elapsed := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    RAISE NOTICE '[BM25 PARALLEL] Complete! Total time: %s seconds (%s minutes)', 
        round(v_phase_elapsed::numeric, 1), round((v_phase_elapsed / 60)::numeric, 1);
    
    RETURN;
END;
$$;

-- ============================================================================
-- SECTION: GENERIC BM25 REBUILD FUNCTION
-- Rebuild BM25 index for any registered table
-- ============================================================================

CREATE OR REPLACE FUNCTION facets.bm25_rebuild_index(
    p_table regclass,
    p_id_column text DEFAULT 'id',
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT 'english',
    p_num_workers int DEFAULT 0,            -- 0 = auto, 1 = sequential, >1 = parallel
    p_connection_string text DEFAULT NULL,  -- only used for parallel mode
    p_progress_step_size int DEFAULT 50000  -- Progress reporting frequency
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_source_query text;
    v_total_docs bigint;
    v_has_dblink boolean;
    v_workers int;
    v_result record;
    -- Sequential mode variables
    v_doc record;
    v_count bigint := 0;
    v_batch_count bigint := 0;
    v_start_time timestamptz;
    v_elapsed_seconds numeric;
    v_estimated_remaining numeric;
    v_docs_per_second numeric;
    -- Verification variables
    v_indexed_docs bigint;
    v_indexed_terms bigint;
    v_stats_records bigint;
    v_conn_string text;
BEGIN
    v_table_oid := p_table::oid;
    v_start_time := clock_timestamp();

    -- Guard: BM25 requires the table to be registered
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table % (oid: %) is not registered in facets.faceted_table. Run facets.add_faceting_to_table() first.', p_table::text, v_table_oid;
    END IF;

    RAISE NOTICE '[BM25 REBUILD] %: Starting rebuild at %', p_table::text, v_start_time;

    -- Clear existing BM25 data for this table
    RAISE NOTICE '[BM25 REBUILD] %: Clearing existing BM25 data...', p_table::text;
    DELETE FROM facets.bm25_index WHERE table_id = v_table_oid;
    DELETE FROM facets.bm25_documents WHERE table_id = v_table_oid;
    DELETE FROM facets.bm25_statistics WHERE table_id = v_table_oid;

    -- Build source query
    v_source_query := format(
        'SELECT (%1$I)::bigint AS doc_id, %2$I AS content FROM %3$s WHERE %2$I IS NOT NULL AND %2$I <> '''' ORDER BY (%1$I)::bigint',
        p_id_column, p_content_column, p_table
    );

    -- Count documents
    EXECUTE format('SELECT COUNT(*) FROM (%s) AS src', v_source_query) INTO v_total_docs;
    IF v_total_docs = 0 THEN
        RAISE NOTICE '[BM25 REBUILD] %: no documents to index', p_table::text;
        RETURN;
    END IF;

    RAISE NOTICE '[BM25 REBUILD] %: Found % documents to index', p_table::text, v_total_docs;

    v_has_dblink := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink');

    -- Auto workers: if dblink is available, use 4 workers; otherwise sequential
    IF p_num_workers = 0 THEN
        v_workers := CASE WHEN v_has_dblink THEN 4 ELSE 1 END;
    ELSE
        v_workers := GREATEST(1, p_num_workers);
    END IF;

    IF v_workers > 1 AND v_has_dblink THEN
        RAISE NOTICE '[BM25 REBUILD] %: starting parallel rebuild with % workers', p_table::text, v_workers;
        
        -- Build connection string if not provided
        IF p_connection_string IS NULL THEN
            v_conn_string := format('dbname=%s user=%s', current_database(), current_user);
        ELSE
            v_conn_string := p_connection_string;
        END IF;
        
        FOR v_result IN
            SELECT *
            FROM facets.bm25_index_documents_parallel(
                p_table,
                v_source_query,
                p_content_column,
                p_language,
                v_workers,
                v_conn_string
            )
        LOOP
            RAISE NOTICE '[BM25 REBUILD] %: worker % -> % docs in % ms (%s)',
                p_table::text, v_result.worker_id, v_result.docs_indexed,
                round(v_result.elapsed_ms::numeric, 1), v_result.status;
        END LOOP;
    ELSE
        RAISE NOTICE '[BM25 REBUILD] %: starting sequential rebuild (% documents)', p_table::text, v_total_docs;

        -- Sequential rebuild using cursor
        FOR v_doc IN EXECUTE v_source_query
        LOOP
            PERFORM facets.bm25_index_document(
                p_table,
                v_doc.doc_id,
                v_doc.content,
                p_content_column,
                p_language
            );
            
            v_count := v_count + 1;
            v_batch_count := v_batch_count + 1;
            
            -- Progress reporting
            IF v_batch_count >= p_progress_step_size THEN
                v_elapsed_seconds := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
                v_docs_per_second := v_count / NULLIF(v_elapsed_seconds, 0);
                
                IF v_count < v_total_docs AND v_docs_per_second > 0 THEN
                    v_estimated_remaining := (v_total_docs - v_count) / v_docs_per_second;
                    RAISE NOTICE '[BM25 REBUILD] %: Progress: % / % (%.1f%%) - Rate: % docs/sec - ETA: % min',
                        p_table::text, v_count, v_total_docs,
                        (v_count::numeric / v_total_docs * 100),
                        round(v_docs_per_second::numeric, 0),
                        round((v_estimated_remaining / 60)::numeric, 1);
                END IF;
                
                v_batch_count := 0;
            END IF;
        END LOOP;

        -- Recalculate statistics
        PERFORM facets.bm25_recalculate_statistics(p_table);
    END IF;

    -- Verification
    SELECT COUNT(*) INTO v_indexed_docs FROM facets.bm25_documents WHERE table_id = v_table_oid;
    SELECT COUNT(*) INTO v_indexed_terms FROM facets.bm25_index WHERE table_id = v_table_oid;
    
    v_elapsed_seconds := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    RAISE NOTICE '[BM25 REBUILD] %: Complete! % docs, % terms in % seconds',
        p_table::text, v_indexed_docs, v_indexed_terms, round(v_elapsed_seconds::numeric, 1);
    
    IF v_indexed_docs < v_total_docs * 0.9 THEN
        RAISE WARNING '[BM25 REBUILD] %: Only %.1f%% of documents indexed', 
            p_table::text, (v_indexed_docs::numeric / v_total_docs * 100);
    END IF;
END;
$$;

-- ============================================================================
-- SECTION: BM25 MONITORING AND CLEANUP FUNCTIONS
-- Tools for monitoring BM25 indexing progress and cleaning up stuck processes
-- ============================================================================

-- Check BM25 index status for all registered tables
CREATE OR REPLACE FUNCTION facets.bm25_status()
RETURNS TABLE(
    table_name text,
    documents_indexed bigint,
    unique_terms bigint,
    total_documents bigint,
    avg_doc_length numeric,
    last_updated timestamp
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.table_id::regclass::text as table_name,
        COALESCE(d.doc_count, 0)::bigint as documents_indexed,
        COALESCE(i.term_count, 0)::bigint as unique_terms,
        s.total_documents::bigint,
        round(s.avg_document_length::numeric, 2) as avg_doc_length,
        s.last_updated::timestamp
    FROM facets.bm25_statistics s
    LEFT JOIN (
        SELECT table_id, COUNT(*) as doc_count 
        FROM facets.bm25_documents 
        GROUP BY table_id
    ) d ON s.table_id = d.table_id
    LEFT JOIN (
        SELECT table_id, COUNT(DISTINCT term_hash) as term_count 
        FROM facets.bm25_index 
        GROUP BY table_id
    ) i ON s.table_id = i.table_id
    ORDER BY s.table_id;
    
    IF NOT FOUND THEN
        table_name := 'No BM25 indexes found';
        documents_indexed := 0;
        unique_terms := 0;
        total_documents := 0;
        avg_doc_length := 0;
        last_updated := NULL;
        RETURN NEXT;
    END IF;
END;
$$;

-- Quick progress check for a specific table or all tables
CREATE OR REPLACE FUNCTION facets.bm25_progress(p_table regclass DEFAULT NULL)
RETURNS TABLE(
    table_name text,
    documents_indexed bigint,
    source_documents bigint,
    progress_pct numeric,
    unique_terms bigint
)
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_source_count bigint;
BEGIN
    IF p_table IS NULL THEN
        -- Show all registered tables
        RETURN QUERY
        SELECT 
            ft.table_id::regclass::text as table_name,
            COALESCE(d.cnt, 0)::bigint as documents_indexed,
            NULL::bigint as source_documents,
            NULL::numeric as progress_pct,
            COALESCE(i.cnt, 0)::bigint as unique_terms
        FROM facets.faceted_table ft
        LEFT JOIN (
            SELECT table_id, COUNT(*) as cnt FROM facets.bm25_documents GROUP BY table_id
        ) d ON ft.table_id = d.table_id
        LEFT JOIN (
            SELECT table_id, COUNT(DISTINCT term_hash) as cnt FROM facets.bm25_index GROUP BY table_id
        ) i ON ft.table_id = i.table_id
        ORDER BY ft.table_id;
    ELSE
        v_table_oid := p_table::oid;
        
        -- Try to get source document count
        BEGIN
            EXECUTE format('SELECT COUNT(*) FROM %s WHERE content IS NOT NULL AND content <> ''''', p_table::text)
            INTO v_source_count;
        EXCEPTION WHEN OTHERS THEN
            v_source_count := NULL;
        END;
        
        RETURN QUERY
        SELECT 
            p_table::text as table_name,
            COALESCE((SELECT COUNT(*) FROM facets.bm25_documents WHERE table_id = v_table_oid), 0) as documents_indexed,
            v_source_count as source_documents,
            CASE 
                WHEN v_source_count > 0 THEN 
                    round((COALESCE((SELECT COUNT(*) FROM facets.bm25_documents WHERE table_id = v_table_oid), 0)::numeric / v_source_count * 100), 2)
                ELSE NULL
            END as progress_pct,
            COALESCE((SELECT COUNT(DISTINCT term_hash) FROM facets.bm25_index WHERE table_id = v_table_oid), 0) as unique_terms;
    END IF;
END;
$$;

-- Check active BM25-related processes
CREATE OR REPLACE FUNCTION facets.bm25_active_processes()
RETURNS TABLE(
    pid int,
    state text,
    duration interval,
    wait_event text,
    operation_type text,
    query_preview text
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pa.pid::int,
        pa.state::text,
        (now() - pa.query_start)::interval as duration,
        COALESCE(pa.wait_event_type || ':' || pa.wait_event, '')::text as wait_event,
        CASE 
            WHEN pa.query LIKE '%bm25_index_worker%' THEN 'worker'
            WHEN pa.query LIKE '%bm25_index_document%' THEN 'index_document'
            WHEN pa.query LIKE '%rebuild%bm25%' OR pa.query LIKE '%bm25_rebuild%' THEN 'rebuild'
            WHEN pa.query LIKE '%staging%' THEN 'staging'
            WHEN pa.query LIKE '%dblink%' THEN 'dblink'
            ELSE 'other'
        END::text as operation_type,
        left(pa.query, 80)::text as query_preview
    FROM pg_stat_activity pa
    WHERE (
        pa.query ILIKE '%bm25%' 
        OR pa.query ILIKE '%dblink%' 
        OR pa.query ILIKE '%staging%'
    )
    AND pa.pid != pg_backend_pid()
    AND pa.state != 'idle'
    ORDER BY pa.query_start;
    
    IF NOT FOUND THEN
        pid := NULL;
        state := 'No active BM25 processes';
        duration := NULL;
        wait_event := NULL;
        operation_type := NULL;
        query_preview := NULL;
        RETURN NEXT;
    END IF;
END;
$$;

-- Disconnect all dblink connections
CREATE OR REPLACE FUNCTION facets.bm25_cleanup_dblinks()
RETURNS TABLE(connection_name text, status text)
LANGUAGE plpgsql AS $$
DECLARE
    v_conn text;
    v_conns text[];
BEGIN
    -- Check if dblink is available
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') THEN
        connection_name := NULL;
        status := 'dblink extension not installed';
        RETURN NEXT;
        RETURN;
    END IF;
    
    SELECT array_agg(unnest) INTO v_conns
    FROM (SELECT unnest(dblink_get_connections())) AS t(unnest);
    
    IF v_conns IS NULL OR array_length(v_conns, 1) IS NULL THEN
        connection_name := NULL;
        status := 'No dblink connections found';
        RETURN NEXT;
        RETURN;
    END IF;
    
    FOREACH v_conn IN ARRAY v_conns
    LOOP
        BEGIN
            PERFORM dblink_disconnect(v_conn);
            connection_name := v_conn;
            status := 'disconnected';
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            connection_name := v_conn;
            status := 'error: ' || SQLERRM;
            RETURN NEXT;
        END;
    END LOOP;
END;
$$;

-- Drop orphaned staging tables
CREATE OR REPLACE FUNCTION facets.bm25_cleanup_staging()
RETURNS TABLE(table_name text, status text)
LANGUAGE plpgsql AS $$
DECLARE
    v_table text;
    v_found boolean := false;
BEGIN
    -- Check facets schema for all BM25 staging table patterns
    FOR v_table IN 
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'facets'
        AND (
            tablename LIKE 'bm25_staging%'
            OR tablename LIKE 'bm25_src_%'
            OR tablename LIKE 'bm25_w%'
        )
    LOOP
        v_found := true;
        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_table);
            table_name := 'facets.' || v_table;
            status := 'dropped';
            RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            table_name := 'facets.' || v_table;
            status := 'error: ' || SQLERRM;
            RETURN NEXT;
        END;
    END LOOP;
    
    IF NOT v_found THEN
        table_name := NULL;
        status := 'No staging tables found';
        RETURN NEXT;
    END IF;
END;
$$;

-- Kill stuck BM25 processes
CREATE OR REPLACE FUNCTION facets.bm25_kill_stuck(p_min_duration interval DEFAULT '5 minutes')
RETURNS TABLE(pid int, duration interval, status text)
LANGUAGE plpgsql AS $$
DECLARE
    v_pid int;
    v_killed int := 0;
BEGIN
    FOR v_pid IN 
        SELECT pa.pid 
        FROM pg_stat_activity pa
        WHERE (pa.query ILIKE '%bm25%' OR pa.query ILIKE '%rebuild%' OR pa.query ILIKE '%staging%')
        AND pa.state IN ('active', 'idle', 'idle in transaction')
        AND pa.pid != pg_backend_pid()
        AND now() - pa.query_start > p_min_duration
    LOOP
        BEGIN
            pid := v_pid;
            SELECT now() - query_start INTO duration 
            FROM pg_stat_activity WHERE pg_stat_activity.pid = v_pid;
            
            PERFORM pg_terminate_backend(v_pid);
            status := 'terminated';
            RETURN NEXT;
            v_killed := v_killed + 1;
        EXCEPTION WHEN OTHERS THEN
            status := 'error: ' || SQLERRM;
            RETURN NEXT;
        END;
    END LOOP;
    
    IF v_killed = 0 THEN
        pid := NULL;
        duration := NULL;
        status := format('No stuck processes found (threshold: %s)', p_min_duration);
        RETURN NEXT;
    END IF;
END;
$$;

-- Full cleanup: disconnect dblinks, drop staging tables, kill stuck processes
CREATE OR REPLACE FUNCTION facets.bm25_full_cleanup(p_kill_threshold interval DEFAULT '5 minutes')
RETURNS TABLE(operation text, details text)
LANGUAGE plpgsql AS $$
DECLARE
    v_rec record;
    v_results text[];
BEGIN
    -- 1. Disconnect dblink connections
    operation := 'Disconnect dblinks';
    v_results := ARRAY[]::text[];
    FOR v_rec IN SELECT * FROM facets.bm25_cleanup_dblinks() LOOP
        IF v_rec.connection_name IS NOT NULL THEN
            v_results := array_append(v_results, v_rec.connection_name || ': ' || v_rec.status);
        ELSE
            v_results := array_append(v_results, v_rec.status);
        END IF;
    END LOOP;
    details := array_to_string(v_results, ', ');
    RETURN NEXT;
    
    -- 2. Drop staging tables
    operation := 'Drop staging tables';
    v_results := ARRAY[]::text[];
    FOR v_rec IN SELECT * FROM facets.bm25_cleanup_staging() LOOP
        IF v_rec.table_name IS NOT NULL THEN
            v_results := array_append(v_results, v_rec.table_name || ': ' || v_rec.status);
        ELSE
            v_results := array_append(v_results, v_rec.status);
        END IF;
    END LOOP;
    details := array_to_string(v_results, ', ');
    RETURN NEXT;
    
    -- 3. Kill stuck processes
    operation := 'Kill stuck processes';
    v_results := ARRAY[]::text[];
    FOR v_rec IN SELECT * FROM facets.bm25_kill_stuck(p_kill_threshold) LOOP
        IF v_rec.pid IS NOT NULL THEN
            v_results := array_append(v_results, 'pid ' || v_rec.pid::text || ': ' || v_rec.status);
        ELSE
            v_results := array_append(v_results, v_rec.status);
        END IF;
    END LOOP;
    details := array_to_string(v_results, ', ');
    RETURN NEXT;
    
    -- 4. Show current status
    operation := 'Current status';
    SELECT string_agg(
        s.table_name || ': ' || s.documents_indexed || ' docs', 
        ', '
    ) INTO details
    FROM facets.bm25_status() s
    WHERE s.table_name != 'No BM25 indexes found';
    
    IF details IS NULL THEN
        details := 'No BM25 indexes';
    END IF;
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- SECTION: SIMPLIFIED SETUP FUNCTION
-- One-stop setup for facets + BM25 indexing
-- ============================================================================

CREATE OR REPLACE FUNCTION facets.setup_table_with_bm25(
    p_table regclass,
    p_id_column text DEFAULT 'id',
    p_content_column text DEFAULT 'content',
    p_facets facets.facet_definition[] DEFAULT NULL,
    p_language text DEFAULT 'english',
    p_create_trigger boolean DEFAULT true,
    p_chunk_bits int DEFAULT NULL,
    p_populate_facets boolean DEFAULT true,
    p_build_bm25_index boolean DEFAULT true,
    p_bm25_workers int DEFAULT 0  -- 0 = auto
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_effective_facets facets.facet_definition[];
BEGIN
    v_table_oid := p_table::oid;
    
    RAISE NOTICE '[SETUP] Starting setup for table %', p_table;
    
    -- Use provided facets or default to empty array
    v_effective_facets := COALESCE(p_facets, ARRAY[]::facets.facet_definition[]);
    
    -- Step 1: Add faceting to the table
    RAISE NOTICE '[SETUP] Adding faceting to table...';
    PERFORM facets.add_faceting_to_table(
        p_table,
        p_id_column::name,
        v_effective_facets,
        p_chunk_bits,
        true,  -- keep_deltas
        p_populate_facets
    );
    
    -- Step 2: Set BM25 language
    RAISE NOTICE '[SETUP] Setting BM25 language to %', p_language;
    PERFORM facets.bm25_set_language(p_table, p_language);
    
    -- Step 3: Create BM25 sync trigger if requested
    IF p_create_trigger THEN
        RAISE NOTICE '[SETUP] Creating BM25 sync trigger...';
        PERFORM facets.bm25_create_sync_trigger(p_table, p_id_column, p_content_column, p_language);
    END IF;
    
    -- Step 4: Build BM25 index if requested
    IF p_build_bm25_index THEN
        RAISE NOTICE '[SETUP] Building BM25 index...';
        PERFORM facets.bm25_rebuild_index(
            p_table,
            p_id_column,
            p_content_column,
            p_language,
            p_bm25_workers
        );
    END IF;
    
    RAISE NOTICE '[SETUP] Setup complete for %', p_table;
END;
$$;

-- Log version activation
DO $$
BEGIN
    RAISE NOTICE 'pg_facets upgraded to version 0.4.2';
END $$;

