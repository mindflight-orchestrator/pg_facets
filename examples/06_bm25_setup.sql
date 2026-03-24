-- 06_bm25_setup.sql
-- BM25 indexing setup for IMDB titles + names using pg_facets 0.4.2+ helper functions.
--
-- PERFORMANCE OPTIMIZATIONS (pg_facets 0.4.3+):
-- ============================================
-- This file uses parallel BM25 indexing for fast indexation (3-4x faster):
--   - Default: 4 parallel workers (configurable)
--   - Requires dblink extension (auto-detected, falls back to sequential if not available)
--   - For very large datasets (10M+ docs), use 8-16 workers
--
-- This complements the existing faceting tables created in 04_faceting_setup.sql:
--   - providers_imdb.title_basics_faceting_mv
--   - providers_imdb.name_basics_faceting_mv
--
-- IMPORTANT - pg_facets 0.4.2+ REQUIRED
-- - This file uses helper functions from pg_facets 0.4.2+
-- - Run order:
--     1) 04_faceting_setup.sql (creates tables and registers them)
--     2) 06_bm25_setup.sql (sets up BM25 triggers and provides rebuild wrappers)
--     3) (optional) 05_faceting_indexes.sql if you still use tsvector-based search paths
--
-- After this, you can query BM25 directly:
--   SELECT * FROM facets.bm25_search('providers_imdb.title_basics_faceting_mv'::regclass, 'star wars');
--   SELECT * FROM facets.bm25_search('providers_imdb.name_basics_faceting_mv'::regclass, 'harrison ford');
--
-- NOTE: Most BM25 helper functions are now in the core extension (0.4.2+).
-- This file provides project-specific convenience wrappers and trigger setup.
--
-- IMPORTANT: This file does NOT use BEGIN/COMMIT transaction blocks.
-- Each statement runs in its own transaction to allow partial execution
-- if errors occur. This is safer for setup scripts.

-- Ensure application schema exists (your tables live under this schema).
CREATE SCHEMA IF NOT EXISTS providers_imdb;

-- Ensure required extensions exist (safe if already installed).
-- If pg_facets was installed via CREATE EXTENSION, this will be a no-op.
CREATE EXTENSION IF NOT EXISTS pg_facets;

-- Optional (enables fuzzy/prefix helpers inside facets.bm25_search when requested).
-- If you don't want pg_trgm, you can comment this out.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Optional (enables true parallel BM25 rebuild via facets.bm25_index_documents_parallel()).
-- If you don't have permissions to install it, you can comment this out and the rebuild will fall back to sequential.
CREATE EXTENSION IF NOT EXISTS dblink;

-- =============================================================================
-- BM25 sync triggers (using 0.4.2+ helper functions)
-- =============================================================================
-- 
-- With pg_facets 0.4.2+, we can use facets.bm25_create_sync_trigger() instead
-- of manually creating trigger functions. This is simpler and more maintainable.
--
-- The helper function automatically:
-- - Creates the trigger function
-- - Creates the trigger
-- - Handles INSERT/UPDATE/DELETE operations
-- - Uses the table's bm25_language setting (or 'english' as default)

-- Create BM25 sync triggers for both tables (if they exist)
DO $$
BEGIN
    IF to_regclass('providers_imdb.title_basics_faceting_mv') IS NOT NULL THEN
        PERFORM facets.bm25_create_sync_trigger(
            'providers_imdb.title_basics_faceting_mv'::regclass,
            'id',        -- id_column
            'content',   -- content_column
            'english'    -- language (or NULL to use table's bm25_language)
        );
        RAISE NOTICE 'Created BM25 sync trigger for title_basics_faceting_mv';
    ELSE
        RAISE NOTICE 'Skipping title_basics_faceting_mv (table does not exist yet - run 04_faceting_setup.sql first)';
    END IF;
    
    IF to_regclass('providers_imdb.name_basics_faceting_mv') IS NOT NULL THEN
        PERFORM facets.bm25_create_sync_trigger(
            'providers_imdb.name_basics_faceting_mv'::regclass,
            'id',        -- id_column
            'content',   -- content_column
            'english'    -- language (or NULL to use table's bm25_language)
        );
        RAISE NOTICE 'Created BM25 sync trigger for name_basics_faceting_mv';
    ELSE
        RAISE NOTICE 'Skipping name_basics_faceting_mv (table does not exist yet - run 04_faceting_setup.sql first)';
    END IF;
END
$$;

-- =============================================================================
-- NOTE: Parallel Indexing Functions Now in Core Extension (0.4.2+)
-- =============================================================================
--
-- The following functions are now part of pg_facets 0.4.2+ and do not need to be
-- defined here:
--   - facets.bm25_index_worker_lockfree()      (now in core)
--   - facets.bm25_cleanup_staging_tables()     (now in core)
--   - facets.bm25_index_documents_parallel()  (optimized version now in core)
--
-- The core version uses the same lock-free staging table approach and is 90-95%
-- faster than the old OFFSET-based method.
--
-- To use parallel indexing, simply call:
--   SELECT * FROM facets.bm25_index_documents_parallel(
--       'providers_imdb.title_basics_faceting_mv'::regclass,
--       'SELECT id::bigint AS doc_id, content FROM providers_imdb.title_basics_faceting_mv',
--       'content',
--       'english',
--       4  -- num_workers
--   );
--
-- Or use the rebuild helper:
--   SELECT facets.bm25_rebuild_index(
--       'providers_imdb.title_basics_faceting_mv'::regclass,
--       'id',
--       'content',
--       'english',
--       4  -- num_workers (0 = auto)
--   );

-- =============================================================================
-- Full rebuild helpers (fastest/cleanest way to initialize BM25 at scale)
-- =============================================================================
--
-- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+):
-- - Parallel indexing with multiple workers (default: 4) provides 3-4x faster indexing
-- - Requires dblink extension for parallel mode (auto-detected, falls back to sequential if not available)
-- - For very large datasets (10M+ documents), use 8-16 workers on systems with sufficient CPU/RAM
--
-- Example usage for maximum performance:
--   SELECT providers_imdb.rebuild_title_basics_bm25(8);  -- 8 parallel workers
--
--
-- BM25 INDEX STORAGE AND SCHEMA SAFETY
-- =============================================================================
-- BM25 indexes are stored in the 'facets' schema in three tables:
--   1. facets.bm25_index      - Inverted index (term → documents mapping)
--   2. facets.bm25_documents  - Document metadata (doc_id, doc_length, timestamps)
--   3. facets.bm25_statistics  - Collection statistics (total_documents, avg_document_length)
--
-- SCHEMA UPDATE SAFETY:
--   - SAFE: ALTER statements, function updates, adding columns - BM25 data will remain
--   - SAFE: Updates to providers_imdb schema - BM25 data is in facets schema
--   - UNSAFE: DROP SCHEMA facets - will delete all BM25 data
--   - UNSAFE: DROP TABLE from facets.faceted_table - will CASCADE delete BM25 data
--   - The BM25 tables use CREATE TABLE IF NOT EXISTS, so schema updates won't drop them
--
-- BACKUP STRATEGY:
--   Before major schema changes, backup BM25 data:
--     pg_dump -t facets.bm25_index -t facets.bm25_documents -t facets.bm25_statistics
-- =============================================================================

-- =============================================================================
-- Project-Specific Rebuild Wrapper (uses core facets.bm25_rebuild_index)
-- =============================================================================
-- 
-- This is a thin wrapper around facets.bm25_rebuild_index() that provides
-- project-specific defaults and convenience. The core function handles:
-- - Parallel vs sequential mode detection
-- - Progress reporting
-- - Statistics recalculation
-- - Verification

CREATE OR REPLACE FUNCTION providers_imdb.rebuild_bm25_index_for_table(
    p_table regclass,
    p_id_column text DEFAULT 'id',
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT 'english',
    p_num_workers int DEFAULT 4,            -- PERFORMANCE: Default to 4 workers for parallel indexing (0 = auto, 1 = sequential, >1 = parallel requires dblink)
    p_connection_string text DEFAULT NULL, -- only used for parallel mode; NULL uses current DB
    p_progress_step_size int DEFAULT 50000 -- Progress reporting frequency (default: every 50K docs)
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Simply delegate to the core function (available in 0.4.2+)
    PERFORM facets.bm25_rebuild_index(
        p_table,
        p_id_column,
        p_content_column,
        p_language,
        p_num_workers,
        p_connection_string,
        p_progress_step_size
    );
END;
$$;

-- Drop old versions if they exist (to avoid function signature conflicts)
DROP FUNCTION IF EXISTS providers_imdb.rebuild_title_basics_bm25();
DROP FUNCTION IF EXISTS providers_imdb.rebuild_title_basics_bm25(int);
DROP FUNCTION IF EXISTS providers_imdb.rebuild_title_basics_bm25(int, text);
DROP FUNCTION IF EXISTS providers_imdb.rebuild_title_basics_bm25(int, text, int);

DROP FUNCTION IF EXISTS providers_imdb.rebuild_name_basics_bm25();
DROP FUNCTION IF EXISTS providers_imdb.rebuild_name_basics_bm25(int);
DROP FUNCTION IF EXISTS providers_imdb.rebuild_name_basics_bm25(int, text);
DROP FUNCTION IF EXISTS providers_imdb.rebuild_name_basics_bm25(int, text, int);

CREATE OR REPLACE FUNCTION providers_imdb.rebuild_title_basics_bm25(
    p_num_workers int DEFAULT 4,  -- PERFORMANCE: Default to 4 workers for parallel indexing (0 = auto, 1 = sequential, >1 = parallel)
    p_connection_string text DEFAULT NULL,
    p_progress_step_size int DEFAULT 50000
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM providers_imdb.rebuild_bm25_index_for_table(
        'providers_imdb.title_basics_faceting_mv'::regclass,
        'id',
        'content',
        'english',
        p_num_workers,
        p_connection_string,
        p_progress_step_size
    );
END;
$$;

CREATE OR REPLACE FUNCTION providers_imdb.rebuild_name_basics_bm25(
    p_num_workers int DEFAULT 4,  -- PERFORMANCE: Default to 4 workers for parallel indexing (0 = auto, 1 = sequential, >1 = parallel)
    p_connection_string text DEFAULT NULL,
    p_progress_step_size int DEFAULT 50000
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM providers_imdb.rebuild_bm25_index_for_table(
        'providers_imdb.name_basics_faceting_mv'::regclass,
        'id',
        'content',
        'english',
        p_num_workers,
        p_connection_string,
        p_progress_step_size
    );
END;
$$;

-- Convenience wrappers: refresh faceting table then rebuild BM25.
-- Uses dynamic EXECUTE so this file can be loaded even if 04 hasn't been run yet.

-- Drop old versions if they exist (to avoid function signature conflicts)
DROP FUNCTION IF EXISTS providers_imdb.refresh_title_facets_and_bm25(boolean);
DROP FUNCTION IF EXISTS providers_imdb.refresh_title_facets_and_bm25(boolean, int);
DROP FUNCTION IF EXISTS providers_imdb.refresh_title_facets_and_bm25(boolean, int, text);
DROP FUNCTION IF EXISTS providers_imdb.refresh_title_facets_and_bm25(boolean, int, text, int);

DROP FUNCTION IF EXISTS providers_imdb.refresh_name_facets_and_bm25(boolean);
DROP FUNCTION IF EXISTS providers_imdb.refresh_name_facets_and_bm25(boolean, int);
DROP FUNCTION IF EXISTS providers_imdb.refresh_name_facets_and_bm25(boolean, int, text);
DROP FUNCTION IF EXISTS providers_imdb.refresh_name_facets_and_bm25(boolean, int, text, int);

CREATE OR REPLACE FUNCTION providers_imdb.refresh_title_facets_and_bm25(
    p_resume boolean DEFAULT false,
    p_bm25_num_workers int DEFAULT 4,  -- PERFORMANCE: Default to 4 workers for parallel BM25 indexing
    p_bm25_connection_string text DEFAULT NULL,
    p_bm25_progress_step_size int DEFAULT 50000
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF to_regprocedure('providers_imdb.refresh_title_basics_faceting(boolean)') IS NULL THEN
        RAISE EXCEPTION 'providers_imdb.refresh_title_basics_faceting(boolean) not found. Run 04_faceting_setup.sql first.';
    END IF;

    EXECUTE 'SELECT providers_imdb.refresh_title_basics_faceting($1)' USING p_resume;
    PERFORM providers_imdb.rebuild_title_basics_bm25(p_bm25_num_workers, p_bm25_connection_string, p_bm25_progress_step_size);
END;
$$;

CREATE OR REPLACE FUNCTION providers_imdb.refresh_name_facets_and_bm25(
    p_resume boolean DEFAULT false,
    p_bm25_num_workers int DEFAULT 4,  -- PERFORMANCE: Default to 4 workers for parallel BM25 indexing
    p_bm25_connection_string text DEFAULT NULL,
    p_bm25_progress_step_size int DEFAULT 50000
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF to_regprocedure('providers_imdb.refresh_name_basics_faceting(boolean)') IS NULL THEN
        RAISE EXCEPTION 'providers_imdb.refresh_name_basics_faceting(boolean) not found. Run 04_faceting_setup.sql first.';
    END IF;

    EXECUTE 'SELECT providers_imdb.refresh_name_basics_faceting($1)' USING p_resume;
    PERFORM providers_imdb.rebuild_name_basics_bm25(p_bm25_num_workers, p_bm25_connection_string, p_bm25_progress_step_size);
END;
$$;

-- =============================================================================
-- BM25 Monitoring and Cleanup Functions (Now in Core Extension 0.4.2+)
-- =============================================================================
--
-- The following monitoring and cleanup functions are now part of pg_facets 0.4.2+
-- and are available directly from the core extension:
--
--   - facets.bm25_status()              -- Check index status for all tables
--   - facets.bm25_progress(table)      -- Check indexing progress
--   - facets.bm25_active_processes()   -- List running BM25 processes
--   - facets.bm25_cleanup_dblinks()    -- Disconnect stuck dblink connections
--   - facets.bm25_cleanup_staging()    -- Drop orphaned staging tables
--   - facets.bm25_kill_stuck(interval) -- Kill processes running longer than interval
--   - facets.bm25_full_cleanup(interval) -- All cleanup operations combined
--
-- Usage examples:
--   SELECT * FROM facets.bm25_status();
--   SELECT * FROM facets.bm25_progress('providers_imdb.title_basics_faceting_mv'::regclass);
--   SELECT * FROM facets.bm25_active_processes();
--   SELECT * FROM facets.bm25_full_cleanup('5 minutes'::interval);
--
-- These functions no longer need to be defined in this file.
-- =============================================================================
