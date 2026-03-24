-- 04_faceting_setup.sql
-- Setup faceting for IMDB titles and names
-- 
-- PERFORMANCE OPTIMIZATIONS (pg_facets 0.4.3+):
-- ============================================
-- This file uses UNLOGGED tables for fast bulk loading (2-3x faster):
--   1. Tables created as UNLOGGED (no WAL writes during bulk load)
--   2. Bulk populate (FAST - no WAL overhead)
--   3. Convert to LOGGED after load (one-time WAL write for durability)
--   4. Refresh functions also use UNLOGGED → LOGGED workflow
--
-- NOTE: For NEW projects using pg_facets 0.4.2+, consider using the simplified
-- one-stop setup function instead:
--   SELECT facets.setup_table_with_bm25(
--       'providers_imdb.title_basics_faceting_mv'::regclass,
--       'id', 'content',
--       ARRAY[facets.plain_facet('title_type'), ...],
--       'english', true, NULL, true, true, 0
--   );
--
-- This file shows the manual setup approach for reference and projects that
-- need custom refresh logic or progress tracking.

-- ============================================================================
-- SETUP NOTES: Content Column for facets.search_documents_with_facets()
-- ============================================================================
-- Before calling facets.search_documents_with_facets(), ensure proper setup:
--
-- 1. Content Column Name
--    - Both tables use 'content' as the column name for full-text search
--    - Title table: providers_imdb.title_basics_faceting_mv.content
--    - Name table: providers_imdb.name_basics_faceting_mv.content
--    - This column is created in the CREATE TABLE statements below
--
-- 2. GIN Index Requirement
--    - MUST create GIN index on to_tsvector('english', content) before searching
--    - See 05_faceting_indexes.sql for the index creation statements
--    - Without this index, search_documents_with_facets() will be extremely slow
--
-- 3. API Configuration
--    - The API must pass ContentColumn: "content" in SearchWithFacetsRequest
--    - This matches the p_content_column parameter in search_documents_with_facets()
--    - Default value is 'content' if not specified
--
-- 4. Function Signature Reference
--    facets.search_documents_with_facets(
--        p_schema_name text,
--        p_table_name text,
--        p_query text,
--        p_facets jsonb DEFAULT NULL,
--        p_vector_column text DEFAULT NULL,
--        p_content_column text DEFAULT 'content',  -- <-- This parameter
--        p_metadata_column text DEFAULT 'metadata',
--        p_created_at_column text DEFAULT 'created_at',
--        p_updated_at_column text DEFAULT 'updated_at',
--        p_limit integer DEFAULT 10,
--        p_offset integer DEFAULT 0,
--        p_min_score double precision DEFAULT 0.0,
--        p_vector_weight double precision DEFAULT 0.5,
--        p_facet_limit integer DEFAULT 5
--    )
--
-- Setup Order:
--   1. Run this file (04_faceting_setup.sql) - creates tables with 'content' column
--   2. Run 05_faceting_indexes.sql - creates GIN indexes on content column
--   3. Call facets.search_documents_with_facets() with p_content_column='content'
-- ============================================================================

-- ============================================================================
-- Progress Tracking Table
-- ============================================================================
-- Create a table to track refresh progress and enable resume capability
CREATE TABLE IF NOT EXISTS providers_imdb.faceting_refresh_progress (
    table_name TEXT PRIMARY KEY,
    phase TEXT NOT NULL,  -- 'preparing', 'inserting', 'merging', 'completed', 'failed'
    total_rows BIGINT,
    processed_rows BIGINT DEFAULT 0,
    batch_offset BIGINT DEFAULT 0,
    started_at TIMESTAMPTZ,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    estimated_completion TIMESTAMPTZ,
    error_message TEXT
);

-- Function to update progress
CREATE OR REPLACE FUNCTION providers_imdb.update_faceting_progress(
    p_table_name TEXT,
    p_phase TEXT,
    p_total_rows BIGINT DEFAULT NULL,
    p_processed_rows BIGINT DEFAULT NULL,
    p_batch_offset BIGINT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS void AS $$
BEGIN
    INSERT INTO providers_imdb.faceting_refresh_progress (
        table_name, phase, total_rows, processed_rows, batch_offset, 
        started_at, last_updated, error_message
    )
    VALUES (
        p_table_name, p_phase, p_total_rows, p_processed_rows, p_batch_offset,
        COALESCE((SELECT started_at FROM providers_imdb.faceting_refresh_progress WHERE table_name = p_table_name), NOW()),
        NOW(), p_error_message
    )
    ON CONFLICT (table_name) DO UPDATE SET
        phase = EXCLUDED.phase,
        total_rows = COALESCE(EXCLUDED.total_rows, faceting_refresh_progress.total_rows),
        processed_rows = COALESCE(EXCLUDED.processed_rows, faceting_refresh_progress.processed_rows),
        batch_offset = COALESCE(EXCLUDED.batch_offset, faceting_refresh_progress.batch_offset),
        last_updated = NOW(),
        error_message = EXCLUDED.error_message,
        estimated_completion = CASE 
            WHEN EXCLUDED.processed_rows > 0 AND EXCLUDED.total_rows > 0 THEN
                NOW() + (NOW() - COALESCE(faceting_refresh_progress.started_at, NOW())) * 
                (EXCLUDED.total_rows::NUMERIC / EXCLUDED.processed_rows - 1)
            ELSE NULL
        END;
END;
$$ LANGUAGE plpgsql;

-- Function to get progress status by counting documents using bitmap cardinality
-- This counts documents from the facets table bitmaps, which is more accurate
-- during merge_deltas phase since it reflects what's actually indexed
CREATE OR REPLACE FUNCTION providers_imdb.get_faceting_progress(p_table_name TEXT)
RETURNS TABLE (
    table_name TEXT,
    phase TEXT,
    total_rows BIGINT,
    processed_rows BIGINT,
    progress_pct NUMERIC,
    status_message TEXT
) AS $$
DECLARE
    v_actual_count BIGINT;
    v_expected_count BIGINT;
    v_progress_pct NUMERIC;
    v_phase TEXT;
    v_facets_table TEXT;
    v_table_oid OID;
BEGIN
    -- Determine table names and OID
    IF p_table_name = 'title_basics_faceting_mv' THEN
        v_facets_table := 'providers_imdb.title_basics_faceting_mv_facets';
        v_table_oid := 'providers_imdb.title_basics_faceting_mv'::regclass::oid;
        SELECT COUNT(*) INTO v_expected_count FROM providers_imdb.title_basics WHERE tconst IS NOT NULL;
    ELSIF p_table_name = 'name_basics_faceting_mv' THEN
        v_facets_table := 'providers_imdb.name_basics_faceting_mv_facets';
        v_table_oid := 'providers_imdb.name_basics_faceting_mv'::regclass::oid;
        SELECT COUNT(*) INTO v_expected_count FROM providers_imdb.name_basics WHERE nconst IS NOT NULL;
    ELSE
        RETURN; -- Unknown table
    END IF;
    
    -- Count documents using bitmap cardinality from facets table
    -- OR all postinglists together to get the union of all document IDs
    -- Use a simple iterative OR aggregation
    BEGIN
        -- Try to get count from facets table using bitmap union
        -- We'll build the union by iteratively ORing bitmaps
        EXECUTE format('
            WITH RECURSIVE bitmap_union AS (
                -- Start with first bitmap
                SELECT postinglist as result, 1 as level
                FROM %I 
                WHERE postinglist IS NOT NULL
                LIMIT 1
                UNION ALL
                -- OR with next bitmap
                SELECT rb_or(bu.result, fv.postinglist) as result, bu.level + 1
                FROM bitmap_union bu
                CROSS JOIN LATERAL (
                    SELECT postinglist
                    FROM %I
                    WHERE postinglist IS NOT NULL
                    OFFSET bu.level
                    LIMIT 1
                ) fv
            )
            SELECT rb_cardinality(result)
            FROM bitmap_union
            ORDER BY level DESC
            LIMIT 1
        ', v_facets_table, v_facets_table) INTO v_actual_count;
        
        -- If no result or NULL, set to 0
        IF v_actual_count IS NULL THEN
            v_actual_count := 0;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- If facets table doesn't exist or query fails, fall back to table count
            v_actual_count := 0;
    END;
    
    -- Fallback: if facets table doesn't exist or is empty, count from main table
    IF v_actual_count = 0 THEN
        IF p_table_name = 'title_basics_faceting_mv' THEN
            SELECT COUNT(*) INTO v_actual_count FROM providers_imdb.title_basics_faceting_mv;
        ELSIF p_table_name = 'name_basics_faceting_mv' THEN
            SELECT COUNT(*) INTO v_actual_count FROM providers_imdb.name_basics_faceting_mv;
        END IF;
    END IF;
    
    -- Calculate progress percentage
    IF v_expected_count > 0 THEN
        v_progress_pct := ROUND((v_actual_count::NUMERIC / v_expected_count * 100)::NUMERIC, 2);
    ELSE
        v_progress_pct := 0;
    END IF;
    
    -- Determine phase based on progress
    IF v_actual_count = 0 THEN
        v_phase := 'not_started';
    ELSIF v_progress_pct < 100 THEN
        -- Check if merge_deltas is running
        IF EXISTS (
            SELECT 1 FROM pg_stat_activity 
            WHERE query LIKE '%merge_deltas%' 
            AND query LIKE '%' || p_table_name || '%'
            AND state != 'idle'
        ) THEN
            v_phase := 'merging';
        ELSE
            v_phase := 'inserting';
        END IF;
    ELSE
        -- Check if merge_deltas is still running
        IF EXISTS (
            SELECT 1 FROM pg_stat_activity 
            WHERE query LIKE '%merge_deltas%' 
            AND query LIKE '%' || p_table_name || '%'
            AND state != 'idle'
        ) THEN
            v_phase := 'merging';
        ELSE
            v_phase := 'completed';
        END IF;
    END IF;
    
    -- Return results
    RETURN QUERY
    SELECT 
        p_table_name,
        v_phase,
        v_expected_count,
        v_actual_count,
        v_progress_pct,
        CASE v_phase
            WHEN 'not_started' THEN 'Faceting refresh has not started yet'
            WHEN 'inserting' THEN format('Inserting data: %s / %s rows (%.1f%%)', 
                v_actual_count, v_expected_count, v_progress_pct)
            WHEN 'merging' THEN format('Merging deltas: %s / %s rows indexed in facets (%.1f%%). Merge phase can take hours for large datasets.', 
                v_actual_count, v_expected_count, v_progress_pct)
            WHEN 'completed' THEN format('Completed: %s / %s rows indexed (100%%)', 
                v_actual_count, v_expected_count)
            ELSE 'Unknown status'
        END as status_message;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Title Basics Faceting Setup
-- ============================================================================

-- Drop existing table if it exists (changed from materialized view to support triggers)
DROP TABLE IF EXISTS providers_imdb.title_basics_faceting_mv CASCADE;

-- Create table for faceting (changed from materialized view to support triggers for incremental updates)
-- pgfaceting requires: id (integer), content (text), metadata (jsonb), created_at, updated_at
--
-- IMPORTANT: Content Column Setup for facets.search_documents_with_facets()
-- ----------------------------------------------------------------------------
-- The 'content' column is REQUIRED for full-text search via facets.search_documents_with_facets().
-- This column name must match the p_content_column parameter passed to the function
-- (defaults to 'content' if not specified).
--
-- Before calling facets.search_documents_with_facets(), ensure:
-- 1. The 'content' column exists (created below)
-- 2. A GIN index exists on to_tsvector('english', content) - see 05_faceting_indexes.sql
-- 3. The API passes ContentColumn: "content" in SearchWithFacetsRequest
--
-- The search_documents_with_facets function signature:
--   facets.search_documents_with_facets(
--       p_schema_name, p_table_name, p_query, p_facets,
--       p_vector_column, p_content_column, ...  -- p_content_column defaults to 'content'
--   )
-- ----------------------------------------------------------------------------
CREATE TABLE providers_imdb.title_basics_faceting_mv AS
SELECT 
    -- Use stable hash-based ID from tconst (pgfaceting requires integer IDs)
    -- This ensures IDs remain stable across refreshes
    ABS(HASHTEXT(tb.tconst))::INTEGER AS id,
    
    -- document_id column alias (required by facets.search_documents_with_facets)
    -- This is an alias for id to match the expected column name
    ABS(HASHTEXT(tb.tconst))::INTEGER AS document_id,
    
    -- Content for full-text search (combine primaryTitle, originalTitle, and genres)
    -- This column is used by facets.search_documents_with_facets() for BM25 search
    -- Column name: 'content' (must match p_content_column parameter in search_documents_with_facets)
    COALESCE(
        tb.primaryTitle || ' ' || 
        COALESCE(tb.originalTitle, '') || ' ' || 
        COALESCE(tb.genres, ''),
        ''
    ) AS content,
    
    -- Metadata as JSONB (store all title data here)
    jsonb_build_object(
        'tconst', tb.tconst,
        'titleType', tb.titleType,
        'primaryTitle', tb.primaryTitle,
        'originalTitle', tb.originalTitle,
        'isAdult', COALESCE(tb.isAdult, false),
        'startYear', tb.startYear,
        'endYear', tb.endYear,
        'runtimeMinutes', tb.runtimeMinutes,
        'genres', tb.genres,
        'averageRating', COALESCE(tr.averageRating, 0),
        'numVotes', COALESCE(tr.numVotes, 0),
        'region', region_data.region
    ) AS metadata,
    
    -- Timestamps (use startYear if available, otherwise current timestamp)
    COALESCE(
        make_timestamp(COALESCE(tb.startYear, 1900), 1, 1, 0, 0, 0),
        CURRENT_TIMESTAMP
    ) AS created_at,
    
    CURRENT_TIMESTAMP AS updated_at,
    
    -- Reference to original table (for joins and foreign keys)
    tb.tconst AS tconst,
    
    -- Facet columns (extracted for efficient faceting)
    tb.titleType AS title_type,
    tb.isAdult AS is_adult,
    tb.startYear AS start_year,
    COALESCE(tr.averageRating, 0) AS average_rating,
    COALESCE(tr.numVotes, 0) AS num_votes,
    -- For genres, we'll handle comma-separated values in a separate approach
    -- Store first genre for simple faceting (can be expanded later)
    CASE 
        WHEN tb.genres IS NOT NULL AND tb.genres != '' THEN 
            TRIM(SPLIT_PART(tb.genres, ',', 1))
        ELSE NULL
    END AS primary_genre,
    -- Region facet (get first non-null region from title_akas)
    region_data.region AS region
    
FROM providers_imdb.title_basics tb
LEFT JOIN providers_imdb.title_ratings tr ON tb.tconst = tr.tconst
LEFT JOIN LATERAL (
    SELECT ta.region
    FROM providers_imdb.title_akas ta
    WHERE ta.titleId = tb.tconst
      AND ta.region IS NOT NULL
      AND ta.region != ''
    ORDER BY ta.ordering
    LIMIT 1
) region_data ON TRUE
WHERE tb.tconst IS NOT NULL;

-- Add document_id column if it doesn't exist (for compatibility with facets.search_documents_with_facets)
-- This column is required because filter_documents_by_facets returns document_id, not id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'providers_imdb' 
        AND table_name = 'title_basics_faceting_mv' 
        AND column_name = 'document_id'
    ) THEN
        ALTER TABLE providers_imdb.title_basics_faceting_mv 
        ADD COLUMN document_id INTEGER GENERATED ALWAYS AS (id) STORED;
        RAISE NOTICE 'Added document_id column to title_basics_faceting_mv';
    END IF;
END $$;

-- Add faceting to the table
-- Following the RAG pattern: add_faceting_to_table(table, id_column, facets[], chunk_bits, keep_deltas, populate, skip_table_creation, unlogged)
-- 
-- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+):
-- Using unlogged => true for initial bulk load provides 2-3x faster performance by bypassing WAL writes.
-- After bulk load, we convert to LOGGED for durability.
SELECT facets.add_faceting_to_table(
    'providers_imdb.title_basics_faceting_mv'::regclass,
    'id',
    ARRAY[
        -- Plain facets (simple column values)
        facets.plain_facet('title_type'),
        facets.plain_facet('primary_genre'),
        facets.plain_facet('region'),
        
        -- Boolean facet
        facets.boolean_facet('is_adult'),
        
        -- Bucket facet for start year (group into decades/periods)
        facets.bucket_facet('start_year', ARRAY[
            1880::float8, 1900::float8, 1920::float8, 1940::float8, 1960::float8, 
            1980::float8, 2000::float8, 2010::float8, 2015::float8, 2020::float8, 2025::float8
        ]),
        
        -- Bucket facet for average rating (group into rating ranges)
        facets.bucket_facet('average_rating', ARRAY[
            0.0::float8, 1.0::float8, 2.0::float8, 3.0::float8, 4.0::float8, 
            5.0::float8, 6.0::float8, 7.0::float8, 8.0::float8, 9.0::float8, 10.0::float8
        ]),
        
        -- Bucket facet for number of votes (popularity ranges)
        facets.bucket_facet('num_votes', ARRAY[
            0::float8, 10::float8, 100::float8, 1000::float8, 10000::float8, 
            100000::float8, 1000000::float8
        ])
    ]::facets.facet_definition[],
    20,   -- chunk_bits (2^20 = ~1M documents per chunk)
    TRUE, -- keep_deltas (for incremental updates)
    TRUE, -- populate (initial population - this may take a while for large datasets)
    FALSE, -- skip_table_creation
    TRUE  -- unlogged (FAST bulk load - 2-3x faster, no WAL writes)
);

-- Convert to LOGGED for durability after initial bulk load
-- This writes to WAL once (can take 5-15 min for large tables) but ensures durability
DO $$
BEGIN
    RAISE NOTICE 'Converting facets tables to LOGGED for durability...';
    PERFORM facets.set_table_logged('providers_imdb.title_basics_faceting_mv'::regclass, p_include_deltas => true);
    RAISE NOTICE 'Facets tables are now LOGGED (durable).';
END $$;



-- Create a function to refresh the table and merge deltas
-- Optimized with batch processing, trigger management, optimized joins, and session settings
-- Supports resume capability and progress tracking
CREATE OR REPLACE FUNCTION providers_imdb.refresh_title_basics_faceting(
    p_resume BOOLEAN DEFAULT FALSE  -- If true, skip insert phase if data already exists
)
RETURNS void AS $$
DECLARE
    batch_size INTEGER := 100000;  -- Process 100K rows per batch
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_offset BIGINT := 0;
    rows_inserted INTEGER;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    elapsed_seconds NUMERIC;
    estimated_total_seconds NUMERIC;
    existing_row_count BIGINT;
    function_start_time TIMESTAMP := clock_timestamp();
    table_name TEXT := 'title_basics_faceting_mv';
    merge_start_time TIMESTAMP;
    merge_elapsed INTERVAL;
BEGIN
    -- Optimize session settings for bulk operations (optimized for 32 CPUs, 128GB RAM)
    SET LOCAL work_mem = '512MB';                    -- Increased for large sorts/joins
    SET LOCAL maintenance_work_mem = '4GB';         -- Increased for index operations
    SET LOCAL max_parallel_workers_per_gather = 16;  -- Increased for parallel processing
    SET LOCAL max_parallel_workers = 32;             -- Match CPU count
    
    -- Initialize progress tracking
    PERFORM providers_imdb.update_faceting_progress(table_name, 'preparing', NULL, NULL, NULL);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting refresh_title_basics_faceting() at %', clock_timestamp();
    RAISE NOTICE 'Resume mode: %', p_resume;
    RAISE NOTICE '========================================';
    
    -- Pre-aggregate region data to avoid expensive LATERAL JOIN per row
    RAISE NOTICE 'Pre-aggregating region data from title_akas...';
    DROP TABLE IF EXISTS region_lookup;
    CREATE TEMP TABLE region_lookup AS
    SELECT DISTINCT ON (titleId) 
        titleId, 
        region
    FROM providers_imdb.title_akas
    WHERE region IS NOT NULL 
      AND region != ''
    ORDER BY titleId, ordering;
    
    CREATE INDEX region_lookup_titleId_idx ON region_lookup(titleId);
    RAISE NOTICE 'Region data pre-aggregated';
    
    -- Get total row count for progress tracking
    SELECT COUNT(*) INTO total_rows
    FROM providers_imdb.title_basics
    WHERE tconst IS NOT NULL;
    
    -- Check if we should resume
    SELECT COUNT(*) INTO existing_row_count
    FROM providers_imdb.title_basics_faceting_mv;
    
    RAISE NOTICE 'Total rows to process: %', total_rows;
    RAISE NOTICE 'Existing rows in faceting table: %', existing_row_count;
    
    -- Resume logic: if resume mode and data exists, skip insert phase
    IF p_resume AND existing_row_count > 0 AND existing_row_count >= total_rows * 0.9 THEN
        RAISE NOTICE 'Resume mode: Data already exists (%.0f%% complete), skipping insert phase', 
            (existing_row_count::NUMERIC / total_rows * 100);
        processed_rows := existing_row_count;
        batch_offset := existing_row_count;
        PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
    ELSE
        -- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+): Convert to UNLOGGED for fast bulk load
        RAISE NOTICE 'Converting facets tables to UNLOGGED for fast bulk load (2-3x faster)...';
        PERFORM facets.set_table_unlogged('providers_imdb.title_basics_faceting_mv'::regclass, p_include_deltas => true);
        
        -- Truncate the table (unless resuming)
        IF NOT p_resume OR existing_row_count = 0 THEN
            TRUNCATE TABLE providers_imdb.title_basics_faceting_mv;
            RAISE NOTICE 'Table truncated';
            processed_rows := 0;
            batch_offset := 0;
        ELSE
            -- Resume from where we left off
            RAISE NOTICE 'Resuming from row %', existing_row_count;
            batch_offset := existing_row_count;
            processed_rows := existing_row_count;
        END IF;
        
        -- Disable triggers to avoid per-row trigger overhead during bulk insert
        ALTER TABLE providers_imdb.title_basics_faceting_mv DISABLE TRIGGER ALL;
        RAISE NOTICE 'Triggers disabled for bulk insert';
        
        PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
    
    -- Process in batches using OFFSET
    -- Note: OFFSET becomes slower for later batches, but this is acceptable for a full refresh
    WHILE batch_offset < total_rows LOOP
        batch_start_time := clock_timestamp();
        
        INSERT INTO providers_imdb.title_basics_faceting_mv
        SELECT 
            -- Use stable hash-based ID from tconst (pgfaceting requires integer IDs)
            -- This ensures IDs remain stable across refreshes
            ABS(HASHTEXT(tb.tconst))::INTEGER AS id,
            
            -- document_id column alias (required by facets.search_documents_with_facets)
            -- This is an alias for id to match the expected column name
            ABS(HASHTEXT(tb.tconst))::INTEGER AS document_id,
            
            -- Content for full-text search (combine primaryTitle, originalTitle, and genres)
            COALESCE(
                tb.primaryTitle || ' ' || 
                COALESCE(tb.originalTitle, '') || ' ' || 
                COALESCE(tb.genres, ''),
                ''
            ) AS content,
            
            -- Metadata as JSONB (store all title data here)
            jsonb_build_object(
                'tconst', tb.tconst,
                'titleType', tb.titleType,
                'primaryTitle', tb.primaryTitle,
                'originalTitle', tb.originalTitle,
                'isAdult', COALESCE(tb.isAdult, false),
                'startYear', tb.startYear,
                'endYear', tb.endYear,
                'runtimeMinutes', tb.runtimeMinutes,
                'genres', tb.genres,
                'averageRating', COALESCE(tr.averageRating, 0),
                'numVotes', COALESCE(tr.numVotes, 0),
                'region', rl.region
            ) AS metadata,
            
            -- Timestamps (use startYear if available, otherwise current timestamp)
            COALESCE(
                make_timestamp(COALESCE(tb.startYear, 1900), 1, 1, 0, 0, 0),
                CURRENT_TIMESTAMP
            ) AS created_at,
            
            CURRENT_TIMESTAMP AS updated_at,
            
            -- Reference to original table (for joins and foreign keys)
            tb.tconst AS tconst,
            
            -- Facet columns (extracted for efficient faceting)
            tb.titleType AS title_type,
            tb.isAdult AS is_adult,
            tb.startYear AS start_year,
            COALESCE(tr.averageRating, 0) AS average_rating,
            COALESCE(tr.numVotes, 0) AS num_votes,
            -- For genres, we'll handle comma-separated values in a separate approach
            -- Store first genre for simple faceting (can be expanded later)
            CASE 
                WHEN tb.genres IS NOT NULL AND tb.genres != '' THEN 
                    TRIM(SPLIT_PART(tb.genres, ',', 1))
                ELSE NULL
            END AS primary_genre,
            -- Region facet (from pre-aggregated temp table instead of LATERAL JOIN)
            rl.region AS region
            
        FROM providers_imdb.title_basics tb
        LEFT JOIN providers_imdb.title_ratings tr ON tb.tconst = tr.tconst
        LEFT JOIN region_lookup rl ON rl.titleId = tb.tconst
        WHERE tb.tconst IS NOT NULL
        ORDER BY tb.tconst  -- Consistent ordering for stable batching
        LIMIT batch_size
        OFFSET batch_offset;
        
        GET DIAGNOSTICS rows_inserted = ROW_COUNT;
        
        -- Break if no rows were inserted
        IF rows_inserted = 0 THEN
            EXIT;
        END IF;
        
        batch_offset := batch_offset + rows_inserted;
        processed_rows := batch_offset;
        
        batch_end_time := clock_timestamp();
        elapsed_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        
        -- Update progress tracking
        PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
        
        -- Estimate remaining time
        IF processed_rows > 0 AND elapsed_seconds > 0 AND processed_rows < total_rows THEN
            estimated_total_seconds := (elapsed_seconds / rows_inserted) * (total_rows - processed_rows);
            RAISE NOTICE '[INSERT PHASE] Processed % / % rows (%.1f%%) - Batch took %.1f seconds - Estimated remaining: %.1f seconds (%.1f minutes)',
                processed_rows, total_rows, 
                (processed_rows::NUMERIC / total_rows * 100),
                elapsed_seconds,
                estimated_total_seconds,
                estimated_total_seconds / 60;
        ELSE
            RAISE NOTICE '[INSERT PHASE] Processed % / % rows (%.1f%%) - Batch took %.1f seconds',
                processed_rows, total_rows,
                (processed_rows::NUMERIC / total_rows * 100),
                elapsed_seconds;
        END IF;
    END LOOP;
    
    -- Final insert phase update
    PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
    RAISE NOTICE '[INSERT PHASE] Completed: % / % rows (100%%)', processed_rows, total_rows;
    
    -- Clean up temp table
    DROP TABLE IF EXISTS region_lookup;
    RAISE NOTICE 'Temporary region lookup table dropped';
    
    -- Re-enable triggers
    ALTER TABLE providers_imdb.title_basics_faceting_mv ENABLE TRIGGER ALL;
    RAISE NOTICE 'Triggers re-enabled';
    END IF;
    
    -- Merge deltas once at the end (much faster than per-row trigger updates)
    RAISE NOTICE '========================================';
    RAISE NOTICE '[MERGE PHASE] Starting merge_deltas() at %', clock_timestamp();
    RAISE NOTICE '[MERGE PHASE] This phase can take HOURS for large datasets';
    RAISE NOTICE '[MERGE PHASE] Progress cannot be tracked internally, but checkpoints indicate activity';
    RAISE NOTICE '========================================';
    
    PERFORM providers_imdb.update_faceting_progress(table_name, 'merging', total_rows, processed_rows, batch_offset);
    
    -- Record merge start time for progress estimation
    merge_start_time := clock_timestamp();
    PERFORM facets.merge_deltas('providers_imdb.title_basics_faceting_mv'::regclass);
    
    merge_elapsed := clock_timestamp() - merge_start_time;
    RAISE NOTICE '[MERGE PHASE] Completed in %', merge_elapsed;
    
    -- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+): Convert back to LOGGED for durability
    RAISE NOTICE 'Converting facets tables back to LOGGED for durability...';
    PERFORM facets.set_table_logged('providers_imdb.title_basics_faceting_mv'::regclass, p_include_deltas => true);
    RAISE NOTICE 'Facets tables are now LOGGED (durable).';
    
    -- Mark as completed
    PERFORM providers_imdb.update_faceting_progress(table_name, 'completed', total_rows, processed_rows, batch_offset);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'refresh_title_basics_faceting() completed at %', clock_timestamp();
    RAISE NOTICE 'Total elapsed time: %', clock_timestamp() - function_start_time;
    RAISE NOTICE '========================================';
END;
$$ LANGUAGE plpgsql;

-- Create a helper view for genre faceting (handles comma-separated genres)
-- This creates one row per genre for titles with multiple genres
-- Note: This view can be used for additional genre filtering queries
-- Indexes are not needed on views, but the underlying table is already indexed
CREATE OR REPLACE VIEW providers_imdb.title_genres_expanded AS
SELECT 
    tbf.id,
    tbf.metadata->>'tconst' AS tconst,
    TRIM(unnest(string_to_array(tbf.metadata->>'genres', ','))) AS genre
FROM providers_imdb.title_basics_faceting_mv tbf
WHERE tbf.metadata->>'genres' IS NOT NULL 
  AND tbf.metadata->>'genres' != '';

-- ============================================================================
-- Name Basics Faceting Setup
-- ============================================================================

-- Drop existing table if it exists
DROP TABLE IF EXISTS providers_imdb.name_basics_faceting_mv CASCADE;

-- Create table for name faceting
--
-- IMPORTANT: Content Column Setup for facets.search_documents_with_facets()
-- ----------------------------------------------------------------------------
-- The 'content' column is REQUIRED for full-text search via facets.search_documents_with_facets().
-- This column name must match the p_content_column parameter passed to the function
-- (defaults to 'content' if not specified).
--
-- Before calling facets.search_documents_with_facets(), ensure:
-- 1. The 'content' column exists (created below)
-- 2. A GIN index exists on to_tsvector('english', content) - see 05_faceting_indexes.sql
-- 3. The API passes ContentColumn: "content" in SearchWithFacetsRequest
--
-- The search_documents_with_facets function signature:
--   facets.search_documents_with_facets(
--       p_schema_name, p_table_name, p_query, p_facets,
--       p_vector_column, p_content_column, ...  -- p_content_column defaults to 'content'
--   )
-- ----------------------------------------------------------------------------
CREATE TABLE providers_imdb.name_basics_faceting_mv AS
SELECT 
    -- Use stable hash-based ID from nconst (pgfaceting requires integer IDs)
    -- This ensures IDs remain stable across refreshes
    ABS(HASHTEXT(nb.nconst))::INTEGER AS id,
    
    -- document_id column alias (required by facets.search_documents_with_facets)
    -- This is an alias for id to match the expected column name
    ABS(HASHTEXT(nb.nconst))::INTEGER AS document_id,
    
    -- Content for full-text search (primaryName and primaryProfession)
    -- This column is used by facets.search_documents_with_facets() for BM25 search
    -- Column name: 'content' (must match p_content_column parameter in search_documents_with_facets)
    COALESCE(
        nb.primaryName || ' ' || 
        COALESCE(nb.primaryProfession, ''),
        ''
    ) AS content,
    
    -- Metadata as JSONB (store all name data here)
    jsonb_build_object(
        'nconst', nb.nconst,
        'primaryName', nb.primaryName,
        'birthYear', nb.birthYear,
        'deathYear', nb.deathYear,
        'primaryProfession', nb.primaryProfession,
        'knownForTitles', nb.knownForTitles
    ) AS metadata,
    
    -- Timestamps (use birthYear if available, otherwise current timestamp)
    COALESCE(
        make_timestamp(COALESCE(nb.birthYear, 1900), 1, 1, 0, 0, 0),
        CURRENT_TIMESTAMP
    ) AS created_at,
    
    CURRENT_TIMESTAMP AS updated_at,
    
    -- Reference to original table (for joins and foreign keys)
    nb.nconst AS nconst,
    
    -- Facet columns (extracted for efficient faceting)
    nb.primaryName AS primary_name,
    nb.birthYear AS birth_year,
    nb.deathYear AS death_year,
    -- Store first profession for simple faceting (can be expanded later)
    CASE 
        WHEN nb.primaryProfession IS NOT NULL AND nb.primaryProfession != '' THEN 
            TRIM(SPLIT_PART(nb.primaryProfession, ',', 1))
        ELSE NULL
    END AS primary_profession
    
FROM providers_imdb.name_basics nb
WHERE nb.nconst IS NOT NULL;

-- Add document_id column if it doesn't exist (for compatibility with facets.search_documents_with_facets)
-- This column is required because filter_documents_by_facets returns document_id, not id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'providers_imdb' 
        AND table_name = 'name_basics_faceting_mv' 
        AND column_name = 'document_id'
    ) THEN
        ALTER TABLE providers_imdb.name_basics_faceting_mv 
        ADD COLUMN document_id INTEGER GENERATED ALWAYS AS (id) STORED;
        RAISE NOTICE 'Added document_id column to name_basics_faceting_mv';
    END IF;
END $$;

-- Add faceting to the table
-- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+):
-- Using unlogged => true for initial bulk load provides 2-3x faster performance by bypassing WAL writes.
-- After bulk load, we convert to LOGGED for durability.
SELECT facets.add_faceting_to_table(
    'providers_imdb.name_basics_faceting_mv'::regclass,
    'id',
    ARRAY[
        -- Plain facets (simple column values)
        facets.plain_facet('primary_profession'),
        
        -- Bucket facet for birth year (group into decades/periods)
        facets.bucket_facet('birth_year', ARRAY[
            1850::float8, 1870::float8, 1890::float8, 1910::float8, 1930::float8, 
            1950::float8, 1970::float8, 1990::float8, 2010::float8, 2030::float8
        ]),
        
        -- Bucket facet for death year (group into decades/periods)
        facets.bucket_facet('death_year', ARRAY[
            1850::float8, 1870::float8, 1890::float8, 1910::float8, 1930::float8, 
            1950::float8, 1970::float8, 1990::float8, 2010::float8, 2030::float8
        ])
    ]::facets.facet_definition[],
    20,   -- chunk_bits (2^20 = ~1M documents per chunk)
    TRUE, -- keep_deltas (for incremental updates)
    TRUE, -- populate (initial population)
    FALSE, -- skip_table_creation
    TRUE  -- unlogged (FAST bulk load - 2-3x faster, no WAL writes)
);

-- Convert to LOGGED for durability after initial bulk load
DO $$
BEGIN
    RAISE NOTICE 'Converting facets tables to LOGGED for durability...';
    PERFORM facets.set_table_logged('providers_imdb.name_basics_faceting_mv'::regclass, p_include_deltas => true);
    RAISE NOTICE 'Facets tables are now LOGGED (durable).';
END $$;


-- Create a function to refresh the name_basics faceting table
-- Optimized with batch processing, trigger management, and session settings
-- Supports resume capability and progress tracking
CREATE OR REPLACE FUNCTION providers_imdb.refresh_name_basics_faceting(
    p_resume BOOLEAN DEFAULT FALSE  -- If true, skip insert phase if data already exists
)
RETURNS void AS $$
DECLARE
    batch_size INTEGER := 100000;  -- Process 100K rows per batch
    total_rows BIGINT;
    processed_rows BIGINT := 0;
    batch_offset BIGINT := 0;
    rows_inserted INTEGER;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    elapsed_seconds NUMERIC;
    estimated_total_seconds NUMERIC;
    existing_row_count BIGINT;
    function_start_time TIMESTAMP := clock_timestamp();
    table_name TEXT := 'name_basics_faceting_mv';
    merge_start_time TIMESTAMP;
    merge_elapsed INTERVAL;
BEGIN
    -- Optimize session settings for bulk operations (optimized for 32 CPUs, 128GB RAM)
    SET LOCAL work_mem = '512MB';                    -- Increased for large sorts/joins
    SET LOCAL maintenance_work_mem = '4GB';         -- Increased for index operations
    SET LOCAL max_parallel_workers_per_gather = 16;  -- Increased for parallel processing
    SET LOCAL max_parallel_workers = 32;             -- Match CPU count
    
    -- Initialize progress tracking
    PERFORM providers_imdb.update_faceting_progress(table_name, 'preparing', NULL, NULL, NULL);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting refresh_name_basics_faceting() at %', clock_timestamp();
    RAISE NOTICE 'Resume mode: %', p_resume;
    RAISE NOTICE '========================================';
    
    -- Get total row count for progress tracking
    SELECT COUNT(*) INTO total_rows
    FROM providers_imdb.name_basics
    WHERE nconst IS NOT NULL;
    
    -- Check if we should resume
    SELECT COUNT(*) INTO existing_row_count
    FROM providers_imdb.name_basics_faceting_mv;
    
    RAISE NOTICE 'Total rows to process: %', total_rows;
    RAISE NOTICE 'Existing rows in faceting table: %', existing_row_count;
    
    -- Resume logic: if resume mode and data exists, skip insert phase
    IF p_resume AND existing_row_count > 0 AND existing_row_count >= total_rows * 0.9 THEN
        RAISE NOTICE 'Resume mode: Data already exists (%.0f%% complete), skipping insert phase', 
            (existing_row_count::NUMERIC / total_rows * 100);
        processed_rows := existing_row_count;
        batch_offset := existing_row_count;
        PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
    ELSE
        -- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+): Convert to UNLOGGED for fast bulk load
        RAISE NOTICE 'Converting facets tables to UNLOGGED for fast bulk load (2-3x faster)...';
        PERFORM facets.set_table_unlogged('providers_imdb.name_basics_faceting_mv'::regclass, p_include_deltas => true);
        
        -- Truncate the table (unless resuming)
        IF NOT p_resume OR existing_row_count = 0 THEN
            TRUNCATE TABLE providers_imdb.name_basics_faceting_mv;
            RAISE NOTICE 'Table truncated';
            processed_rows := 0;
            batch_offset := 0;
        ELSE
            -- Resume from where we left off
            RAISE NOTICE 'Resuming from row %', existing_row_count;
            batch_offset := existing_row_count;
            processed_rows := existing_row_count;
        END IF;
        
        -- Disable triggers to avoid per-row trigger overhead during bulk insert
        ALTER TABLE providers_imdb.name_basics_faceting_mv DISABLE TRIGGER ALL;
        RAISE NOTICE 'Triggers disabled for bulk insert';
        
        PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
    
    -- Process in batches using OFFSET
    -- Note: OFFSET becomes slower for later batches, but this is acceptable for a full refresh
    WHILE batch_offset < total_rows LOOP
            batch_start_time := clock_timestamp();
            
            INSERT INTO providers_imdb.name_basics_faceting_mv
            SELECT 
                -- Use stable hash-based ID from nconst (pgfaceting requires integer IDs)
                ABS(HASHTEXT(nb.nconst))::INTEGER AS id,
                
                -- document_id column alias (required by facets.search_documents_with_facets)
                -- This is an alias for id to match the expected column name
                ABS(HASHTEXT(nb.nconst))::INTEGER AS document_id,
                
                -- Content for full-text search (primaryName and primaryProfession)
                COALESCE(
                    nb.primaryName || ' ' || 
                    COALESCE(nb.primaryProfession, ''),
                    ''
                ) AS content,
                
                -- Metadata as JSONB (store all name data here)
                jsonb_build_object(
                    'nconst', nb.nconst,
                    'primaryName', nb.primaryName,
                    'birthYear', nb.birthYear,
                    'deathYear', nb.deathYear,
                    'primaryProfession', nb.primaryProfession,
                    'knownForTitles', nb.knownForTitles
                ) AS metadata,
                
                -- Timestamps (use birthYear if available, otherwise current timestamp)
                COALESCE(
                    make_timestamp(COALESCE(nb.birthYear, 1900), 1, 1, 0, 0, 0),
                    CURRENT_TIMESTAMP
                ) AS created_at,
                
                CURRENT_TIMESTAMP AS updated_at,
                
                -- Reference to original table (for joins and foreign keys)
                nb.nconst AS nconst,
                
                -- Facet columns (extracted for efficient faceting)
                nb.primaryName AS primary_name,
                nb.birthYear AS birth_year,
                nb.deathYear AS death_year,
                -- Store first profession for simple faceting (can be expanded later)
                CASE 
                    WHEN nb.primaryProfession IS NOT NULL AND nb.primaryProfession != '' THEN 
                        TRIM(SPLIT_PART(nb.primaryProfession, ',', 1))
                    ELSE NULL
                END AS primary_profession
                
            FROM providers_imdb.name_basics nb
            WHERE nb.nconst IS NOT NULL
            ORDER BY nb.nconst  -- Consistent ordering for stable batching
            LIMIT batch_size
            OFFSET batch_offset;
            
            GET DIAGNOSTICS rows_inserted = ROW_COUNT;
            
            -- Break if no rows were inserted
            IF rows_inserted = 0 THEN
                EXIT;
            END IF;
            
            batch_offset := batch_offset + rows_inserted;
            processed_rows := batch_offset;
            
            batch_end_time := clock_timestamp();
            elapsed_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
            
            -- Update progress tracking
            PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
            
            -- Estimate remaining time
            IF processed_rows > 0 AND elapsed_seconds > 0 AND processed_rows < total_rows THEN
                estimated_total_seconds := (elapsed_seconds / rows_inserted) * (total_rows - processed_rows);
                RAISE NOTICE '[INSERT PHASE] Processed % / % rows (%.1f%%) - Batch took %.1f seconds - Estimated remaining: %.1f seconds (%.1f minutes)',
                    processed_rows, total_rows, 
                    (processed_rows::NUMERIC / total_rows * 100),
                    elapsed_seconds,
                    estimated_total_seconds,
                    estimated_total_seconds / 60;
            ELSE
                RAISE NOTICE '[INSERT PHASE] Processed % / % rows (%.1f%%) - Batch took %.1f seconds',
                    processed_rows, total_rows,
                    (processed_rows::NUMERIC / total_rows * 100),
                    elapsed_seconds;
            END IF;
    END LOOP;
    
    -- Final insert phase update
    PERFORM providers_imdb.update_faceting_progress(table_name, 'inserting', total_rows, processed_rows, batch_offset);
    RAISE NOTICE '[INSERT PHASE] Completed: % / % rows (100%%)', processed_rows, total_rows;
    
    -- Re-enable triggers
    ALTER TABLE providers_imdb.name_basics_faceting_mv ENABLE TRIGGER ALL;
    RAISE NOTICE 'Triggers re-enabled';
    END IF;
    
    -- Merge deltas once at the end (much faster than per-row trigger updates)
    RAISE NOTICE '========================================';
    RAISE NOTICE '[MERGE PHASE] Starting merge_deltas() at %', clock_timestamp();
    RAISE NOTICE '[MERGE PHASE] This phase can take HOURS for large datasets';
    RAISE NOTICE '[MERGE PHASE] Progress cannot be tracked internally, but checkpoints indicate activity';
    RAISE NOTICE '========================================';
    
    PERFORM providers_imdb.update_faceting_progress(table_name, 'merging', total_rows, processed_rows, batch_offset);
    
    -- Record merge start time for progress estimation
    merge_start_time := clock_timestamp();
    PERFORM facets.merge_deltas('providers_imdb.name_basics_faceting_mv'::regclass);
    
    merge_elapsed := clock_timestamp() - merge_start_time;
    RAISE NOTICE '[MERGE PHASE] Completed in %', merge_elapsed;
    
    -- PERFORMANCE OPTIMIZATION (pg_facets 0.4.3+): Convert back to LOGGED for durability
    RAISE NOTICE 'Converting facets tables back to LOGGED for durability...';
    PERFORM facets.set_table_logged('providers_imdb.name_basics_faceting_mv'::regclass, p_include_deltas => true);
    RAISE NOTICE 'Facets tables are now LOGGED (durable).';
    
    -- Mark as completed
    PERFORM providers_imdb.update_faceting_progress(table_name, 'completed', total_rows, processed_rows, batch_offset);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'refresh_name_basics_faceting() completed at %', clock_timestamp();
    RAISE NOTICE 'Total elapsed time: %', clock_timestamp() - function_start_time;
    RAISE NOTICE '========================================';
END;
$$ LANGUAGE plpgsql;



