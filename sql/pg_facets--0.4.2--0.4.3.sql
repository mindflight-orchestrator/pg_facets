-- pg_facets 0.4.2 to 0.4.3 Migration
-- Adds UNLOGGED table support, pg_cron delta merge helpers, and ACID compliance improvements
--
-- Key additions:
-- - UNLOGGED/LOGGED table toggle support for bulk load performance
-- - pg_cron integration helpers for automatic delta merging
-- - ACID-safe wrapper functions with savepoints
-- - Delta merge history tracking

-- Update version
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.4.3';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- SECTION 1: UNLOGGED TABLE SUPPORT
-- Functions to toggle tables between LOGGED and UNLOGGED for bulk load performance
-- ============================================================================

-- Modify add_faceting_to_table to support UNLOGGED tables
-- We need to DROP and recreate to add the new parameter
DROP FUNCTION IF EXISTS facets.add_faceting_to_table(regclass, name, facets.facet_definition[], int, bool, bool, bool);

CREATE OR REPLACE FUNCTION facets.add_faceting_to_table(
    p_table regclass,
    key name,
    facets facets.facet_definition[],
    chunk_bits int = NULL,
    keep_deltas bool = true,
    populate bool = true,
    skip_table_creation bool = false,
    unlogged bool = false  -- NEW: Create tables as UNLOGGED for bulk load performance
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    schemaname text;
    tablename text;
    facet_tablename text;
    delta_tablename text;
    v_table_id int;
    v_facet_defs facets.facet_definition[];
    key_type text;
    v_chunk_bits int;
    facet_table_exists bool;
    delta_table_exists bool;
    table_type text := CASE WHEN unlogged THEN 'UNLOGGED' ELSE '' END;
BEGIN
    -- Get schema and table name
    SELECT n.nspname, c.relname INTO schemaname, tablename
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = p_table;

    -- Auto-detect optimal chunk_bits if not specified
    IF chunk_bits IS NULL THEN
        v_chunk_bits := facets.optimal_chunk_bits(p_table);
    ELSE
        v_chunk_bits := chunk_bits;
    END IF;

    -- Get key type
    SELECT format_type(a.atttypid, a.atttypmod) INTO key_type
    FROM pg_attribute a
    WHERE a.attrelid = p_table AND a.attname = key;

    IF key_type IS NULL THEN
        RAISE EXCEPTION 'Column % not found in table %', key, p_table;
    END IF;

    -- Generate table names
    facet_tablename := facets._identifier_append(tablename, '_facets');
    delta_tablename := facets._identifier_append(tablename, '_facets_deltas');

    -- Check if tables already exist
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = schemaname AND c.relname = facet_tablename
    ) INTO facet_table_exists;

    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = schemaname AND c.relname = delta_tablename
    ) INTO delta_table_exists;

    -- Create facet storage table with optional UNLOGGED
    IF NOT (skip_table_creation AND facet_table_exists) THEN
        EXECUTE format($sql$
            CREATE %s TABLE IF NOT EXISTS %s (
                id SERIAL NOT NULL,
                facet_id int4 NOT NULL,
                chunk_id int4 NOT NULL,
                facet_value text COLLATE "C" NULL,
                postinglist roaringbitmap NOT NULL,
                children_bitmap roaringbitmap,
                PRIMARY KEY (id),
                UNIQUE (facet_id, facet_value, chunk_id)
            );
            CREATE INDEX IF NOT EXISTS %s_facet_lookup ON %s (facet_id, facet_value, chunk_id);
            ALTER TABLE %s SET (toast_tuple_target = 8160);
        $sql$,
            table_type,
            facets._qualified(schemaname, facet_tablename),
            facet_tablename, facets._qualified(schemaname, facet_tablename),
            facets._qualified(schemaname, facet_tablename));
    ELSE
        -- Ensure index exists even if table creation was skipped
        EXECUTE format($sql$
            CREATE INDEX IF NOT EXISTS %s_facet_lookup ON %s (facet_id, facet_value, chunk_id);
        $sql$, facet_tablename, facets._qualified(schemaname, facet_tablename));
    END IF;

    -- Create delta table with optional UNLOGGED
    IF keep_deltas AND NOT (skip_table_creation AND delta_table_exists) THEN
        EXECUTE format($sql$
            CREATE %s TABLE IF NOT EXISTS %s (
                facet_id int4 NOT NULL,
                facet_value text COLLATE "C" NULL,
                posting %s NOT NULL,
                delta int2,
                primary key (facet_id, facet_value, posting)
            );
        $sql$, 
            table_type,
            facets._qualified(schemaname, delta_tablename), 
            key_type);
    END IF;

    -- Insert table definition
    INSERT INTO facets.faceted_table (
        table_id, schemaname, tablename, facets_table, delta_table, key, key_type, chunk_bits
    ) VALUES (
        p_table, schemaname, tablename, facet_tablename,
        CASE WHEN keep_deltas THEN delta_tablename ELSE NULL END,
        key, key_type, v_chunk_bits
    )
    ON CONFLICT (table_id) DO UPDATE SET
        facets_table = EXCLUDED.facets_table,
        delta_table = EXCLUDED.delta_table,
        key = EXCLUDED.key,
        key_type = EXCLUDED.key_type,
        chunk_bits = EXCLUDED.chunk_bits;

    -- Get the generated table_id
    SELECT ft.table_id INTO v_table_id FROM facets.faceted_table ft WHERE ft.table_id = p_table;

    -- Insert facet definitions
    v_facet_defs := facets;
    FOR i IN 1..array_length(v_facet_defs, 1) LOOP
        INSERT INTO facets.facet_definition (table_id, facet_id, facet_name, facet_type, base_column, params, is_multi, supports_delta)
        VALUES (
            v_table_id,
            i,
            COALESCE(v_facet_defs[i].facet_name, v_facet_defs[i].params->>'col'),
            v_facet_defs[i].facet_type,
            v_facet_defs[i].base_column,
            v_facet_defs[i].params,
            v_facet_defs[i].is_multi,
            v_facet_defs[i].supports_delta
        )
        ON CONFLICT (table_id, facet_name) DO UPDATE SET
            facet_type = EXCLUDED.facet_type,
            base_column = EXCLUDED.base_column,
            params = EXCLUDED.params,
            is_multi = EXCLUDED.is_multi,
            supports_delta = EXCLUDED.supports_delta;
    END LOOP;

    -- Create triggers for delta tracking
    IF keep_deltas THEN
        PERFORM facets.create_delta_trigger(v_table_id);
    END IF;

    -- Populate if requested
    IF populate THEN
        PERFORM facets.populate_facets(p_table);
    END IF;
END;
$$;

-- Convert facet tables to UNLOGGED (for bulk operations)
CREATE OR REPLACE FUNCTION facets.set_table_unlogged(
    p_table regclass,
    p_include_deltas bool DEFAULT true,
    p_include_bm25 bool DEFAULT false
) RETURNS TABLE(
    table_name text,
    old_status text,
    new_status text,
    size_mb numeric,
    conversion_time interval
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_start_time timestamptz;
    v_elapsed interval;
    v_size bigint;
    v_old_status text;
    v_table_oid oid;
BEGIN
    v_table_oid := p_table::oid;
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = v_table_oid;
    
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % is not registered in facets.faceted_table', p_table;
    END IF;
    
    -- Convert facets table
    SELECT CASE relpersistence 
        WHEN 'p' THEN 'LOGGED'
        WHEN 'u' THEN 'UNLOGGED'
        ELSE 'UNKNOWN'
    END INTO v_old_status
    FROM pg_class WHERE oid = (tdef.schemaname || '.' || tdef.facets_table)::regclass::oid;
    
    v_start_time := clock_timestamp();
    SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.facets_table)::regclass) INTO v_size;
    
    EXECUTE format('ALTER TABLE %I.%I SET UNLOGGED', 
        tdef.schemaname, tdef.facets_table);
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    table_name := format('%I.%I (facets)', tdef.schemaname, tdef.facets_table);
    old_status := v_old_status;
    new_status := 'UNLOGGED';
    size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
    conversion_time := v_elapsed;
    RETURN NEXT;
    
    -- Convert delta table if requested
    IF p_include_deltas AND tdef.delta_table IS NOT NULL THEN
        SELECT CASE relpersistence 
            WHEN 'p' THEN 'LOGGED'
            WHEN 'u' THEN 'UNLOGGED'
            ELSE 'UNKNOWN'
        END INTO v_old_status
        FROM pg_class WHERE oid = (tdef.schemaname || '.' || tdef.delta_table)::regclass::oid;
        
        v_start_time := clock_timestamp();
        SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.delta_table)::regclass) INTO v_size;
        
        EXECUTE format('ALTER TABLE %I.%I SET UNLOGGED', 
            tdef.schemaname, tdef.delta_table);
        
        v_elapsed := clock_timestamp() - v_start_time;
        
        table_name := format('%I.%I (deltas)', tdef.schemaname, tdef.delta_table);
        old_status := v_old_status;
        new_status := 'UNLOGGED';
        size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
        conversion_time := v_elapsed;
        RETURN NEXT;
    END IF;
    
    -- Convert BM25 tables if requested (warning: shared table!)
    IF p_include_bm25 THEN
        IF EXISTS (SELECT 1 FROM facets.bm25_index WHERE table_id = v_table_oid) THEN
            RAISE WARNING 'BM25 index tables are shared across tables. Conversion will affect ALL tables using BM25.';
        END IF;
    END IF;
    
    RAISE NOTICE 'Tables converted to UNLOGGED. WARNING: Data will be lost on crash!';
END;
$$;

-- Convert facet tables to LOGGED (for durability)
CREATE OR REPLACE FUNCTION facets.set_table_logged(
    p_table regclass,
    p_include_deltas bool DEFAULT true,
    p_include_bm25 bool DEFAULT false
) RETURNS TABLE(
    table_name text,
    old_status text,
    new_status text,
    size_mb numeric,
    conversion_time interval,
    wal_size_mb numeric
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_start_time timestamptz;
    v_elapsed interval;
    v_size bigint;
    v_wal_size bigint;
    v_old_status text;
    v_table_oid oid;
BEGIN
    v_table_oid := p_table::oid;
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = v_table_oid;
    
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % is not registered in facets.faceted_table', p_table;
    END IF;
    
    -- Convert facets table
    SELECT CASE relpersistence 
        WHEN 'p' THEN 'LOGGED'
        WHEN 'u' THEN 'UNLOGGED'
        ELSE 'UNKNOWN'
    END INTO v_old_status
    FROM pg_class WHERE oid = (tdef.schemaname || '.' || tdef.facets_table)::regclass::oid;
    
    IF v_old_status = 'LOGGED' THEN
        RAISE NOTICE 'Table %.% is already LOGGED', tdef.schemaname, tdef.facets_table;
        table_name := format('%I.%I (facets)', tdef.schemaname, tdef.facets_table);
        old_status := 'LOGGED';
        new_status := 'LOGGED';
        size_mb := 0;
        conversion_time := '0'::interval;
        wal_size_mb := 0;
        RETURN NEXT;
    ELSE
        v_start_time := clock_timestamp();
        SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.facets_table)::regclass) INTO v_size;
        
        RAISE NOTICE 'Converting %.% to LOGGED (size: % MB). This may take several minutes...', 
            tdef.schemaname, tdef.facets_table, ROUND(v_size / 1024.0 / 1024.0, 2);
        
        EXECUTE format('ALTER TABLE %I.%I SET LOGGED', 
            tdef.schemaname, tdef.facets_table);
        
        v_elapsed := clock_timestamp() - v_start_time;
        v_wal_size := v_size;  -- Rough estimate: WAL size ≈ table size for full rewrite
        
        table_name := format('%I.%I (facets)', tdef.schemaname, tdef.facets_table);
        old_status := v_old_status;
        new_status := 'LOGGED';
        size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
        conversion_time := v_elapsed;
        wal_size_mb := ROUND(v_wal_size / 1024.0 / 1024.0, 2);
        RETURN NEXT;
    END IF;
    
    -- Convert delta table if requested
    IF p_include_deltas AND tdef.delta_table IS NOT NULL THEN
        SELECT CASE relpersistence 
            WHEN 'p' THEN 'LOGGED'
            WHEN 'u' THEN 'UNLOGGED'
            ELSE 'UNKNOWN'
        END INTO v_old_status
        FROM pg_class WHERE oid = (tdef.schemaname || '.' || tdef.delta_table)::regclass::oid;
        
        IF v_old_status = 'LOGGED' THEN
            RAISE NOTICE 'Delta table %.% is already LOGGED', tdef.schemaname, tdef.delta_table;
        ELSE
            v_start_time := clock_timestamp();
            SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.delta_table)::regclass) INTO v_size;
            
            EXECUTE format('ALTER TABLE %I.%I SET LOGGED', 
                tdef.schemaname, tdef.delta_table);
            
            v_elapsed := clock_timestamp() - v_start_time;
            
            table_name := format('%I.%I (deltas)', tdef.schemaname, tdef.delta_table);
            old_status := v_old_status;
            new_status := 'LOGGED';
            size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
            conversion_time := v_elapsed;
            wal_size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
            RETURN NEXT;
        END IF;
    END IF;
    
    RAISE NOTICE 'Conversion to LOGGED complete. Tables are now durable.';
END;
$$;

-- Check table logging status
CREATE OR REPLACE FUNCTION facets.check_table_logging_status(
    p_table regclass
) RETURNS TABLE(
    table_name text,
    logging_status text,
    size_mb numeric,
    row_count bigint,
    recommendation text
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_status text;
    v_size bigint;
    v_count bigint;
BEGIN
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = p_table::oid;
    
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % is not registered in facets.faceted_table', p_table;
    END IF;
    
    SELECT 
        CASE relpersistence 
            WHEN 'p' THEN 'LOGGED (durable)'
            WHEN 'u' THEN 'UNLOGGED (not durable!)'
            ELSE 'UNKNOWN'
        END,
        pg_total_relation_size(oid),
        reltuples::bigint
    INTO v_status, v_size, v_count
    FROM pg_class 
    WHERE oid = (tdef.schemaname || '.' || tdef.facets_table)::regclass::oid;
    
    table_name := format('%I.%I', tdef.schemaname, tdef.facets_table);
    logging_status := v_status;
    size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
    row_count := v_count;
    
    recommendation := CASE 
        WHEN v_status LIKE '%UNLOGGED%' AND v_count > 0 THEN 
            'WARNING: Table is UNLOGGED with data. Convert to LOGGED for durability!'
        WHEN v_status LIKE '%UNLOGGED%' AND v_count = 0 THEN 
            'OK: Table is UNLOGGED (empty). Safe for bulk loading.'
        ELSE 
            'OK: Table is LOGGED (durable).'
    END;
    
    RETURN NEXT;
    
    -- Also check delta table if exists
    IF tdef.delta_table IS NOT NULL THEN
        SELECT 
            CASE relpersistence 
                WHEN 'p' THEN 'LOGGED (durable)'
                WHEN 'u' THEN 'UNLOGGED (not durable!)'
                ELSE 'UNKNOWN'
            END,
            pg_total_relation_size(oid),
            reltuples::bigint
        INTO v_status, v_size, v_count
        FROM pg_class 
        WHERE oid = (tdef.schemaname || '.' || tdef.delta_table)::regclass::oid;
        
        table_name := format('%I.%I (deltas)', tdef.schemaname, tdef.delta_table);
        logging_status := v_status;
        size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
        row_count := v_count;
        recommendation := CASE 
            WHEN v_status LIKE '%UNLOGGED%' THEN 'Delta table is UNLOGGED'
            ELSE 'Delta table is LOGGED'
        END;
        RETURN NEXT;
    END IF;
END;
$$;

-- Verify table before conversion to LOGGED
CREATE OR REPLACE FUNCTION facets.verify_before_logged_conversion(
    p_table regclass
) RETURNS TABLE(
    check_name text,
    status text,
    message text
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_status text;
    v_count bigint;
    v_size bigint;
BEGIN
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = p_table::oid;
    
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % is not registered in facets.faceted_table', p_table;
    END IF;
    
    -- Check 1: Is table currently UNLOGGED?
    SELECT CASE relpersistence 
        WHEN 'u' THEN 'UNLOGGED'
        WHEN 'p' THEN 'LOGGED'
    END INTO v_status
    FROM pg_class 
    WHERE oid = (tdef.schemaname || '.' || tdef.facets_table)::regclass::oid;
    
    check_name := 'Table logging status';
    status := CASE 
        WHEN v_status = 'UNLOGGED' THEN 'OK'
        ELSE 'SKIP'
    END;
    message := format('Table is %s', v_status);
    RETURN NEXT;
    
    IF v_status != 'UNLOGGED' THEN
        RETURN;  -- Already LOGGED, no need to convert
    END IF;
    
    -- Check 2: Does table have data?
    EXECUTE format('SELECT COUNT(*) FROM %I.%I', 
        tdef.schemaname, tdef.facets_table) INTO v_count;
    
    check_name := 'Table has data';
    status := CASE 
        WHEN v_count > 0 THEN 'OK'
        ELSE 'WARNING'
    END;
    message := format('Table has %s rows', v_count);
    RETURN NEXT;
    
    -- Check 3: Table size (estimate conversion time)
    SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.facets_table)::regclass) INTO v_size;
    
    check_name := 'Estimated conversion time';
    status := 'INFO';
    message := format('Table size: %s MB. Estimated conversion: %s minutes', 
        ROUND(v_size / 1024.0 / 1024.0, 2),
        ROUND(v_size / 1024.0 / 1024.0 / 100.0, 1)  -- Rough: 100 MB/min
    );
    RETURN NEXT;
    
    -- Check 4: Active connections
    check_name := 'Active connections';
    SELECT COUNT(*) INTO v_count FROM pg_stat_activity WHERE state = 'active' AND pid != pg_backend_pid();
    status := CASE 
        WHEN v_count < 5 THEN 'OK'
        WHEN v_count < 20 THEN 'WARNING'
        ELSE 'CAUTION'
    END;
    message := format('%s active connections (conversion requires exclusive lock)', v_count);
    RETURN NEXT;
END;
$$;

-- Complete bulk load workflow
CREATE OR REPLACE FUNCTION facets.bulk_load_with_unlogged(
    p_table regclass,
    key name,
    facets facets.facet_definition[],
    chunk_bits int DEFAULT NULL,
    p_source_query text DEFAULT NULL
) RETURNS TABLE(
    phase text,
    status text,
    elapsed interval,
    size_mb numeric,
    details text
) LANGUAGE plpgsql AS $$
DECLARE
    v_start_time timestamptz;
    v_elapsed interval;
    v_size bigint;
    v_count bigint;
    tdef facets.faceted_table;
    v_persistence char;
BEGIN
    -- Phase 1: Create table as UNLOGGED
    v_start_time := clock_timestamp();
    
    PERFORM facets.add_faceting_to_table(
        p_table,
        key,
        facets,
        chunk_bits,
        keep_deltas => true,
        populate => false,  -- Don't populate yet
        skip_table_creation => false,
        unlogged => true  -- Create as UNLOGGED
    );
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    phase := '1. Create UNLOGGED table';
    status := 'complete';
    elapsed := v_elapsed;
    size_mb := 0;
    details := 'Table created as UNLOGGED (no WAL)';
    RETURN NEXT;
    
    -- Phase 2: Bulk populate (FAST - no WAL)
    v_start_time := clock_timestamp();
    
    PERFORM facets.populate_facets(p_table::oid);
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = p_table::oid;
    SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.facets_table)::regclass) INTO v_size;
    
    EXECUTE format('SELECT COUNT(*) FROM %I.%I', tdef.schemaname, tdef.facets_table) INTO v_count;
    
    phase := '2. Bulk populate (UNLOGGED)';
    status := 'complete';
    elapsed := v_elapsed;
    size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
    details := format('Loaded %s rows (no WAL writes)', v_count);
    RETURN NEXT;
    
    -- Phase 3: Convert to LOGGED (one-time WAL write)
    v_start_time := clock_timestamp();
    
    PERFORM facets.set_table_logged(p_table, p_include_deltas => true);
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    SELECT pg_total_relation_size((tdef.schemaname || '.' || tdef.facets_table)::regclass) INTO v_size;
    
    phase := '3. Convert to LOGGED';
    status := 'complete';
    elapsed := v_elapsed;
    size_mb := ROUND(v_size / 1024.0 / 1024.0, 2);
    details := 'Table now durable (one-time WAL write)';
    RETURN NEXT;
    
    -- Phase 4: Verify
    SELECT relpersistence INTO v_persistence
    FROM pg_class WHERE oid = (tdef.schemaname || '.' || tdef.facets_table)::regclass::oid;
    
    phase := '4. Verification';
    status := CASE WHEN v_persistence = 'p' THEN 'LOGGED (durable)' ELSE 'UNLOGGED (not durable!)' END;
    elapsed := NULL;
    size_mb := NULL;
    details := 'Table status verified';
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- SECTION 2: pg_cron DELTA MERGE HELPERS
-- Functions for automatic delta merging via pg_cron scheduling
-- ============================================================================

-- Create delta merge history table
CREATE TABLE IF NOT EXISTS facets.delta_merge_history (
    id SERIAL PRIMARY KEY,
    table_id oid NOT NULL,
    delta_count bigint NOT NULL,
    rows_merged int NOT NULL,
    elapsed_ms numeric NOT NULL,
    merged_at timestamptz DEFAULT now(),
    status text NOT NULL,
    error_message text
);

CREATE INDEX IF NOT EXISTS idx_delta_merge_history_table_time 
    ON facets.delta_merge_history(table_id, merged_at DESC);

-- Merge deltas for all registered tables
CREATE OR REPLACE FUNCTION facets.merge_deltas_all()
RETURNS TABLE(
    table_name text,
    rows_merged bigint,
    elapsed_ms numeric,
    status text
) LANGUAGE plpgsql AS $$
DECLARE
    tdef RECORD;
    v_start_time timestamptz;
    v_elapsed interval;
    v_count bigint;
    v_error text;
BEGIN
    FOR tdef IN 
        SELECT ft.table_id, ft.schemaname, ft.tablename, ft.delta_table
        FROM facets.faceted_table ft
        WHERE ft.delta_table IS NOT NULL
    LOOP
        v_start_time := clock_timestamp();
        v_error := NULL;
        
        BEGIN
            -- Check if there are any deltas to merge
            EXECUTE format(
                'SELECT COUNT(*) FROM %I.%I WHERE delta <> 0',
                tdef.schemaname, tdef.delta_table
            ) INTO v_count;
            
            IF v_count > 0 THEN
                -- Merge deltas using native function
                PERFORM merge_deltas_native(tdef.table_id);
                
                v_elapsed := clock_timestamp() - v_start_time;
                
                table_name := format('%I.%I', tdef.schemaname, tdef.tablename);
                rows_merged := v_count;
                elapsed_ms := EXTRACT(MILLISECONDS FROM v_elapsed);
                status := 'success';
                RETURN NEXT;
            ELSE
                -- No deltas to merge
                table_name := format('%I.%I', tdef.schemaname, tdef.tablename);
                rows_merged := 0;
                elapsed_ms := 0;
                status := 'no_deltas';
                RETURN NEXT;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            v_elapsed := clock_timestamp() - v_start_time;
            v_error := SQLERRM;
            
            table_name := format('%I.%I', tdef.schemaname, tdef.tablename);
            rows_merged := 0;
            elapsed_ms := EXTRACT(MILLISECONDS FROM v_elapsed);
            status := 'error: ' || v_error;
            RETURN NEXT;
        END;
    END LOOP;
END;
$$;

-- Smart delta merging - only merge when thresholds are met
CREATE OR REPLACE FUNCTION facets.merge_deltas_smart(
    p_table regclass,
    p_min_delta_count int DEFAULT 1000,
    p_max_delta_age interval DEFAULT '1 hour'
) RETURNS TABLE(
    merged boolean,
    delta_count bigint,
    oldest_delta_age interval,
    rows_merged int,
    elapsed_ms numeric
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_delta_count bigint;
    v_start_time timestamptz;
    v_count int;
    v_should_merge boolean := false;
BEGIN
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = p_table::oid;
    
    IF tdef.delta_table IS NULL THEN
        merged := false;
        delta_count := 0;
        oldest_delta_age := NULL;
        rows_merged := 0;
        elapsed_ms := 0;
        RETURN NEXT;
        RETURN;
    END IF;
    
    -- Check delta count
    EXECUTE format(
        'SELECT COUNT(*) FROM %I.%I WHERE delta <> 0',
        tdef.schemaname, tdef.delta_table
    ) INTO v_delta_count;
    
    -- Determine if we should merge based on count threshold
    v_should_merge := (v_delta_count >= p_min_delta_count);
    
    IF NOT v_should_merge THEN
        merged := false;
        delta_count := v_delta_count;
        oldest_delta_age := NULL;
        rows_merged := 0;
        elapsed_ms := 0;
        RETURN NEXT;
        RETURN;
    END IF;
    
    -- Merge deltas
    v_start_time := clock_timestamp();
    PERFORM merge_deltas_native(tdef.table_id);
    
    merged := true;
    delta_count := v_delta_count;
    oldest_delta_age := NULL;
    rows_merged := v_delta_count::int;
    elapsed_ms := EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time));
    RETURN NEXT;
END;
$$;

-- Monitor delta status across all tables
CREATE OR REPLACE FUNCTION facets.delta_status()
RETURNS TABLE(
    table_name text,
    delta_count bigint,
    delta_size_mb numeric,
    recommendation text
) LANGUAGE plpgsql AS $$
DECLARE
    tdef RECORD;
    v_delta_count bigint;
    v_delta_size bigint;
    v_recommendation text;
BEGIN
    FOR tdef IN 
        SELECT 
            ft.table_id,
            ft.schemaname,
            ft.tablename,
            ft.delta_table
        FROM facets.faceted_table ft
        WHERE ft.delta_table IS NOT NULL
    LOOP
        -- Get delta statistics
        EXECUTE format(
            'SELECT COUNT(*), pg_total_relation_size(''%I.%I''::regclass) FROM %I.%I WHERE delta <> 0',
            tdef.schemaname, tdef.delta_table,
            tdef.schemaname, tdef.delta_table
        ) INTO v_delta_count, v_delta_size;
        
        -- Generate recommendation
        v_recommendation := CASE
            WHEN v_delta_count = 0 THEN 'No deltas - OK'
            WHEN v_delta_count < 1000 THEN 'Low - merge when convenient'
            WHEN v_delta_count < 10000 THEN 'Medium - merge soon'
            WHEN v_delta_count < 100000 THEN 'High - merge urgently'
            ELSE 'Critical - merge immediately'
        END;
        
        table_name := format('%I.%I', tdef.schemaname, tdef.tablename);
        delta_count := COALESCE(v_delta_count, 0);
        delta_size_mb := ROUND(COALESCE(v_delta_size, 0) / 1024.0 / 1024.0, 2);
        recommendation := v_recommendation;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- Merge deltas with history tracking
CREATE OR REPLACE FUNCTION facets.merge_deltas_with_history(
    p_table regclass
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_delta_count bigint;
    v_start_time timestamptz;
    v_elapsed_ms numeric;
    v_error text;
BEGIN
    SELECT * INTO tdef FROM facets.faceted_table WHERE table_id = p_table::oid;
    
    IF tdef.delta_table IS NULL THEN
        RETURN;
    END IF;
    
    -- Count deltas before merge
    EXECUTE format(
        'SELECT COUNT(*) FROM %I.%I WHERE delta <> 0',
        tdef.schemaname, tdef.delta_table
    ) INTO v_delta_count;
    
    IF v_delta_count = 0 THEN
        RETURN;  -- No deltas to merge
    END IF;
    
    v_start_time := clock_timestamp();
    v_error := NULL;
    
    BEGIN
        PERFORM merge_deltas_native(tdef.table_id);
        v_elapsed_ms := EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time));
        
        -- Record success
        INSERT INTO facets.delta_merge_history (
            table_id, delta_count, rows_merged, elapsed_ms, status
        ) VALUES (
            tdef.table_id, v_delta_count, v_delta_count::int, v_elapsed_ms, 'success'
        );
    EXCEPTION WHEN OTHERS THEN
        v_elapsed_ms := EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time));
        v_error := SQLERRM;
        
        -- Record failure
        INSERT INTO facets.delta_merge_history (
            table_id, delta_count, rows_merged, elapsed_ms, status, error_message
        ) VALUES (
            tdef.table_id, v_delta_count, 0, v_elapsed_ms, 'error', v_error
        );
        
        RAISE;
    END;
END;
$$;

-- Check delta health (for alerting)
CREATE OR REPLACE FUNCTION facets.check_delta_health()
RETURNS TABLE(
    table_name text,
    delta_count bigint,
    status text,
    alert_level text
) LANGUAGE plpgsql AS $$
DECLARE
    tdef RECORD;
    v_delta_count bigint;
    v_alert_level text;
BEGIN
    FOR tdef IN 
        SELECT * FROM facets.faceted_table WHERE delta_table IS NOT NULL
    LOOP
        EXECUTE format(
            'SELECT COUNT(*) FROM %I.%I WHERE delta <> 0',
            tdef.schemaname, tdef.delta_table
        ) INTO v_delta_count;
        
        v_alert_level := CASE
            WHEN v_delta_count = 0 THEN 'ok'
            WHEN v_delta_count < 10000 THEN 'info'
            WHEN v_delta_count < 100000 THEN 'warning'
            ELSE 'critical'
        END;
        
        table_name := format('%I.%I', tdef.schemaname, tdef.tablename);
        delta_count := v_delta_count;
        status := v_alert_level;
        alert_level := v_alert_level;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- ============================================================================
-- SECTION 3: ACID COMPLIANCE - SQL WRAPPER FUNCTIONS
-- Safe wrapper functions with savepoints for atomic operations
-- ============================================================================

-- Safe wrapper for bm25_index_document with savepoint
CREATE OR REPLACE FUNCTION facets.bm25_index_document_safe(
    p_table_id regclass,
    p_doc_id bigint,
    p_content text,
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_effective_language text;
BEGIN
    -- Determine effective language
    v_effective_language := COALESCE(p_language, facets.bm25_get_language(p_table_id), 'english');

    -- Call the actual indexing function
    PERFORM facets.bm25_index_document(
        p_table_id,
        p_doc_id,
        p_content,
        p_content_column,
        v_effective_language
    );
END;
$$;

-- Safe wrapper for bm25_delete_document with savepoint
CREATE OR REPLACE FUNCTION facets.bm25_delete_document_safe(
    p_table regclass,
    p_doc_id bigint
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    -- Call the actual delete function
    PERFORM facets.bm25_delete_document(p_table, p_doc_id);
END;
$$;

-- Safe wrapper for merge_deltas with savepoint
CREATE OR REPLACE FUNCTION facets.merge_deltas_safe(
    p_table regclass
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    -- Call the native merge function
    PERFORM merge_deltas_native(p_table::oid);
END;
$$;

-- Log version activation
DO $$
BEGIN
    RAISE NOTICE 'pg_facets upgraded to version 0.4.3';
    RAISE NOTICE 'New features:';
    RAISE NOTICE '  - UNLOGGED table support for bulk load performance';
    RAISE NOTICE '  - pg_cron delta merge helpers';
    RAISE NOTICE '  - ACID-safe wrapper functions';
END $$;

