-- pg_facets 0.4.2
-- PostgreSQL extension for efficient faceted search using Roaring Bitmaps
-- High-performance faceting with Zig native functions
-- BM25 Full-Text Search Integration
-- 
-- Features:
-- - BM25 (Best Matching 25) full-text search with proper ranking
-- - Language-aware tokenization using PostgreSQL text search configs
-- - Prefix and fuzzy prefix matching support
-- - Integration with existing faceting functionality

-- SECTION 1: CORE DEFINITIONS AND UTILITY FUNCTIONS

-- Extension initialization
CREATE SCHEMA IF NOT EXISTS facets;

-- Version information
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.4.2';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Core utility functions
CREATE FUNCTION facets._identifier_append(ident text, append text) RETURNS text
    LANGUAGE SQL
AS $$
    SELECT CASE WHEN right(ident, 1) = '"' THEN
        substr(ident, 1, length(ident) - 1) || append || '"'
    ELSE ident || append END;
$$;

CREATE FUNCTION facets._name_only(ident text) RETURNS text
    LANGUAGE SQL
AS $$
    SELECT regexp_replace(ident, '^([^"]*|"([^\"]|\\")*")\.', '');
$$;

CREATE FUNCTION facets._qualified(schemaname text, tablename text) RETURNS text
    LANGUAGE SQL
AS $$
    SELECT format('%s.%s', quote_ident(schemaname), quote_ident(tablename));
$$;

CREATE FUNCTION facets._trigger_names(tablename text, OUT tfunc_name text, OUT trg_name text)
    LANGUAGE plpgsql
    AS $$
BEGIN
    tfunc_name := facets._identifier_append(tablename, '_facets_trigger');
    trg_name := facets._identifier_append(tablename, '_facets_update');
    RETURN;
END;
$$;

-- Core data structures
CREATE TABLE facets.faceted_table (
    table_id oid primary key,
    schemaname text,
    tablename text,
    facets_table text,
    delta_table text,
    key name,
    key_type text,
    chunk_bits int,
    bm25_language text DEFAULT 'english'  -- Language for BM25 text search (PostgreSQL text search configuration)
);


--SELECT pg_catalog.pg_extension_config_dump('facets.faceted_table', '');

CREATE TABLE facets.facet_definition (
    table_id oid NOT NULL REFERENCES facets.faceted_table (table_id),
    facet_id int NOT NULL,
    facet_name text NOT NULL,
    facet_type text NOT NULL,
    base_column name,
    params jsonb,
    is_multi bool not null,
    supports_delta bool not null,
    PRIMARY KEY (table_id, facet_id)
);

CREATE UNIQUE INDEX facet_definition_uniq_name ON facets.facet_definition (table_id, facet_name);

--SELECT pg_catalog.pg_extension_config_dump('facets.facet_definition', '');

-- Define common types
CREATE TYPE facets.facet_counts AS (
    facet_name text,
    facet_value text,
    cardinality bigint,
    facet_id int
);

CREATE TYPE facets.facet_filter AS (
    facet_name text,
    facet_value text
);

-- Helper function for fetching facet values from different facet types
CREATE FUNCTION facets._get_values_clause(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT facets.%s_facet_values($1, $2, $3)', fdef.facet_type) INTO result
            USING fdef, extra_cols, table_alias;
    RETURN result;
END;
$$;

-- Helper function for fetching subqueries from different facet types
CREATE FUNCTION facets._get_subquery_clause(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    result text;
    sql_call text;
BEGIN
    sql_call := format('SELECT facets.%s_facet_subquery($1, $2, $3)', fdef.facet_type);
    EXECUTE sql_call INTO result USING fdef, extra_cols, table_alias;
    RETURN result;
END;
$$;

-- SECTION 2: FACET TYPE DEFINITIONS
-- Different types of facets supported by the system

-- Plain facet type - direct column value
CREATE FUNCTION facets.plain_facet(col name, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE SQL AS $$
        SELECT null::oid, null::int, coalesce(p_facet_name, col), 'plain', col, '{}'::jsonb, false, true;
    $$;

CREATE FUNCTION facets.plain_facet_values(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, %s%I::text%s)', fdef.facet_id, table_alias, fdef.base_column, extra_cols);
        END;
    $$;

-- Date/time truncation facet
CREATE FUNCTION facets.datetrunc_facet(col name, "precision" text, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE SQL AS $$
        SELECT null::int, null::int, coalesce(p_facet_name, col), 'datetrunc', col, jsonb_build_object('precision', "precision"), false, true;
    $$;

CREATE FUNCTION facets.datetrunc_facet_values(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, date_trunc(%L, %s%I)::text%s)',
                fdef.facet_id, fdef.params->>'precision', table_alias, fdef.base_column, extra_cols);
        END;
    $$;

-- Bucket facet type - numeric value ranges
CREATE FUNCTION facets.bucket_facet(col name, buckets anyarray, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE SQL AS $$
        SELECT null::int, null::int, coalesce(p_facet_name, col), 'bucket', col, jsonb_build_object('buckets', buckets::text), false, true;
    $$;

CREATE FUNCTION facets.bucket_facet_values(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, width_bucket(%s%I, %L)::text%s)',
                fdef.facet_id, table_alias, fdef.base_column, fdef.params->>'buckets', extra_cols);
        END;
    $$;

-- Array facet type - column with array values
CREATE FUNCTION facets.array_facet(col name, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE SQL AS $$
        SELECT null::oid, null::int, coalesce(p_facet_name, col), 'array', col, '{}'::jsonb, true, true;
    $$;

CREATE FUNCTION facets.array_facet_subquery(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(SELECT %s AS facet_id, element_value::text AS facet_value%s FROM unnest(%s%I) AS element_value)',
                fdef.facet_id, extra_cols, table_alias, fdef.base_column);
        END;
    $$;

-- Joined plain facet type - values from joined tables
CREATE FUNCTION facets.joined_plain_facet(col text, from_clause text, correlation text, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE plpgsql AS $$
        DECLARE
            base_col_name text;
        BEGIN
            SELECT ident[array_upper(ident, 1)] INTO base_col_name FROM parse_ident(col) ident;
            RETURN row(null::oid, null::int, coalesce(p_facet_name, base_col_name), 'joined_plain'::text, NULL::name,
                jsonb_build_object('col', col, 'from_clause', from_clause, 'correlation', correlation),
                true, false);
        END;
    $$;

CREATE FUNCTION facets.joined_plain_facet_subquery(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        DECLARE
            correlation_clause text;
        BEGIN
            correlation_clause := replace(fdef.params->>'correlation', '{TABLE}.', table_alias);
            RETURN format('(SELECT %s, %s::text%s FROM %s WHERE %s)',
                fdef.facet_id, fdef.params->>'col', extra_cols, fdef.params->>'from_clause', correlation_clause);
        END;
    $$;

-- Function facet type - custom function returning values
CREATE FUNCTION facets.function_facet(function_name text, facet_name text, base_column text DEFAULT NULL, parent_facet name DEFAULT NULL)
    RETURNS facets.facet_definition AS $$
    BEGIN
        RETURN ROW(
            NULL::oid, NULL::int,
            facet_name,
            'function',
            NULL::name,
            jsonb_build_object('function', function_name, 'base_column', base_column, 'parent_facet', parent_facet),
            FALSE, TRUE
        )::facets.facet_definition;
    END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION facets.function_facet_values(fdef facets.facet_definition, extra_cols text, table_alias text)
RETURNS text AS $$
BEGIN
    RETURN format('(%s, %s(%s%s)%s)',
        fdef.facet_id,
        fdef.params->>'function', 
        table_alias,
        'id',
        extra_cols
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION facets.function_facet_subquery(fdef facets.facet_definition, extra_cols text, table_alias text)
RETURNS text AS $$
BEGIN
    RETURN format('(SELECT %s, %s(%s%s)%s)',
        fdef.facet_id,
        fdef.params->>'function',
        table_alias,
        'id',
        extra_cols
    );
END;
$$ LANGUAGE plpgsql;

-- Function array facet type - custom function returning arrays
CREATE FUNCTION facets.function_array_facet(function_name text, facet_name text, base_column text DEFAULT NULL)
RETURNS facets.facet_definition AS $$
BEGIN
    RETURN ROW(
        NULL::oid, NULL::int, 
        facet_name,
        'function_array',
        NULL::name,
        jsonb_build_object('function', function_name, 'base_column', base_column),
        TRUE, TRUE
    )::facets.facet_definition;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION facets.function_array_facet_subquery(
    fdef facets.facet_definition,
    extra_cols text,
    table_alias text)
RETURNS text AS $$
BEGIN
    RETURN format(
        '(SELECT %s AS facet_id, elem AS facet_value%s FROM unnest(%s(%sid)) AS elem)',
        fdef.facet_id,
        extra_cols,
        fdef.params->>'function',
        table_alias
    );
END;
$$ LANGUAGE plpgsql;

-- Boolean facet type - direct column value with boolean handling
CREATE FUNCTION facets.boolean_facet(col name, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE SQL AS $$
        SELECT null::oid, null::int, coalesce(p_facet_name, col), 'boolean', col, '{}'::jsonb, false, true;
    $$;

CREATE FUNCTION facets.boolean_facet_values(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, CASE WHEN %s%I THEN ''true'' ELSE ''false'' END::text%s)', 
                fdef.facet_id, table_alias, fdef.base_column, extra_cols);
        END;
    $$;

CREATE FUNCTION facets.boolean_facet_subquery(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(SELECT %s AS facet_id, CASE WHEN %s%I THEN ''true'' ELSE ''false'' END AS facet_value%s)',
                fdef.facet_id, table_alias, fdef.base_column, extra_cols);
        END;
    $$;

-- Rating facet type - numeric rating values
CREATE FUNCTION facets.rating_facet(col name, p_facet_name text = null)
    RETURNS facets.facet_definition
    LANGUAGE SQL AS $$
        SELECT null::oid, null::int, coalesce(p_facet_name, col), 'rating', col, '{}'::jsonb, false, true;
    $$;

CREATE FUNCTION facets.rating_facet_values(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(%s, %s%I::text%s)', fdef.facet_id, table_alias, fdef.base_column, extra_cols);
        END;
    $$;

CREATE FUNCTION facets.rating_facet_subquery(fdef facets.facet_definition, extra_cols text, table_alias text)
    RETURNS text
    LANGUAGE plpgsql AS $$
        BEGIN
            RETURN format('(SELECT %s AS facet_id, %s%I::text AS facet_value%s)',
                fdef.facet_id, table_alias, fdef.base_column, extra_cols);
        END;
    $$;

-- SECTION 3: TABLE MANAGEMENT FUNCTIONS
-- Functions to add/remove faceting to tables

-- Helper function to determine optimal chunk bits based on table size
CREATE OR REPLACE FUNCTION facets.optimal_chunk_bits(
    p_table regclass
) RETURNS int AS $$
DECLARE
    row_count bigint;
    optimal_bits int;
BEGIN
    -- Get approximate row count using statistics for better performance
    SELECT reltuples::bigint INTO row_count
    FROM pg_class
    WHERE oid = p_table;

    -- Estimate optimal chunk bits based on data size
    IF row_count < 100000 THEN
        optimal_bits := 16; -- Smaller chunks for small datasets
    ELSIF row_count < 1000000 THEN
        optimal_bits := 18; -- Medium chunks for medium datasets
    ELSIF row_count < 10000000 THEN
        optimal_bits := 20; -- Default for large datasets
    ELSE
        optimal_bits := 22; -- Larger chunks for very large datasets
    END IF;

    RETURN optimal_bits;
END;
$$ LANGUAGE plpgsql STABLE;

-- Add faceting to a table
CREATE OR REPLACE FUNCTION facets.add_faceting_to_table(
    p_table regclass,
    key name,
    facets facets.facet_definition[],
    chunk_bits int = NULL, -- Default to NULL to trigger auto-detection
    keep_deltas bool = true,
    populate bool = true,
    skip_table_creation bool = false  -- If true, skip creating facet/delta tables if they already exist
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
BEGIN
    SELECT relname, nspname INTO tablename, schemaname
        FROM pg_class c JOIN pg_namespace n ON relnamespace = n.oid WHERE c.oid = p_table::oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Cannot find table %', p_table;
    END IF;

    -- Determine chunk bits: use provided value or calculate optimal
    v_chunk_bits := COALESCE(chunk_bits, facets.optimal_chunk_bits(p_table));

    -- Validate chunk_bits (can't use highest bit of int4)
    IF v_chunk_bits NOT BETWEEN 1 AND 31 THEN
        RAISE EXCEPTION 'Invalid number of bits per chunk: %', v_chunk_bits;
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

    IF keep_deltas THEN
        SELECT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = schemaname AND c.relname = delta_tablename
        ) INTO delta_table_exists;
    ELSE
        delta_table_exists := false;
    END IF;

    -- Verify key column exists and has supported type
    SELECT t.typname INTO key_type FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
                                       WHERE attrelid = p_table::oid AND attname = key;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Key column % not found in %s.%s', key, schemaname, tablename;
    ELSIF key_type NOT IN ('int2', 'int4', 'int8') THEN
        RAISE EXCEPTION 'Key column type % is not supported.', key_type;
    END IF;

    -- Insert table metadata (or get existing if already registered)
    INSERT INTO facets.faceted_table (table_id, schemaname, tablename, facets_table, delta_table, key,
                                        key_type, chunk_bits)
    VALUES (p_table::oid, schemaname, tablename, facet_tablename, CASE WHEN keep_deltas THEN delta_tablename END, key,
            key_type, v_chunk_bits)
    ON CONFLICT (table_id) DO UPDATE SET
        schemaname = EXCLUDED.schemaname,
        tablename = EXCLUDED.tablename,
        facets_table = EXCLUDED.facets_table,
        delta_table = EXCLUDED.delta_table,
        key = EXCLUDED.key,
        key_type = EXCLUDED.key_type,
        chunk_bits = EXCLUDED.chunk_bits
    RETURNING table_id INTO v_table_id;

    -- Insert facet definitions
    WITH stored_definitions AS (
        INSERT INTO facets.facet_definition (table_id, facet_id, facet_name, facet_type, base_column, params, is_multi, supports_delta)
            SELECT v_table_id, assigned_id, facet_name, facet_type, base_column, params, is_multi, supports_delta
            FROM UNNEST(facets) WITH ORDINALITY AS x(_, _, facet_name, facet_type, base_column, params, is_multi, supports_delta, assigned_id)
            RETURNING *)
    SELECT array_agg(f) INTO v_facet_defs FROM stored_definitions f;

    -- Create facet storage with new schema (id and children_bitmap)
    -- Skip creation if skip_table_creation is true and table already exists
    IF NOT (skip_table_creation AND facet_table_exists) THEN
        -- IMPORTANT CHANGE: Added UNIQUE constraint to match the ON CONFLICT specification
        EXECUTE format($sql$
            CREATE TABLE IF NOT EXISTS %s (
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
            ALTER TABLE %s SET (toast_tuple_target = 8160);$sql$,
            facets._qualified(schemaname, facet_tablename),
            facet_tablename, facets._qualified(schemaname, facet_tablename),
            facets._qualified(schemaname, facet_tablename));
    ELSIF facet_table_exists THEN
        -- Table exists and we're skipping creation, but ensure index exists
        EXECUTE format($sql$
            CREATE INDEX IF NOT EXISTS %s_facet_lookup ON %s (facet_id, facet_value, chunk_id);
        $sql$, facet_tablename, facets._qualified(schemaname, facet_tablename));
    END IF;

    -- Create delta table if needed
    -- Skip creation if skip_table_creation is true and table already exists
    IF keep_deltas AND NOT (skip_table_creation AND delta_table_exists) THEN
        -- Delta storage
        EXECUTE format($sql$
            CREATE TABLE IF NOT EXISTS %s (
                facet_id int4 NOT NULL,
                facet_value text COLLATE "C" NULL,
                posting %s NOT NULL,
                delta int2,
                primary key (facet_id, facet_value, posting)
            );
            $sql$, facets._qualified(schemaname, delta_tablename), key_type);

        -- Create delta trigger
        PERFORM facets.create_delta_trigger(v_table_id);
    END IF;

    -- Populate facets if requested
    IF populate THEN
        PERFORM facets.populate_facets(v_table_id, false);
    END IF;
END;
$$;

-- Add new facets to an already faceted table
CREATE OR REPLACE FUNCTION facets.add_facets(
    p_table regclass,
    facets facets.facet_definition[],
    populate bool = true
) RETURNS SETOF int4 LANGUAGE plpgsql AS $$
DECLARE
    v_table_id oid;
    tdef facets.faceted_table;
    highest_facet_id int4;
    v_facet_names text[];
    v_facet_ids int4[];
BEGIN
    v_table_id := p_table::oid;
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = v_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % is not faceted', p_table;
    END IF;
    
    -- Get highest existing facet_id
    SELECT MAX(facet_id) INTO highest_facet_id FROM facets.facet_definition WHERE table_id = v_table_id;
    IF highest_facet_id IS NULL THEN
        highest_facet_id := 0;
    END IF;

    -- Insert new facet definitions
    WITH stored_definitions AS (
        INSERT INTO facets.facet_definition (table_id, facet_id, facet_name, facet_type, base_column, params, is_multi, supports_delta)
            SELECT v_table_id, highest_facet_id + assigned_id, facet_name, facet_type, base_column, params, is_multi, supports_delta
            FROM UNNEST(facets) WITH ORDINALITY AS x(_, _, facet_name, facet_type, base_column, params, is_multi, supports_delta, assigned_id)
            RETURNING *)
    SELECT array_agg(f.facet_name), array_agg(f.facet_id) INTO v_facet_names, v_facet_ids FROM stored_definitions f;

    -- Update delta trigger if needed
    IF tdef.delta_table IS NOT NULL THEN
        PERFORM facets.create_delta_trigger(v_table_id);
    END IF;

    -- Populate new facets if requested - FIXED: use facets parameter name instead of p_facet_names
    IF populate THEN
        PERFORM facets.populate_facets(v_table_id, false, facets := v_facet_names);
    END IF;
    
    -- Return the new facet IDs
    RETURN QUERY SELECT unnest(v_facet_ids);
END;
$$;

-- Drop facets from a table
CREATE FUNCTION facets.drop_facets(
    p_table regclass,
    facet_names text[]
) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE
    v_table_id oid;
    tdef facets.faceted_table;
    v_dropped_ids int4[];
    v_dropped_names text[];
BEGIN
    v_table_id := p_table::oid;
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = v_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % is not faceted', p_table;
    END IF;

    -- Delete facet definitions and get dropped IDs and names
    WITH deleted_facets AS (
        DELETE FROM facets.facet_definition
        WHERE table_id = v_table_id AND facet_name = ANY(facet_names)
        RETURNING facet_id, facet_name
    )
    SELECT array_agg(facet_id), array_agg(facet_name) INTO v_dropped_ids, v_dropped_names FROM deleted_facets;

    -- If no facets were dropped, return empty set
    IF v_dropped_ids IS NULL THEN
        RETURN;
    END IF;

    -- Delete facet data
    EXECUTE format('DELETE FROM %s WHERE facet_id = ANY ($1)',
                   facets._qualified(tdef.schemaname, tdef.facets_table))
        USING v_dropped_ids;

    -- Delete delta data if delta table exists
    IF tdef.delta_table IS NOT NULL THEN
        EXECUTE format('DELETE FROM %s WHERE facet_id = ANY ($1)',
                       facets._qualified(tdef.schemaname, tdef.delta_table))
            USING v_dropped_ids;
    END IF;

    -- Return names of dropped facets
    RETURN QUERY SELECT unnest(v_dropped_names);
END;
$$;

-- Completely remove faceting from a table
CREATE FUNCTION facets.drop_faceting(
    p_table regclass
) RETURNS bool LANGUAGE plpgsql AS $$
DECLARE
    v_table_id oid;
    tdef facets.faceted_table;
    tfunc_name text;
    trg_name text;
BEGIN
    v_table_id := p_table::oid;
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = v_table_id;
    IF NOT FOUND THEN
        RAISE NOTICE 'Table % is not faceted', p_table;
        RETURN true;
    END IF;

    -- Lock the table before making changes
    EXECUTE format('LOCK TABLE %s', facets._qualified(tdef.schemaname, tdef.tablename));

    -- Delete facet definitions and metadata
    DELETE FROM facets.facet_definition WHERE table_id = v_table_id;
    DELETE FROM facets.faceted_table WHERE table_id = v_table_id;

    -- Drop facets table
    EXECUTE format('DROP TABLE %s', facets._qualified(tdef.schemaname, tdef.facets_table));
    
    -- Drop delta table and triggers if they exist
    IF tdef.delta_table IS NOT NULL THEN
        SELECT tn.tfunc_name, tn.trg_name INTO tfunc_name, trg_name FROM facets._trigger_names(tdef.tablename) tn;
        EXECUTE format('DROP TRIGGER %s ON %s', trg_name, facets._qualified(tdef.schemaname, tdef.tablename));
        EXECUTE format('DROP TRIGGER %s_ins_upd ON %s', trg_name, facets._qualified(tdef.schemaname, tdef.tablename));
        EXECUTE format('DROP FUNCTION %s', facets._qualified(tdef.schemaname, tfunc_name));
        EXECUTE format('DROP TABLE %s', facets._qualified(tdef.schemaname, tdef.delta_table));
    END IF;

    RETURN true;
END;
$$;

-- SECTION 4: FACET POPULATION AND MAINTENANCE
-- Functions for populating facets, managing deltas and maintenance operations

-- Generate SQL to populate facets- return 2 queries: one to populate the facets table, one to update the children_bitmap
CREATE OR REPLACE FUNCTION facets.populate_facets_query(p_table_id oid, facets text[] = null)
RETURNS text[] LANGUAGE plpgsql AS $$
DECLARE
    sql text;
    values_entries text[];
    subquery_entries text[];
    clauses text[];
    v_chunk_bits int;
    v_keycol name;
    tdef facets.faceted_table;
    child_bitmap_update text;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    SELECT chunk_bits, key INTO v_chunk_bits, v_keycol FROM facets.faceted_table WHERE table_id = p_table_id;
    
    -- Get values entries for non-multi facets
    SELECT array_agg(facets._get_values_clause(fd, '', 'd.') ORDER BY facet_id) INTO values_entries
            FROM facets.facet_definition fd WHERE (facets IS NULL OR fd.facet_name = ANY (facets))
                                                   AND table_id = p_table_id AND NOT fd.is_multi;

    -- Get subquery entries for multi-value facets
    SELECT array_agg(facets._get_subquery_clause(fd, '', 'd.')) INTO subquery_entries
            FROM facets.facet_definition fd WHERE (facets IS NULL OR fd.facet_name = ANY (facets))
                                                   AND table_id = p_table_id AND fd.is_multi;

    -- Build combined clauses
    IF array_length(values_entries, 1) > 0 THEN
        clauses := array[format('
            SELECT v.facet_id, v.facet_value FROM (
                VALUES %s
            ) AS v(facet_id, facet_value)', 
            array_to_string(values_entries, E',\n               '))];
    ELSE
        clauses := array[]::text[];
    END IF;

    -- Add subquery entries if any exist
    IF array_length(subquery_entries, 1) > 0 THEN
        clauses := clauses || array[format('
            SELECT v.facet_id, v.facet_value FROM (
                %s
            ) AS v(facet_id, facet_value)', 
            array_to_string(subquery_entries, E'\n            UNION ALL\n        '))];
    END IF;

    -- If no clauses exist (no facets defined), use a dummy subquery that returns no rows
    IF array_length(clauses, 1) IS NULL OR array_length(clauses, 1) = 0 THEN
        clauses := ARRAY['SELECT NULL::int AS facet_id, NULL::text AS facet_value WHERE false'];
    END IF;

    -- First populate the facets table with base data - FIXED to use proper column aliasing
    sql := format($sql$
WITH data_insertion AS (
    WITH distinct_facets AS (
        SELECT DISTINCT vals.facet_id, vals.facet_value, (%s >> %s)::int4 AS chunk_id, 
                        (%s & ((1 << %s) - 1))::int4 AS in_chunk_id
        FROM %s d,
        LATERAL (
            %s
        ) vals(facet_id, facet_value)
        WHERE vals.facet_value IS NOT NULL
    )
    INSERT INTO %s (facet_id, chunk_id, facet_value, postinglist)
    SELECT facet_id, chunk_id, facet_value COLLATE "C", -- Changed from POSIX to C
        rb_build_agg(in_chunk_id ORDER BY in_chunk_id)
    FROM distinct_facets
    GROUP BY facet_id, facet_value COLLATE "C", chunk_id -- Changed from POSIX to C
    ON CONFLICT (facet_id, facet_value, chunk_id) DO NOTHING
)
SELECT 1;
$sql$,
        v_keycol, v_chunk_bits, v_keycol, v_chunk_bits, 
        p_table_id::regclass::text,
        array_to_string(clauses, E'\n            UNION ALL\n        '),
        facets._qualified(tdef.schemaname, tdef.facets_table)
    );

    -- Second query for updating children_bitmap - Unchanged
    -- Remember to properly escape the single quotes in the JSON path extraction
    child_bitmap_update := format($sql$
-- Update children_bitmap for hierarchical facets
WITH parent_child_facets AS (
    SELECT 
        pfd.facet_id AS parent_facet_id,
        cfd.facet_id AS child_facet_id
    FROM 
        facets.facet_definition pfd
    JOIN 
        facets.facet_definition cfd ON cfd.params->>'parent_facet' = pfd.facet_name
    WHERE 
        pfd.table_id = %L AND cfd.table_id = %L
        %s
),
parent_child_values AS (
    SELECT 
        p.id AS parent_id,
        array_agg(c.id) AS child_ids
    FROM 
        %I.%I p
    JOIN 
        %I.%I c ON rb_intersect(c.postinglist, p.postinglist)
    JOIN 
        parent_child_facets pcf ON p.facet_id = pcf.parent_facet_id AND c.facet_id = pcf.child_facet_id
    GROUP BY 
        p.id
)
UPDATE %I.%I f
SET children_bitmap = rb_build(ARRAY(SELECT unnest(pc.child_ids)))
FROM parent_child_values pc
WHERE f.id = pc.parent_id;
$sql$,
        p_table_id, p_table_id,
        CASE WHEN facets IS NOT NULL THEN format('AND (pfd.facet_name = ANY(%L) OR cfd.facet_name = ANY(%L))', facets, facets) ELSE '' END,
        tdef.schemaname, tdef.facets_table,
        tdef.schemaname, tdef.facets_table,
        tdef.schemaname, tdef.facets_table
    );

    -- Return both queries as an array
    RETURN ARRAY[sql, child_bitmap_update];
END;
$$;

CREATE OR REPLACE FUNCTION facets.populate_facets(
    p_table_id oid, 
    p_use_copy bool = false, 
    debug bool = false, 
    facets text[] = null
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    queries text[];
BEGIN
    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

    -- Get SQL queries
    queries := facets.populate_facets_query(p_table_id, facets => facets);
    
    IF debug THEN
        RAISE NOTICE 'facets.populate_facets query 1=%', queries[1];
        RAISE NOTICE 'facets.populate_facets query 2=%', queries[2];
    END IF;

    -- Execute using COPY if requested
    IF p_use_copy THEN
        EXECUTE format($copy$COPY %s FROM PROGRAM $prog$ psql -h localhost %s -c "COPY (%s) TO STDOUT" $prog$ $copy$,
            facets._qualified(tdef.schemaname, tdef.facets_table),
            current_database(),
            replace(queries[1], '"', '\"'));
        -- Note: The second query can't be executed with COPY as it's an UPDATE
        EXECUTE queries[2];
        RETURN;
    END IF;

    -- Execute queries separately
    EXECUTE queries[1];
    EXECUTE queries[2];
END;
$$;

-- =============================================================================
-- FAST BATCH REFRESH: Rebuild all facets from scratch (bypasses triggers)
-- =============================================================================
-- Use this for:
-- 1. Initial population of large tables (millions of rows)
-- 2. Full refresh after bulk data changes
-- 3. When merge_deltas is too slow
--
-- This is MUCH faster than trigger-based updates for large datasets because:
-- - Uses single INSERT with rb_build_agg() aggregation
-- - No per-row trigger overhead
-- - No delta table processing
-- =============================================================================

CREATE OR REPLACE FUNCTION facets.refresh_facets(
    p_table regclass,
    p_truncate bool DEFAULT true,
    p_disable_triggers bool DEFAULT true
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_table_id oid;
    v_start_time timestamp;
    v_elapsed interval;
BEGIN
    v_start_time := clock_timestamp();
    v_table_id := p_table::oid;
    
    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = v_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % is not faceted. Use facets.add_faceting_to_table() first.', p_table;
    END IF;
    
    RAISE NOTICE '[facets.refresh_facets] Starting refresh for %.%', tdef.schemaname, tdef.tablename;
    
    -- Optimize session settings for bulk operations
    SET LOCAL work_mem = '512MB';
    SET LOCAL maintenance_work_mem = '2GB';
    
    -- Disable triggers on source table to avoid delta accumulation during refresh
    IF p_disable_triggers THEN
        EXECUTE format('ALTER TABLE %I.%I DISABLE TRIGGER ALL', tdef.schemaname, tdef.tablename);
        RAISE NOTICE '[facets.refresh_facets] Triggers disabled';
    END IF;
    
    -- Truncate existing facets if requested
    IF p_truncate THEN
        EXECUTE format('TRUNCATE TABLE %I.%I', tdef.schemaname, tdef.facets_table);
        RAISE NOTICE '[facets.refresh_facets] Facets table truncated';
        
        -- Also clear deltas if delta table exists
        IF tdef.delta_table IS NOT NULL THEN
            EXECUTE format('TRUNCATE TABLE %I.%I', tdef.schemaname, tdef.delta_table);
            RAISE NOTICE '[facets.refresh_facets] Delta table truncated';
        END IF;
    END IF;
    
    -- Populate facets using efficient batch INSERT with rb_build_agg
    RAISE NOTICE '[facets.refresh_facets] Populating facets (this may take several minutes for large tables)...';
    PERFORM facets.populate_facets(v_table_id, false);
    
    -- Re-enable triggers
    IF p_disable_triggers THEN
        EXECUTE format('ALTER TABLE %I.%I ENABLE TRIGGER ALL', tdef.schemaname, tdef.tablename);
        RAISE NOTICE '[facets.refresh_facets] Triggers re-enabled';
    END IF;
    
    v_elapsed := clock_timestamp() - v_start_time;
    RAISE NOTICE '[facets.refresh_facets] Completed in %', v_elapsed;
END;
$$;

-- Convenience wrapper that takes schema.table as text
CREATE OR REPLACE FUNCTION facets.refresh_facets(
    p_table_name text,
    p_truncate bool DEFAULT true,
    p_disable_triggers bool DEFAULT true
) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM facets.refresh_facets(p_table_name::regclass, p_truncate, p_disable_triggers);
END;
$$;

-- =============================================================================
-- SIMPLE SETUP HELPER: One-call faceting setup
-- =============================================================================
-- Creates a faceting-ready table from a source query and sets up facets
-- =============================================================================

CREATE OR REPLACE FUNCTION facets.setup_simple(
    p_target_schema text,
    p_target_table text,
    p_source_query text,           -- SELECT query that produces the data
    p_id_column text DEFAULT 'id', -- Column to use as document ID (must be integer)
    p_facet_columns text[] DEFAULT NULL, -- Columns to create plain facets for (auto-detect if NULL)
    p_content_column text DEFAULT 'content', -- Column for full-text search
    p_chunk_bits int DEFAULT 20,   -- 2^20 = ~1M docs per chunk
    p_keep_deltas bool DEFAULT true -- Keep delta table for incremental updates
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_target_table text;
    v_facet_defs facets.facet_definition[];
    v_col text;
    v_start_time timestamp;
BEGIN
    v_start_time := clock_timestamp();
    v_target_table := format('%I.%I', p_target_schema, p_target_table);
    
    RAISE NOTICE '[facets.setup_simple] Creating table %', v_target_table;
    
    -- Create schema if not exists
    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_target_schema);
    
    -- Drop existing table if exists
    EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', v_target_table);
    
    -- Create table from source query
    EXECUTE format('CREATE TABLE %s AS %s', v_target_table, p_source_query);
    
    RAISE NOTICE '[facets.setup_simple] Table created, adding indexes...';
    
    -- Create essential indexes
    EXECUTE format('CREATE UNIQUE INDEX ON %s (%I)', v_target_table, p_id_column);
    
    -- Create GIN index for full-text search if content column exists
    BEGIN
        EXECUTE format('CREATE INDEX ON %s USING gin(to_tsvector(''english'', %I))', 
                       v_target_table, p_content_column);
    EXCEPTION WHEN undefined_column THEN
        RAISE NOTICE '[facets.setup_simple] No content column "%", skipping GIN index', p_content_column;
    END;
    
    -- Build facet definitions
    IF p_facet_columns IS NOT NULL AND array_length(p_facet_columns, 1) > 0 THEN
        SELECT array_agg(facets.plain_facet(col))
        INTO v_facet_defs
        FROM unnest(p_facet_columns) AS col;
    ELSE
        -- Auto-detect: use all text/varchar columns except id and content
        SELECT array_agg(facets.plain_facet(column_name::name))
        INTO v_facet_defs
        FROM information_schema.columns
        WHERE table_schema = p_target_schema
          AND table_name = p_target_table
          AND column_name NOT IN (p_id_column, p_content_column, 'metadata', 'created_at', 'updated_at')
          AND data_type IN ('text', 'character varying', 'boolean', 'integer', 'smallint');
    END IF;
    
    IF v_facet_defs IS NULL OR array_length(v_facet_defs, 1) = 0 THEN
        RAISE EXCEPTION 'No facet columns found or specified';
    END IF;
    
    RAISE NOTICE '[facets.setup_simple] Adding faceting with % facets...', array_length(v_facet_defs, 1);
    
    -- Add faceting (with populate=true to build facets immediately)
    PERFORM facets.add_faceting_to_table(
        v_target_table::regclass,
        p_id_column::name,
        v_facet_defs,
        p_chunk_bits,
        p_keep_deltas,
        true  -- populate
    );
    
    RAISE NOTICE '[facets.setup_simple] Completed in %', clock_timestamp() - v_start_time;
END;
$$;

-- Update children bitmap for affected facets
CREATE OR REPLACE FUNCTION facets._update_children_bitmap_for_deltas(p_table_id oid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    sql text;
BEGIN
    -- Set work_mem higher for this operation
    SET LOCAL work_mem = '256MB';

    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;

    -- Update children_bitmap for facets affected by recent deltas
    sql := format($sql$
    -- First identify affected facets
    WITH delta_facets AS MATERIALIZED (
        SELECT DISTINCT facet_id
        FROM %I.%I
        WHERE delta <> 0
    ),
    -- Pre-compute parent-child facet relationships
    parent_child_facets AS MATERIALIZED (
        SELECT
            pfd.facet_id AS parent_facet_id,
            cfd.facet_id AS child_facet_id
        FROM
            facets.facet_definition pfd
        JOIN
            facets.facet_definition cfd ON cfd.params->>'parent_facet' = pfd.facet_name
        WHERE
            pfd.table_id = %L AND cfd.table_id = %L
            AND (pfd.facet_id IN (SELECT facet_id FROM delta_facets)
                 OR cfd.facet_id IN (SELECT facet_id FROM delta_facets))
    ),
    -- Only select affected parent facets to reduce processing
    affected_parents AS MATERIALIZED (
        SELECT DISTINCT parent_facet_id
        FROM parent_child_facets
    ),
    -- Only select affected child facets to reduce processing
    affected_children AS MATERIALIZED (
        SELECT DISTINCT child_facet_id
        FROM parent_child_facets
    ),
    -- Pre-filter parent and child records
    parent_records AS MATERIALIZED (
        SELECT p.id, p.facet_id, p.postinglist
        FROM %I.%I p
        WHERE p.facet_id IN (SELECT parent_facet_id FROM affected_parents)
    ),
    child_records AS MATERIALIZED (
        SELECT c.id, c.facet_id, c.postinglist
        FROM %I.%I c
        WHERE c.facet_id IN (SELECT child_facet_id FROM affected_children)
    ),
    -- Use rb_intersect to check for intersection instead of && operator
    parent_child_values AS (
        SELECT
            p.id AS parent_id,
            array_agg(c.id) AS child_ids
        FROM
            parent_records p
        JOIN
            child_records c ON rb_intersect(c.postinglist, p.postinglist) -- Use rb_intersect
        JOIN
            parent_child_facets pcf ON p.facet_id = pcf.parent_facet_id AND c.facet_id = pcf.child_facet_id
        GROUP BY
            p.id
    )
    -- Update affected parent rows
    UPDATE %I.%I f
    SET children_bitmap = rb_build(ARRAY(SELECT unnest(pc.child_ids)))
    FROM parent_child_values pc
    WHERE f.id = pc.parent_id;
    $sql$,
        tdef.schemaname, tdef.delta_table, -- delta_facets
        p_table_id, p_table_id,           -- parent_child_facets
        tdef.schemaname, tdef.facets_table, -- parent_records
        tdef.schemaname, tdef.facets_table, -- child_records
        tdef.schemaname, tdef.facets_table  -- final UPDATE target
    );

    EXECUTE sql;
END;
$$;

-- Create delta trigger for tracking changes
CREATE OR REPLACE FUNCTION facets.create_delta_trigger(p_table_id oid, p_create bool = true)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    tfunc_name text;
    trg_name text;
    sql text;
    tdef facets.faceted_table;
    insert_values text[];
    insert_subqueries text[];
    insert_clauses text[];
    delete_values text[];
    delete_subqueries text[];
    delete_clauses text[];
    base_columns text[];
    children_bitmap_update text;
BEGIN
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    SELECT tn.tfunc_name, tn.trg_name INTO tfunc_name, trg_name FROM facets._trigger_names(tdef.tablename) tn;

    -- Collect base columns from all facet types
    SELECT array_agg(DISTINCT COALESCE(NULLIF(fd.base_column, ''), NULLIF(fd.params->>'base_column', ''))) 
    INTO base_columns
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id
      AND (fd.base_column IS NOT NULL OR fd.params ? 'base_column');

    -- Ensure no NULL values in base_columns
    base_columns := array_remove(base_columns, NULL);

    -- Plain and function facets
    SELECT array_agg(facets._get_values_clause(fd, format(', NEW.%I, 1', tdef.key), 'NEW.') ORDER BY facet_id),
           array_agg(facets._get_values_clause(fd, format(', OLD.%I, -1', tdef.key), 'OLD.') ORDER BY facet_id)
    INTO insert_values, delete_values
    FROM facets.facet_definition fd 
    WHERE table_id = p_table_id AND NOT fd.is_multi AND fd.supports_delta AND fd.facet_type NOT IN ('function_array', 'array');

    -- Multi-value and function_array facets
    SELECT array_agg(facets._get_subquery_clause(fd, format(', NEW.%I, 1', tdef.key), 'NEW.') ORDER BY facet_id),
           array_agg(facets._get_subquery_clause(fd, format(', OLD.%I, -1', tdef.key), 'OLD.') ORDER BY facet_id)
    INTO insert_subqueries, delete_subqueries
    FROM facets.facet_definition fd 
    WHERE table_id = p_table_id AND fd.is_multi AND fd.supports_delta AND fd.facet_type IN ('function_array', 'array');

    -- Combine clauses - Fix column aliasing to prevent ambiguity
    -- Use a dummy query that returns no rows if no clauses exist
    insert_clauses := CASE WHEN array_length(insert_values, 1) > 0 THEN array['VALUES ' || array_to_string(insert_values, E',\n                       ')] ELSE '{}'::text[] END || insert_subqueries;
    delete_clauses := CASE WHEN array_length(delete_values, 1) > 0 THEN array['VALUES ' || array_to_string(delete_values, E',\n                       ')] ELSE '{}'::text[] END || delete_subqueries;

    -- If no clauses exist, use a dummy subquery that returns no rows
    IF array_length(insert_clauses, 1) IS NULL OR array_length(insert_clauses, 1) = 0 THEN
        insert_clauses := ARRAY['SELECT NULL::int, NULL::text, NULL::bigint, NULL::int WHERE false'];
    END IF;
    IF array_length(delete_clauses, 1) IS NULL OR array_length(delete_clauses, 1) = 0 THEN
        delete_clauses := ARRAY['SELECT NULL::int, NULL::text, NULL::bigint, NULL::int WHERE false'];
    END IF;

    -- Add children_bitmap maintenance after delta inserts
    children_bitmap_update := format($sql$
        -- Update children_bitmap for affected facets
        -- This is triggered after facet changes to update parent-child relationships
        PERFORM facets._update_children_bitmap_for_deltas(%L);
    $sql$, tdef.table_id);

    -- Generate SQL with children_bitmap updates - Fix column aliasing
    sql := format($sql$
CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $func$
    BEGIN
        IF TG_OP = 'UPDATE' AND OLD.%I != NEW.%I THEN
            RAISE EXCEPTION 'Update of key column of faceted tables is not supported';
        END IF;

        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND (%s)) THEN
            INSERT INTO %s (facet_id, facet_value, posting, delta)
                SELECT vals.facet_id, vals.facet_value, vals.posting, vals.delta 
                FROM (%s) AS vals(facet_id, facet_value, posting, delta)
                WHERE vals.facet_value IS NOT NULL
                ON CONFLICT (facet_id, facet_value, posting) DO UPDATE
                    SET delta = EXCLUDED.delta + %s.delta;
        END IF;

        IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND (%s)) THEN
            INSERT INTO %s (facet_id, facet_value, posting, delta)
                SELECT vals.facet_id, vals.facet_value, vals.posting, vals.delta 
                FROM (%s) AS vals(facet_id, facet_value, posting, delta)
                WHERE vals.facet_value IS NOT NULL
                ON CONFLICT (facet_id, facet_value, posting) DO UPDATE
                SET delta = EXCLUDED.delta + %s.delta;
        END IF;
        
        %s
        
        RETURN NULL;
    END;
$func$ LANGUAGE plpgsql;

-- Modified to fire BEFORE DELETE and AFTER INSERT/UPDATE
CREATE OR REPLACE TRIGGER %s
    BEFORE DELETE ON %s
    FOR EACH ROW EXECUTE FUNCTION %s();

CREATE OR REPLACE TRIGGER %s_ins_upd -- Separate trigger for INSERT/UPDATE
    AFTER INSERT OR UPDATE %s ON %s
    FOR EACH ROW EXECUTE FUNCTION %s();
$sql$,
        facets._qualified(tdef.schemaname, tfunc_name),
        tdef.key, tdef.key,
        COALESCE((SELECT string_agg(format('OLD.%1$I IS DISTINCT FROM NEW.%1$I', col), ' OR ') FROM unnest(base_columns) AS col), 'FALSE'),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        array_to_string(insert_clauses, E'\n                    UNION ALL\n                '),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        COALESCE((SELECT string_agg(format('OLD.%1$I IS DISTINCT FROM NEW.%1$I', col), ' OR ') FROM unnest(base_columns) AS col), 'FALSE'),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        array_to_string(delete_clauses, E'\n                    UNION ALL\n                '),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        children_bitmap_update,
        -- Trigger for DELETE
        trg_name, -- Original trigger name
        facets._qualified(tdef.schemaname, tdef.tablename),
        facets._qualified(tdef.schemaname, tfunc_name),
        -- Trigger for INSERT/UPDATE - use "OF columns" only if there are columns
        trg_name, -- Original name + suffix
        CASE WHEN array_length(base_columns, 1) > 0 
             THEN 'OF ' || array_to_string(base_columns, ', ')
             ELSE '' 
        END,
        facets._qualified(tdef.schemaname, tdef.tablename),
        facets._qualified(tdef.schemaname, tfunc_name)
    );

    IF p_create THEN
        EXECUTE sql;
    END IF;

    RETURN sql;
END;
$$;

-- Apply deltas to facets table
CREATE OR REPLACE FUNCTION facets.apply_deltas(
    p_table_id oid,
    p_facet_id int4 = NULL
) RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    v_chunk_bits int;
    v_count int;
    sql text;
    delta_rec RECORD;
BEGIN
    RAISE NOTICE 'Running facets.apply_deltas - v20240716-1530'; -- Version identifier

    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

    -- Get chunk bits
    SELECT chunk_bits INTO v_chunk_bits FROM facets.faceted_table WHERE table_id = p_table_id;

    -- Create temporary table for grouped deltas
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_grouped_deltas (
        facet_id int4 NOT NULL,
        facet_value text COLLATE "C" NULL,
        chunk_id int4 NOT NULL,
        add_bitmap roaringbitmap,
        remove_bitmap roaringbitmap,
        PRIMARY KEY (facet_id, facet_value, chunk_id)
    ) ON COMMIT DROP;

    -- Ensure the temp table is empty before use
    TRUNCATE tmp_grouped_deltas;

    -- Populate the temporary table
    sql := format($sql$
    WITH delta_data AS (
        SELECT
            facet_id,
            facet_value,
            (posting >> %s)::int4 AS chunk_id,
            (posting & ((1 << %s) - 1))::int4 AS in_chunk_id,
            delta
        FROM %s -- delta_table
        WHERE delta <> 0
        %s -- facet_id filter
    )
    INSERT INTO tmp_grouped_deltas (facet_id, facet_value, chunk_id, add_bitmap, remove_bitmap)
    SELECT
        facet_id,
        facet_value COLLATE "C",
        chunk_id,
        rb_build_agg(CASE WHEN delta > 0 THEN in_chunk_id END) AS add_bitmap,
        rb_build_agg(CASE WHEN delta < 0 THEN in_chunk_id END) AS remove_bitmap
    FROM delta_data
    GROUP BY facet_id, facet_value COLLATE "C", chunk_id;
    $sql$,
        v_chunk_bits, v_chunk_bits,
        facets._qualified(tdef.schemaname, tdef.delta_table),
        CASE WHEN p_facet_id IS NOT NULL THEN format('AND facet_id = %s', p_facet_id) ELSE '' END
    );
    EXECUTE sql;

    -- Log the contents of the temporary table
    RAISE NOTICE '--- apply_deltas: Grouped Deltas ---';
    FOR delta_rec IN SELECT * FROM tmp_grouped_deltas LOOP
        RAISE NOTICE '  facet_id: %, facet_value: %, chunk_id: %, add: %, remove: %',
            delta_rec.facet_id,
            delta_rec.facet_value,
            delta_rec.chunk_id,
            rb_to_array(COALESCE(delta_rec.add_bitmap, rb_build('{}'))),
            rb_to_array(COALESCE(delta_rec.remove_bitmap, rb_build('{}')));
    END LOOP;
    RAISE NOTICE '-----------------------------------';

    -- Refactored logic using the temporary table
    sql := format($sql$
    WITH perform_updates AS (
        UPDATE %s ft -- facets_table
        SET postinglist = rb_or(
                rb_andnot(ft.postinglist, COALESCE(gd.remove_bitmap, rb_build('{}'))),
                COALESCE(gd.add_bitmap, rb_build('{}'))
            )
        FROM tmp_grouped_deltas gd
        WHERE ft.facet_id = gd.facet_id
          AND ft.facet_value = gd.facet_value -- Collation matches temp table
          AND ft.chunk_id = gd.chunk_id
        RETURNING ft.id -- Return the ID of the updated row
    ),
    -- NEW: Delete rows where postinglist became empty after the update
    deleted_empty_rows AS (
        DELETE FROM %s ft -- facets_table
        WHERE ft.id IN (SELECT id FROM perform_updates)
          AND rb_is_empty(ft.postinglist) -- Check if the postinglist is now empty
        RETURNING 1
    ),
    inserted_new AS (
        INSERT INTO %s (facet_id, chunk_id, facet_value, postinglist) -- facets_table
        SELECT
            gd.facet_id,
            gd.chunk_id,
            gd.facet_value, -- Collation matches temp table
            gd.add_bitmap
        FROM tmp_grouped_deltas gd
        WHERE NOT EXISTS (
            SELECT 1
            FROM %s ft -- facets_table
            WHERE ft.facet_id = gd.facet_id
              AND ft.facet_value = gd.facet_value -- Collation matches temp table
              AND ft.chunk_id = gd.chunk_id
        )
        AND gd.add_bitmap IS NOT NULL
        AND NOT rb_is_empty(gd.add_bitmap)
        RETURNING 1 -- Return 1 to count affected rows
    ),
    deleted_deltas AS (
        DELETE FROM %s -- delta_table
        WHERE delta <> 0
        %s -- facet_id filter
        RETURNING 1
    )
    SELECT (SELECT count(*) FROM perform_updates) + 
           (SELECT count(*) FROM inserted_new) + 
           (SELECT count(*) FROM deleted_empty_rows); -- Include deleted rows in the count
    $sql$,
        facets._qualified(tdef.schemaname, tdef.facets_table), -- perform_updates UPDATE target
        facets._qualified(tdef.schemaname, tdef.facets_table), -- deleted_empty_rows DELETE target
        facets._qualified(tdef.schemaname, tdef.facets_table), -- inserted_new INSERT target
        facets._qualified(tdef.schemaname, tdef.facets_table), -- inserted_new NOT EXISTS subquery FROM
        facets._qualified(tdef.schemaname, tdef.delta_table),  -- deleted_deltas DELETE target
        CASE WHEN p_facet_id IS NOT NULL THEN format('AND facet_id = %s', p_facet_id) ELSE '' END -- deleted_deltas WHERE filter
    );

    EXECUTE sql INTO v_count;

    -- Update children_bitmap for affected facets
    PERFORM facets._update_children_bitmap_for_deltas(p_table_id);

    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION facets.merge_deltas(p_table_id oid)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- Simply call the new apply_deltas function and discard the result
    PERFORM facets.apply_deltas(p_table_id);
END;
$$;




CREATE OR REPLACE FUNCTION facets.rebuild_hierarchy(target_table regclass)
RETURNS VOID AS $$
DECLARE
    facets_table_name regclass;
    facets_schema_name text;
    target_table_oid oid;
BEGIN
    target_table_oid := target_table::oid;

    -- Construct the name of the facets table (e.g., 'hier_test.products_facets')
    SELECT quote_ident(nspname), quote_ident(relname || '_facets')
    INTO facets_schema_name, facets_table_name
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = target_table_oid;

    RAISE NOTICE 'Rebuilding hierarchy for %.%', facets_schema_name, facets_table_name;

    -- Step 1: Clear existing children_bitmaps for this table to avoid duplicates if run multiple times
    EXECUTE format('UPDATE %s.%s SET children_bitmap = NULL WHERE facet_id IN (SELECT facet_id FROM facets.facet_definition WHERE table_id = %s)',
                   facets_schema_name, facets_table_name, target_table_oid);

    -- Step 2: Use a recursive CTE to find all descendant IDs for each parent facet value ID
    WITH RECURSIVE facet_hierarchy AS (
        -- Base case: Direct parent-child relationships between facet values
        SELECT
            pf_parent.id AS parent_value_id,
            pf_child.id AS child_value_id
        FROM
            facets.facet_definition fd_parent
        JOIN
            facets.facet_definition fd_child ON fd_parent.facet_name = fd_child.params->>'parent_facet'
                                             AND fd_parent.table_id = fd_child.table_id
        JOIN
            pg_catalog.pg_class tbl ON fd_parent.table_id = tbl.oid -- Join to get schema/table name
        JOIN
            pg_catalog.pg_namespace nsp ON tbl.relnamespace = nsp.oid
        JOIN
            LATERAL (SELECT id, facet_value FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid AND n.nspname=nsp.nspname AND c.relname = tbl.relname || '_facets') AS pf_parent_lookup ON pf_parent_lookup.facet_id = fd_parent.facet_id -- Dynamic table join
        JOIN
            LATERAL (SELECT id, facet_value FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid AND n.nspname=nsp.nspname AND c.relname = tbl.relname || '_facets') AS pf_child_lookup ON pf_child_lookup.facet_id = fd_child.facet_id -- Dynamic table join
        WHERE
            fd_parent.table_id = target_table_oid
            AND pf_parent_lookup.facet_value = pf_child_lookup.facet_value -- This join condition assumes the values match directly across levels? Needs verification based on how function facets work.
                                                                   -- A better join might involve the document bitmaps if parent/child values don't match textually.
                                                                   -- This part is highly dependent on how the facet values are related in the data.

        UNION ALL

        -- Recursive step: Find children of children
        SELECT
            fh.parent_value_id,
            pf_grandchild.id AS child_value_id
        FROM
            facet_hierarchy fh
        JOIN
            facets_table_name pf_child ON fh.child_value_id = pf_child.id -- NEED DYNAMIC TABLE HERE
        JOIN
            facets.facet_definition fd_child ON pf_child.facet_id = fd_child.facet_id
        JOIN
            facets.facet_definition fd_grandchild ON fd_child.facet_name = fd_grandchild.params->>'parent_facet'
                                                  AND fd_child.table_id = fd_grandchild.table_id
        JOIN
            facets_table_name pf_grandchild ON fd_grandchild.facet_id = pf_grandchild.facet_id -- NEED DYNAMIC TABLE HERE
                                          AND pf_child.facet_value = pf_grandchild.facet_value -- Same potentially problematic value join
        WHERE fd_child.table_id = target_table_oid

    ), aggregated_children AS (
        -- Step 3: Aggregate all descendant IDs for each parent ID into a Roaring Bitmap
        SELECT
            parent_value_id,
            rb_build_agg(child_value_id) AS calculated_bitmap
        FROM
            facet_hierarchy
        GROUP BY
            parent_value_id
    )
    -- Step 4: Update the main facets table
    EXECUTE format('UPDATE %1$s.%2$s pf SET children_bitmap = ac.calculated_bitmap FROM aggregated_children ac WHERE pf.id = ac.parent_value_id AND pf.children_bitmap IS DISTINCT FROM ac.calculated_bitmap',
                   facets_schema_name, facets_table_name);

END;
$$ LANGUAGE plpgsql;
-- SECTION 5: QUERY FUNCTIONS
-- Functions for querying facets and filtering data

-- Get top values for facets
CREATE OR REPLACE FUNCTION facets.top_values(
    p_table_id oid, 
    n int = 5, 
    facets text[] = null
) RETURNS TABLE(
    facet_name text, 
    facet_value text, 
    cardinality bigint, 
    facet_id int
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    facet_filter text = '';
BEGIN
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    
    -- Build filter if specific facets requested
    IF facets IS NOT NULL THEN
        -- Fix: Qualify facet_id with table alias to avoid ambiguity
        SELECT format('WHERE facet_id = ANY (''%s'')', array_agg(fd.facet_id)::text) INTO facet_filter
            FROM facets.facet_definition fd WHERE fd.facet_name = ANY (facets);
    END IF;

    -- Execute dynamic query with proper table alias
    RETURN QUERY EXECUTE format($sql$
        SELECT fd.facet_name, counts.facet_value, counts.sum, fd.facet_id FROM (
            SELECT facet_id, facet_value, sum, rank() OVER (PARTITION BY facet_id ORDER BY sum DESC) rank
            FROM (
                SELECT facet_id, facet_value, sum(rb_cardinality(postinglist))::bigint AS sum
                FROM %s
                %s
                GROUP BY 1, 2
                ) x
            ) counts JOIN facets.facet_definition fd USING (facet_id)
        WHERE rank <= $2 AND table_id = $1
        ORDER BY fd.facet_id, rank, facet_value;
    $sql$,
        facets._qualified(tdef.schemaname, tdef.facets_table),
        facet_filter)
    USING p_table_id, n;
END;
$$;

-- Overload to accept regclass parameter for convenience
CREATE OR REPLACE FUNCTION facets.top_values(
    p_table regclass,
    n int = 5,
    facets text[] = null
) RETURNS TABLE(
    facet_name text, 
    facet_value text, 
    cardinality bigint, 
    facet_id int
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY SELECT * FROM facets.top_values(p_table::oid, n, facets);
END;
$$;

-- Get facet counts
CREATE OR REPLACE FUNCTION facets.get_facet_counts(
    p_table_id oid,
    p_facet_name text,
    p_filter_bitmap roaringbitmap = NULL,
    p_limit int = 10
) RETURNS SETOF facets.facet_counts LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    v_facet_id int;
    v_facet_type text;
    sql text;
BEGIN
    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

    -- Get facet ID
    SELECT facet_id, facet_type INTO v_facet_id, v_facet_type
    FROM facets.facet_definition
    WHERE table_id = p_table_id AND facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Handle boolean facets specially
    IF v_facet_type = 'boolean' THEN
        RETURN QUERY
        SELECT 
            p_facet_name AS facet_name,
            facet_value,
            count AS cardinality,
            v_facet_id AS facet_id
        FROM (
            SELECT 
                'true' AS facet_value,
                true_count AS count
            FROM facets.get_filtered_boolean_facet_counts(p_table_id, p_facet_name, p_filter_bitmap)
            UNION ALL
            SELECT 
                'false' AS facet_value,
                false_count AS count
            FROM facets.get_filtered_boolean_facet_counts(p_table_id, p_facet_name, p_filter_bitmap)
        ) AS boolean_counts
        WHERE count > 0
        ORDER BY count DESC
        LIMIT p_limit;
        RETURN;
    END IF;

    -- For other facet types
    IF p_filter_bitmap IS NULL THEN
        -- No filter, just get counts
        -- GROUP BY facet_value to aggregate across chunks
        RETURN QUERY EXECUTE format($sql$
            SELECT
                %L AS facet_name,
                facet_value,
                SUM(rb_cardinality(postinglist))::bigint AS cardinality,
                %s AS facet_id
            FROM %s
            WHERE facet_id = %s
            GROUP BY facet_value
            ORDER BY cardinality DESC
            LIMIT %s
        $sql$,
            p_facet_name,
            v_facet_id,
            facets._qualified(tdef.schemaname, tdef.facets_table),
            v_facet_id,
            p_limit
        );
    ELSE
        -- With filter, calculate intersection
        -- IMPORTANT: The filter_bitmap contains FULL document IDs, but postinglists contain
        -- chunk-relative offsets. We need to convert the filter bitmap for each chunk.
        -- For each chunk_id, extract IDs that belong to that chunk and convert to offsets.
        -- chunk_id = document_id >> chunk_bits
        -- offset = document_id & ((1 << chunk_bits) - 1)
        RETURN QUERY EXECUTE format($sql$
            WITH filter_by_chunk AS (
                -- Convert full document IDs to (chunk_id, offset) pairs
                SELECT 
                    (doc_id >> %s)::int4 AS chunk_id,
                    rb_build_agg((doc_id & ((1 << %s) - 1))::int4) AS chunk_filter
                FROM unnest(rb_to_array($1)) AS doc_id
                GROUP BY (doc_id >> %s)::int4
            )
            SELECT
                %L AS facet_name,
                f.facet_value,
                SUM(rb_and_cardinality(f.postinglist, fbc.chunk_filter))::bigint AS cardinality,
                %s AS facet_id
            FROM %s f
            JOIN filter_by_chunk fbc ON f.chunk_id = fbc.chunk_id
            WHERE f.facet_id = %s
              AND rb_intersect(f.postinglist, fbc.chunk_filter)
            GROUP BY f.facet_value
            HAVING SUM(rb_and_cardinality(f.postinglist, fbc.chunk_filter)) > 0
            ORDER BY cardinality DESC
            LIMIT %s
        $sql$,
            tdef.chunk_bits, tdef.chunk_bits, tdef.chunk_bits,
            p_facet_name,
            v_facet_id,
            facets._qualified(tdef.schemaname, tdef.facets_table),
            v_facet_id,
            p_limit
        ) USING p_filter_bitmap;
    END IF;
END;
$$;

-- Get documents with a specific facet value
CREATE OR REPLACE FUNCTION facets.get_documents_with_facet(
    p_table_id oid,
    p_facet_name text,
    p_facet_value text
) RETURNS roaringbitmap LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    v_facet_id int;
    v_facet_type text;
    result roaringbitmap;
BEGIN
    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

    -- Get facet ID
    SELECT facet_id, facet_type INTO v_facet_id, v_facet_type
    FROM facets.facet_definition
    WHERE table_id = p_table_id AND facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Handle boolean facets specially
    IF v_facet_type = 'boolean' THEN
        RETURN facets.get_documents_with_boolean_facet(
            p_table_id, 
            p_facet_name, 
            CASE WHEN p_facet_value = 'true' THEN true ELSE false END
        );
    END IF;

    -- For other facet types
    -- IMPORTANT: Reconstruct original IDs from (chunk_id << chunk_bits) | in_chunk_id
    EXECUTE format($sql$
        SELECT rb_build_agg(((f.chunk_id::bigint << $3) | pl.in_chunk_id)::int4)
        FROM %s f
        CROSS JOIN LATERAL unnest(rb_to_array(f.postinglist)) AS pl(in_chunk_id)
        WHERE f.facet_id = $1
        AND f.facet_value = $2
    $sql$,
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO result USING v_facet_id, p_facet_value, tdef.chunk_bits;

    RETURN COALESCE(result, rb_build('{}'::int[]));
END;
$$;

-- Get documents with boolean facet value
CREATE OR REPLACE FUNCTION facets.get_documents_with_boolean_facet(
    p_table_id oid,
    p_facet_name text,
    p_is_true boolean
) RETURNS roaringbitmap
    LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_facet_id int;
    v_facet_value text;
    result roaringbitmap;
    tdef facets.faceted_table;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Get facet ID
    SELECT facet_id INTO v_facet_id 
    FROM facets.facet_definition 
    WHERE table_id = p_table_id AND facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Convert boolean to text value
    v_facet_value := CASE WHEN p_is_true THEN 'true' ELSE 'false' END;
    
    -- Get postinglist for this facet value
    -- IMPORTANT: Reconstruct original IDs from (chunk_id << chunk_bits) | in_chunk_id
    EXECUTE format('
        SELECT rb_build_agg(((f.chunk_id::bigint << $3) | pl.in_chunk_id)::int4)
        FROM %s f
        CROSS JOIN LATERAL unnest(rb_to_array(f.postinglist)) AS pl(in_chunk_id)
        WHERE f.facet_id = $1
        AND f.facet_value = $2',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO result USING v_facet_id, v_facet_value, tdef.chunk_bits;
    
    RETURN COALESCE(result, rb_build('{}'::int[]));
END;
$$;

-- Get boolean facet counts
CREATE OR REPLACE FUNCTION facets.get_boolean_facet_counts(
    p_table_id oid,
    p_facet_name text
) RETURNS TABLE(facet_id int, true_count bigint, false_count bigint, total_count bigint)
    LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_facet_id int;
    v_true_count bigint;
    v_false_count bigint;
    tdef facets.faceted_table;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Get facet ID
    SELECT fd.facet_id INTO v_facet_id 
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id AND fd.facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Get true count
    EXECUTE format('
        SELECT COALESCE(rb_cardinality(rb_or_agg(postinglist)), 0)::bigint
        FROM %s
        WHERE facet_id = $1
        AND facet_value = ''true''',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO v_true_count USING v_facet_id;
    
    -- Get false count
    EXECUTE format('
        SELECT COALESCE(rb_cardinality(rb_or_agg(postinglist)), 0)::bigint
        FROM %s
        WHERE facet_id = $1
        AND facet_value = ''false''',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO v_false_count USING v_facet_id;
    
    RETURN QUERY
    SELECT 
        v_facet_id,
        v_true_count,
        v_false_count,
        (v_true_count + v_false_count)::bigint;
END;
$$;

-- Get filtered boolean facet counts
CREATE OR REPLACE FUNCTION facets.get_filtered_boolean_facet_counts(
    p_table_id oid,
    p_facet_name text,
    p_filter_bitmap roaringbitmap
) RETURNS TABLE(facet_id int, true_count bigint, false_count bigint, total_count bigint)
    LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_facet_id int;
    v_true_count bigint;
    v_false_count bigint;
    tdef facets.faceted_table;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Get facet ID
    SELECT fd.facet_id INTO v_facet_id 
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id AND fd.facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Get true count with filter
    EXECUTE format('
        SELECT COALESCE(SUM(rb_and_cardinality(postinglist, $1)), 0)::bigint
        FROM %s
        WHERE facet_id = $2
        AND facet_value = ''true''',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO v_true_count USING p_filter_bitmap, v_facet_id;
    
    -- Get false count with filter
    EXECUTE format('
        SELECT COALESCE(SUM(rb_and_cardinality(postinglist, $1)), 0)::bigint
        FROM %s
        WHERE facet_id = $2
        AND facet_value = ''false''',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO v_false_count USING p_filter_bitmap, v_facet_id;
    
    RETURN QUERY
    SELECT 
        v_facet_id,
        v_true_count,
        v_false_count,
        (v_true_count + v_false_count)::bigint;
END;
$$;

-- Filter with boolean facets
CREATE OR REPLACE FUNCTION facets.filter_with_boolean_facets(
    p_table_id oid,
    p_filters jsonb -- Format: [{"facet_name": "is_active", "value": true}, {"facet_name": "category", "value": "electronics"}]
) RETURNS roaringbitmap
    LANGUAGE plpgsql STABLE AS $$
DECLARE
    result roaringbitmap;
    filter_item jsonb;
    v_facet_id int;
    v_facet_name text;
    v_facet_value text;
    v_facet_type text;
    v_is_boolean boolean;
    facet_bitmap roaringbitmap;
    tdef facets.faceted_table;
    i int;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Process each filter
    FOR i IN 0..jsonb_array_length(p_filters) - 1 LOOP
        filter_item := p_filters->i;
        v_facet_name := filter_item->>'facet_name';
        
        -- Get facet information
        SELECT facet_id, facet_type INTO v_facet_id, v_facet_type
        FROM facets.facet_definition
        WHERE table_id = p_table_id AND facet_name = v_facet_name;
        
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Facet % not found for table %', v_facet_name, p_table_id;
        END IF;
        
        -- Check if this is a boolean facet
        v_is_boolean := (v_facet_type = 'boolean');
        
        -- Process based on facet type
        IF v_is_boolean THEN
            -- Boolean facet
            v_facet_value := CASE 
                WHEN (filter_item->>'value')::boolean THEN 'true' 
                ELSE 'false' 
            END;
        ELSE
            -- Regular facet
            v_facet_value := filter_item->>'value';
        END IF;
        
        -- Get bitmap for this facet value
        -- IMPORTANT: Reconstruct original IDs from (chunk_id << chunk_bits) | in_chunk_id
        EXECUTE format('
            SELECT rb_build_agg(((f.chunk_id::bigint << $3) | pl.in_chunk_id)::int4)
            FROM %s f
            CROSS JOIN LATERAL unnest(rb_to_array(f.postinglist)) AS pl(in_chunk_id)
            WHERE f.facet_id = $1
            AND f.facet_value = $2',
            facets._qualified(tdef.schemaname, tdef.facets_table)
        ) INTO facet_bitmap USING v_facet_id, v_facet_value, tdef.chunk_bits;
        
        -- Combine with result (AND operation)
        IF result IS NULL THEN
            result := facet_bitmap;
        ELSE
            result := rb_and(result, facet_bitmap);
        END IF;
        
        -- Early termination if result is empty
        IF result IS NULL OR rb_cardinality(result) = 0 THEN
            RETURN rb_build('{}'::int[]);
        END IF;
    END LOOP;
    
    RETURN COALESCE(result, rb_build('{}'::int[]));
END;
$$;

-- SECTION 6: CARDINALITY-BASED FILTERING OPTIMIZATION
-- Functions for optimizing filtering based on facet cardinality

-- Function to calculate facet cardinality statistics
CREATE OR REPLACE FUNCTION facets.calculate_facet_cardinality_stats(p_table_id oid) 
RETURNS void AS $$
DECLARE
    v_facet_id int4;
    v_facet_value text;
    v_cardinality bigint;
    tdef facets.faceted_table;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Create stats table if it doesn't exist
    EXECUTE format($sql$
        CREATE TABLE IF NOT EXISTS %I.%I_cardinality_stats (
            facet_id int4 NOT NULL,
            facet_value text COLLATE "C" NOT NULL,
            cardinality bigint NOT NULL,
            last_updated timestamp NOT NULL DEFAULT now(),
            PRIMARY KEY (facet_id, facet_value)
        )
    $sql$, tdef.schemaname, tdef.facets_table);
    
    -- Clear existing stats for this table
    EXECUTE format($sql$
        TRUNCATE %I.%I_cardinality_stats
    $sql$, tdef.schemaname, tdef.facets_table);
    
    -- Calculate cardinality for each facet value
    EXECUTE format($sql$
        INSERT INTO %I.%I_cardinality_stats (facet_id, facet_value, cardinality)
        SELECT 
            f.facet_id,
            f.facet_value,
            rb_cardinality(rb_or_agg(f.postinglist))
        FROM %I.%I f
        GROUP BY f.facet_id, f.facet_value
    $sql$, tdef.schemaname, tdef.facets_table, tdef.schemaname, tdef.facets_table);
END;
$$ LANGUAGE plpgsql;

-- Function to get facet cardinality from stats
CREATE OR REPLACE FUNCTION facets.get_facet_cardinality_from_stats(
    p_table_id oid,
    p_facet_id int4,
    p_facet_value text DEFAULT NULL
) RETURNS bigint AS $$
DECLARE
    v_cardinality bigint;
    tdef facets.faceted_table;
    stats_table_exists boolean; -- Added boolean variable
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Check if stats table exists
    EXECUTE format($sql$
        SELECT EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = %L AND tablename = %L
        )
    $sql$, tdef.schemaname, tdef.facets_table || '_cardinality_stats') INTO stats_table_exists; -- Store into boolean variable
    
    IF NOT stats_table_exists THEN -- Check the boolean variable
        -- Stats table doesn't exist, create it
        PERFORM facets.calculate_facet_cardinality_stats(p_table_id);
    END IF;
    
    IF p_facet_value IS NULL THEN
        -- Get total cardinality for facet
        EXECUTE format($sql$
            SELECT SUM(cardinality)::bigint
            FROM %I.%I_cardinality_stats
            WHERE facet_id = $1
        $sql$, tdef.schemaname, tdef.facets_table) INTO v_cardinality USING p_facet_id;
    ELSE
        -- Get cardinality for specific facet value
        EXECUTE format($sql$
            SELECT cardinality
            FROM %I.%I_cardinality_stats
            WHERE facet_id = $1
            AND facet_value = $2
        $sql$, tdef.schemaname, tdef.facets_table) INTO v_cardinality USING p_facet_id, p_facet_value;
    END IF;
    
    RETURN COALESCE(v_cardinality, 0);
END;
$$ LANGUAGE plpgsql;

-- Function to filter by facets with cardinality optimization
-- v0.3.4: filter_by_facets_with_cardinality_optimization without JSONB casts,
-- processes facets in ascending cardinality using a typed temp table.
CREATE OR REPLACE FUNCTION filter_by_facets_with_cardinality_optimization(
  p_filters jsonb,                           -- [{"facet_id":..., "facet_value":...}, ...]
  p_stats_schema text,                       -- e.g. 'public'
  p_stats_table  text                        -- e.g. 'facet_cardinality_stats'
) RETURNS roaringbitmap
LANGUAGE plpgsql
AS $$
DECLARE
  v_result           roaringbitmap;
  v_facet_id         int;
  v_facet_value      text;
  v_cardinality      bigint;
  v_bitmap           roaringbitmap;
  v_stats_exists     boolean;
  v_stats_qualified  text;
BEGIN
  -- Ensure stats table exists; if not, fall back to dynamic cardinality
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = p_stats_schema
      AND table_name   = p_stats_table
  ) INTO v_stats_exists;

  v_stats_qualified := format('%I.%I', p_stats_schema, p_stats_table);

  -- Temp table keeps native roaringbitmap values.
  CREATE TEMP TABLE tmp_filters_034 (
    facet_id     int,
    facet_value  text,
    bitmap       roaringbitmap,
    cardinality  bigint
  ) ON COMMIT DROP;

  -- Unnest JSON once, build per (facet_id, facet_value) bitmaps, get selectivity
  -- NOTE: replace <facets_table> with your qualified facets storage table.
  WITH filter_items AS (
    SELECT
      (f->>'facet_id')::int  AS facet_id,
      f->>'facet_value'      AS facet_value
    FROM jsonb_array_elements(p_filters) AS f
  ),
  built AS (
    SELECT
      fi.facet_id,
      fi.facet_value,
      -- union across chunks to get the global bitmap for this value
      rb_or_agg(ft.postinglist) AS postinglist
    FROM filter_items fi
    JOIN <facets_table> ft
      ON ft.facet_id   = fi.facet_id
     AND ft.facet_value= fi.facet_value
    GROUP BY 1,2
  )
  INSERT INTO tmp_filters_034 (facet_id, facet_value, bitmap, cardinality)
  SELECT
    b.facet_id,
    b.facet_value,
    b.postinglist,
    CASE
      WHEN v_stats_exists THEN
        COALESCE((
          SELECT s.cardinality
          FROM   %s s
          WHERE  s.facet_id = b.facet_id
          AND    s.facet_value = b.facet_value
          LIMIT  1
        ), rb_cardinality(b.postinglist))
      ELSE
        rb_cardinality(b.postinglist)
    END AS cardinality
  FROM built b;

  -- AND bitmaps in ascending selectivity; short-circuit on empty.
  FOR v_bitmap IN
    SELECT bitmap FROM tmp_filters_034 ORDER BY cardinality ASC
  LOOP
    IF v_result IS NULL THEN
      v_result := v_bitmap;
    ELSE
      v_result := rb_and(v_result, v_bitmap);
    END IF;

    IF v_result IS NULL OR rb_cardinality(v_result) = 0 THEN
      RETURN rb_build('{}'::int[]);
    END IF;
  END LOOP;

  RETURN COALESCE(v_result, rb_build('{}'::int[]));
END;
$$;


-- SECTION 7: VECTOR SEARCH INTEGRATION
-- Functions for integrating with pg_vector

-- Function to get facet counts with vector search
CREATE OR REPLACE FUNCTION facets.get_facet_counts_with_vector(
    p_table_id oid,
    p_facet_name text,
    p_vector_column text,
    p_query_vector vector,
    p_limit int = 10,
    p_filter_bitmap roaringbitmap = NULL
) RETURNS SETOF facets.facet_counts LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    v_facet_id int;
    v_facet_type text;
    v_vector_bitmap roaringbitmap;
    sql text;
BEGIN
    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;

    -- Get facet ID
    SELECT facet_id, facet_type INTO v_facet_id, v_facet_type
    FROM facets.facet_definition
    WHERE table_id = p_table_id AND facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Get vector search results as bitmap
    EXECUTE format($sql$
        WITH vector_matches AS (
            SELECT %I
            FROM %s
            ORDER BY %I <-> $1
            LIMIT 1000
        )
        SELECT rb_build(ARRAY(SELECT %I FROM vector_matches))
    $sql$,
        tdef.key,
        p_table_id::regclass::text,
        p_vector_column,
        tdef.key
    ) INTO v_vector_bitmap USING p_query_vector;
    
    -- Combine with filter bitmap if provided
    IF p_filter_bitmap IS NOT NULL THEN
        v_vector_bitmap := rb_and(v_vector_bitmap, p_filter_bitmap);
    END IF;
    
    -- Handle boolean facets specially
    IF v_facet_type = 'boolean' THEN
        RETURN QUERY
        SELECT 
            p_facet_name AS facet_name,
            facet_value,
            count AS cardinality,
            v_facet_id AS facet_id
        FROM (
            SELECT 
                'true' AS facet_value,
                true_count AS count
            FROM facets.get_filtered_boolean_facet_counts(p_table_id, p_facet_name, v_vector_bitmap)
            UNION ALL
            SELECT 
                'false' AS facet_value,
                false_count AS count
            FROM facets.get_filtered_boolean_facet_counts(p_table_id, p_facet_name, v_vector_bitmap)
        ) AS boolean_counts
        WHERE count > 0
        ORDER BY count DESC
        LIMIT p_limit;
        RETURN;
    END IF;

    -- For other facet types
    -- GROUP BY facet_value to aggregate across chunks
    RETURN QUERY EXECUTE format($sql$
        SELECT
            %L AS facet_name,
            facet_value,
            SUM(rb_and_cardinality(postinglist, $1))::bigint AS cardinality,
            %s AS facet_id
        FROM %s
        WHERE facet_id = %s
          AND rb_intersect(postinglist, $1)
        GROUP BY facet_value
        ORDER BY cardinality DESC
        LIMIT %s
    $sql$,
        p_facet_name,
        v_facet_id,
        facets._qualified(tdef.schemaname, tdef.facets_table),
        v_facet_id,
        p_limit
    ) USING v_vector_bitmap;
END;
$$;

-- Function to filter with facets and vector search
CREATE OR REPLACE FUNCTION facets.filter_with_facets_and_vector(
    p_table_id oid,
    p_filters jsonb,
    p_vector_column text,
    p_query_vector vector,
    p_vector_limit int = 1000
) RETURNS roaringbitmap LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    v_facet_bitmap roaringbitmap;
    v_vector_bitmap roaringbitmap;
    result roaringbitmap;
BEGIN
    -- Get table info
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    
    -- Get facet filter bitmap with cardinality optimization
    v_facet_bitmap := facets.filter_by_facets_with_cardinality_optimization(p_table_id, p_filters);
    
    -- Get vector search results as bitmap
    EXECUTE format($sql$
        WITH vector_matches AS (
            SELECT %I
            FROM %s
            ORDER BY %I <-> $1
            LIMIT $2
        )
        SELECT rb_build(ARRAY(SELECT %I FROM vector_matches))
    $sql$,
        tdef.key,
        p_table_id::regclass::text,
        p_vector_column,
        tdef.key
    ) INTO v_vector_bitmap USING p_query_vector, p_vector_limit;
    
    -- Combine facet and vector bitmaps
    result := rb_and(v_facet_bitmap, v_vector_bitmap);
    
    RETURN COALESCE(result, rb_build('{}'::int[]));
END;
$$;



-- Helper function to get facet hierarchies
CREATE OR REPLACE FUNCTION facets._get_facet_hierarchies(p_table_id oid)
RETURNS TABLE(
    hierarchical_facets text[],
    regular_facets text[],
    main_hierarchy_root text[]
) LANGUAGE plpgsql STABLE AS $$
DECLARE
    excluded_facets text[];
    included_facets text[];
    main_hierarchy_root text[];
BEGIN
    -- Directly identify hierarchical facets by examining parent-child relationships
    WITH hierarchical_relationships AS (
        -- Get all facets with parent-child relationships
        SELECT
            fd.facet_name as child_facet,
            (fd.params->>'parent_facet') as parent_facet
        FROM facets.facet_definition fd
        WHERE fd.table_id = p_table_id
          AND fd.params->>'parent_facet' IS NOT NULL
    ),
    all_hierarchical_facets AS (
        -- Combine both parents and children
        SELECT child_facet as facet_name FROM hierarchical_relationships
        UNION
        SELECT parent_facet as facet_name FROM hierarchical_relationships
    )
    SELECT ARRAY_AGG(facet_name) INTO excluded_facets
    FROM all_hierarchical_facets;
    
    IF excluded_facets IS NULL THEN
        excluded_facets := '{}'::text[];
    END IF;
    
    -- Get all facets that are not part of hierarchies
    SELECT 
        ARRAY_AGG(facet_name) INTO included_facets
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id
      AND fd.facet_name <> ALL(excluded_facets);
    
    -- Find root facets (those that are parents but not children)
    WITH hierarchical_relationships AS (
        SELECT
            fd.facet_name as child_facet,
            (fd.params->>'parent_facet') as parent_facet
        FROM facets.facet_definition fd
        WHERE fd.table_id = p_table_id
          AND fd.params->>'parent_facet' IS NOT NULL
    ),
    root_facets AS (
        SELECT DISTINCT parent_facet
        FROM hierarchical_relationships
        WHERE parent_facet NOT IN (SELECT child_facet FROM hierarchical_relationships)
    )
    SELECT ARRAY_AGG(parent_facet) INTO main_hierarchy_root
    FROM root_facets;
    
    RETURN QUERY SELECT excluded_facets, included_facets, main_hierarchy_root;
END;
$$;

-- Helper function to build filter bitmap
CREATE OR REPLACE FUNCTION facets._build_filter_bitmap(
    p_table_id oid, 
    tdef facets.faceted_table, 
    facet_ids int[]
) RETURNS roaringbitmap LANGUAGE plpgsql STABLE AS $$
DECLARE
    filter_bitmap roaringbitmap;
    query_text text;
BEGIN
    IF facet_ids IS NULL OR array_length(facet_ids, 1) = 0 THEN
        RETURN NULL;
    END IF;
    
    -- Build bitmap directly from facet IDs for efficiency
    query_text := format(
        'SELECT rb_and_agg(postinglist)
         FROM %I.%I
         WHERE id = ANY($1)',
        tdef.schemaname, tdef.facets_table
    );
    
    EXECUTE query_text INTO filter_bitmap USING facet_ids;
    
    -- Return NULL if the bitmap is empty
    IF filter_bitmap IS NULL OR rb_is_empty(filter_bitmap) THEN
        RETURN NULL;
    END IF;
    
    RETURN filter_bitmap;
END;
$$;

-- Helper function to get hierarchical facets
CREATE OR REPLACE FUNCTION facets._get_hierarchical_facets(
    p_table_id oid,
    tdef facets.faceted_table,
    main_hierarchy_root text,
    filter_bitmap roaringbitmap,
    selected_facets jsonb,
    n integer
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    hierarchical_result jsonb;
    root_facet_id int;
    level0_name text;
    level1_name text;
    level2_name text;
    query_text text;
    facet_id_array text := '{}';
BEGIN
    -- Get hierarchical facets using children_bitmap for efficient traversal
    IF main_hierarchy_root IS NULL THEN
        RETURN '[]'::jsonb;
    END IF;
    
    -- Get the facet names for each level in the hierarchy
    -- Level 0 (root level)
    SELECT facet_name INTO level0_name
    FROM facets.facet_definition
    WHERE table_id = p_table_id AND facet_name = main_hierarchy_root;
    
    -- Level 1 (children of root)
    SELECT fd.facet_name INTO level1_name
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id 
      AND fd.params->>'parent_facet' = main_hierarchy_root
    LIMIT 1;
    
    -- Level 2 (grandchildren)
    SELECT fd.facet_name INTO level2_name
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id 
      AND fd.params->>'parent_facet' = level1_name
    LIMIT 1;
    
    -- Fall back to defaults if names not found
    level0_name := COALESCE(level0_name, 'main_category');
    level1_name := COALESCE(level1_name, 'category');
    level2_name := COALESCE(level2_name, 'subcategory');
    
    -- Get the facet_id for the root level
    SELECT facet_id INTO root_facet_id
    FROM facets.facet_definition
    WHERE table_id = p_table_id AND facet_name = main_hierarchy_root;
    
    IF root_facet_id IS NULL THEN
        RETURN '[]'::jsonb;
    END IF;

    -- Split the recursive part for compatibility with different PostgreSQL versions
    IF filter_bitmap IS NULL THEN
        -- No filter case - use a simpler approach without recursion
        -- Get root level
        query_text := format($sql$
WITH root_level AS (
    SELECT 
        id, facet_id, facet_value, 
        rb_cardinality(postinglist) AS doc_count,
        facet_value AS path,
        ARRAY[id] AS id_path
    FROM 
        %I.%I
    WHERE 
        facet_id = %s
    ORDER BY 
        doc_count DESC
    LIMIT %s
),
level1 AS (
    SELECT 
        c.id, c.facet_id, c.facet_value, 
        rb_cardinality(c.postinglist) AS doc_count,
        r.path || ' > ' || c.facet_value AS path,
        r.id_path || c.id AS id_path
    FROM 
        root_level r
    JOIN 
        %I.%I c ON rb_contains(
            (SELECT children_bitmap FROM %I.%I WHERE id = r.id), 
            c.id
        )
),
level2 AS (
    SELECT 
        c.id, c.facet_id, c.facet_value, 
        rb_cardinality(c.postinglist) AS doc_count,
        l.path || ' > ' || c.facet_value AS path,
        l.id_path || c.id AS id_path
    FROM 
        level1 l
    JOIN 
        %I.%I c ON rb_contains(
            (SELECT children_bitmap FROM %I.%I WHERE id = l.id), 
            c.id
        )
),
level0_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', id,
            'facet_id', facet_id,
            'value', facet_value,
            'max_count', doc_count::int,
            'count', doc_count::int,
            'path', path,
            'id_path', id_path
        )
        ORDER BY doc_count DESC
    ) AS level_items
    FROM root_level
),
level1_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', id,
            'facet_id', facet_id,
            'value', facet_value,
            'max_count', doc_count::int,
            'count', doc_count::int,
            'path', path,
            'id_path', id_path
        )
        ORDER BY doc_count DESC
    ) AS level_items
    FROM level1
),
level2_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', id,
            'facet_id', facet_id,
            'value', facet_value,
            'max_count', doc_count::int,
            'count', doc_count::int,
            'path', path,
            'id_path', id_path
        )
        ORDER BY doc_count DESC
    ) AS level_items
    FROM level2
)
SELECT
    jsonb_build_object(
        'hierarchy_type', 'category',
        'facet_id', %s,
        'levels', jsonb_build_array(
            jsonb_build_object('name', %L, 'values', COALESCE((SELECT level_items FROM level0_json), '[]'::jsonb)),
            jsonb_build_object('name', %L, 'values', COALESCE((SELECT level_items FROM level1_json), '[]'::jsonb)),
            jsonb_build_object('name', %L, 'values', COALESCE((SELECT level_items FROM level2_json), '[]'::jsonb))
        )
    ) AS hierarchy
$sql$,
            tdef.schemaname, tdef.facets_table, 
            root_facet_id, n,
            tdef.schemaname, tdef.facets_table, tdef.schemaname, tdef.facets_table,
            tdef.schemaname, tdef.facets_table, tdef.schemaname, tdef.facets_table,
            root_facet_id, level0_name, level1_name, level2_name
        );
    ELSE
        -- With filter - Use a completely flat structure without JSONB parsing to avoid type issues
        query_text := format($sql$
WITH active_docs AS (
    -- Get the document IDs that match our filter bitmap
    SELECT rb_cardinality($1) AS total_docs
),
root_level AS (
    SELECT 
        f.id, 
        f.facet_id, 
        f.facet_value, 
        rb_cardinality(f.postinglist) AS max_count,
        rb_cardinality(rb_and(f.postinglist, $1)) AS filtered_count,
        f.facet_value AS path,
        ARRAY[f.id] AS id_path,
        false AS is_selected
    FROM 
        %I.%I f
    WHERE 
        f.facet_id = %s
    ORDER BY 
        filtered_count DESC
    LIMIT %s
),
level1 AS (
    SELECT 
        c.id, 
        c.facet_id, 
        c.facet_value, 
        rb_cardinality(c.postinglist) AS max_count,
        rb_cardinality(rb_and(c.postinglist, $1)) AS filtered_count,
        r.path || ' > ' || c.facet_value AS path,
        r.id_path || c.id AS id_path,
        false AS is_selected
    FROM 
        root_level r
    JOIN 
        %I.%I c ON rb_contains(
            (SELECT children_bitmap FROM %I.%I WHERE id = r.id), 
            c.id
        )
),
level2 AS (
    SELECT 
        c.id, 
        c.facet_id, 
        c.facet_value, 
        rb_cardinality(c.postinglist) AS max_count,
        rb_cardinality(rb_and(c.postinglist, $1)) AS filtered_count,
        l.path || ' > ' || c.facet_value AS path,
        l.id_path || c.id AS id_path,
        false AS is_selected
    FROM 
        level1 l
    JOIN 
        %I.%I c ON rb_contains(
            (SELECT children_bitmap FROM %I.%I WHERE id = l.id), 
            c.id
        )
),
level0_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', id,
            'facet_id', facet_id,
            'value', facet_value,
            'count', filtered_count::int,
            'max_count', max_count::int,
            'path', path,
            'id_path', id_path,
            'is_selected', is_selected
        )
        ORDER BY filtered_count DESC
    ) AS level_items
    FROM root_level
    WHERE filtered_count > 0 OR is_selected
),
level1_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', id,
            'facet_id', facet_id,
            'value', facet_value,
            'count', filtered_count::int,
            'max_count', max_count::int,
            'path', path,
            'id_path', id_path,
            'is_selected', is_selected
        )
        ORDER BY filtered_count DESC
    ) AS level_items
    FROM level1
    WHERE filtered_count > 0 OR is_selected
),
level2_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'id', id,
            'facet_id', facet_id,
            'value', facet_value,
            'count', filtered_count::int,
            'max_count', max_count::int,
            'path', path,
            'id_path', id_path,
            'is_selected', is_selected
        )
        ORDER BY filtered_count DESC
    ) AS level_items
    FROM level2
    WHERE filtered_count > 0 OR is_selected
)
SELECT
    jsonb_build_object(
        'hierarchy_type', 'category',
        'facet_id', %s,
        'total_docs', (SELECT total_docs FROM active_docs),
        'levels', jsonb_build_array(
            jsonb_build_object('name', %L, 'values', COALESCE((SELECT level_items FROM level0_json), '[]'::jsonb)),
            jsonb_build_object('name', %L, 'values', COALESCE((SELECT level_items FROM level1_json), '[]'::jsonb)),
            jsonb_build_object('name', %L, 'values', COALESCE((SELECT level_items FROM level2_json), '[]'::jsonb))
        )
    ) AS hierarchy
$sql$,
            tdef.schemaname, tdef.facets_table, 
            root_facet_id, n,
            tdef.schemaname, tdef.facets_table, tdef.schemaname, tdef.facets_table,
            tdef.schemaname, tdef.facets_table, tdef.schemaname, tdef.facets_table,
            root_facet_id, level0_name, level1_name, level2_name
        );
        
        -- Execute the query with only the filter bitmap parameter
        EXECUTE query_text INTO hierarchical_result USING filter_bitmap;
        
        -- If no hierarchical facets found, initialize as empty array
        IF hierarchical_result IS NULL THEN
            hierarchical_result := '[]'::jsonb;
        END IF;
        
        RETURN hierarchical_result;
    END IF;
    
    -- For non-filtered case
    EXECUTE query_text INTO hierarchical_result;
    
    -- If no hierarchical facets found, initialize as empty array
    IF hierarchical_result IS NULL THEN
        hierarchical_result := '[]'::jsonb;
    END IF;
    
    RETURN hierarchical_result;
END;
$$;

-- Helper function to get facet counts from a bitmap
CREATE OR REPLACE FUNCTION facets.get_facet_counts_by_bitmap(
    p_table_id oid,
    p_filter_bitmap roaringbitmap,
    p_facet_limit integer DEFAULT 5
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    hierarchies record;
    root_facets text[];
    root_facet text;
    hierarchical_results jsonb[];
    hierarchical_result jsonb;
    combined_hierarchical_result jsonb;
    regular_facets_result jsonb;
    combined_result jsonb;
BEGIN
    -- Get table definition
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    
    -- Get hierarchical information
    SELECT h.hierarchical_facets, h.regular_facets, h.main_hierarchy_root 
    INTO hierarchies 
    FROM facets._get_facet_hierarchies(p_table_id) h;
    
    -- Store the array of root facets
    root_facets := hierarchies.main_hierarchy_root;
    
    -- Initialize array for hierarchical results
    hierarchical_results := ARRAY[]::jsonb[];
   
    -- Get hierarchical facets for each root
    IF root_facets IS NOT NULL AND array_length(root_facets, 1) > 0 THEN
        FOREACH root_facet IN ARRAY root_facets
        LOOP
            -- Get hierarchical facets for this root
            hierarchical_result := facets._get_hierarchical_facets(
                p_table_id, 
                tdef, 
                root_facet, 
                p_filter_bitmap, 
                NULL, 
                p_facet_limit
            );
            
            -- Add to results array
            hierarchical_results := hierarchical_results || hierarchical_result;
        END LOOP;
    END IF;
    
    -- Combine all hierarchical results into a single array
    IF array_length(hierarchical_results, 1) > 0 THEN
        combined_hierarchical_result := jsonb_build_array();
        FOR i IN 1..array_length(hierarchical_results, 1) LOOP
            combined_hierarchical_result := combined_hierarchical_result || hierarchical_results[i];
        END LOOP;
    ELSE
        combined_hierarchical_result := '[]'::jsonb;
    END IF;
    
    -- Compute regular (non-hierarchical) facets
    regular_facets_result := '[]'::jsonb;
    IF hierarchies.regular_facets IS NOT NULL AND array_length(hierarchies.regular_facets, 1) > 0 THEN
        regular_facets_result := jsonb_build_array();
        
        FOREACH root_facet IN ARRAY hierarchies.regular_facets
        LOOP
            DECLARE
                facet_counts jsonb;
                facet_item jsonb;
                facet_rec record;
            BEGIN
                facet_counts := '[]'::jsonb;
                
                FOR facet_rec IN 
                    SELECT * FROM facets.get_facet_counts(
                        p_table_id,
                        root_facet,
                        p_filter_bitmap,
                        p_facet_limit
                    )
                LOOP
                    facet_item := jsonb_build_object(
                        'facet_name', facet_rec.facet_name,
                        'facet_id', facet_rec.facet_id,
                        'value', facet_rec.facet_value,
                        'count', facet_rec.cardinality
                    );
                    facet_counts := facet_counts || jsonb_build_array(facet_item);
                END LOOP;
                
                IF jsonb_array_length(facet_counts) > 0 THEN
                    regular_facets_result := regular_facets_result || jsonb_build_array(
                        jsonb_build_object(
                            'facet_name', root_facet,
                            'values', facet_counts
                        )
                    );
                END IF;
            END;
        END LOOP;
    END IF;
    
    -- Combine everything
    IF jsonb_array_length(combined_hierarchical_result) > 0 AND jsonb_array_length(regular_facets_result) > 0 THEN
        combined_result := combined_hierarchical_result || regular_facets_result;
    ELSIF jsonb_array_length(combined_hierarchical_result) > 0 THEN
        combined_result := combined_hierarchical_result;
    ELSIF jsonb_array_length(regular_facets_result) > 0 THEN
        combined_result := regular_facets_result;
    ELSE
        combined_result := '[]'::jsonb;
    END IF;
    
    RETURN combined_result;
END;
$$;

-- Main hierarchical_facets function
CREATE OR REPLACE FUNCTION facets.hierarchical_facets(
    p_table_id oid,
    n integer DEFAULT 5,
    facet_ids integer[] DEFAULT NULL::integer[],
    document_ids integer[] DEFAULT NULL::integer[]
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    hierarchies record;
    filter_bitmap roaringbitmap;
    document_bitmap roaringbitmap;
    regular_facets_result jsonb;
    hierarchical_results jsonb[];
    combined_hierarchical_result jsonb;
    nested_hierarchical_result jsonb;
    combined_result jsonb;
    root_facets text[];
    root_facet text;
    hierarchical_result jsonb;
    active_document_count int;
    i int;
    j int;
    k int;
    facet jsonb;
    item jsonb;
    item_array jsonb;
    child jsonb;
    child_array jsonb;
    empty_jsonb jsonb := '[]'::jsonb;
BEGIN
    -- Get table definition
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    
    -- Get hierarchical information
    SELECT h.hierarchical_facets, h.regular_facets, h.main_hierarchy_root 
    INTO hierarchies 
    FROM facets._get_facet_hierarchies(p_table_id) h;
    
    -- Store the array of root facets
    root_facets := hierarchies.main_hierarchy_root;
    
    -- Create filter bitmap from facet IDs if provided
    IF facet_ids IS NOT NULL AND array_length(facet_ids, 1) > 0 THEN
        -- Use the bitmap from facet IDs for filtering
        EXECUTE format(
            'SELECT rb_and_agg(postinglist)
             FROM %I.%I
             WHERE id = ANY($1)',
            tdef.schemaname, tdef.facets_table
        ) INTO filter_bitmap USING facet_ids;
    END IF;
    
    -- Create document bitmap from document IDs if provided
    IF document_ids IS NOT NULL AND array_length(document_ids, 1) > 0 THEN
        -- Convert document IDs array to bitmap
        document_bitmap := rb_build(document_ids);
        
        -- Get actual count of documents for reference
        active_document_count := array_length(document_ids, 1);
        
        -- Use document_bitmap as filter_bitmap if we don't have a filter bitmap
        IF filter_bitmap IS NULL THEN
            filter_bitmap := document_bitmap;
        END IF;
    ELSIF filter_bitmap IS NOT NULL THEN
        -- If we have a filter but no explicit document_ids, use the filter bitmap
        document_bitmap := filter_bitmap;
        
        -- Count documents in the filter bitmap
        SELECT rb_cardinality(filter_bitmap) INTO active_document_count;
    ELSE
        -- No filtering case
        document_bitmap := NULL;
        active_document_count := NULL;
    END IF;
    
    -- Initialize array for hierarchical results
    hierarchical_results := ARRAY[]::jsonb[];
   
    -- Get hierarchical facets for each root
    IF root_facets IS NOT NULL AND array_length(root_facets, 1) > 0 THEN
        FOREACH root_facet IN ARRAY root_facets
        LOOP
            -- Get hierarchical facets for this root
            hierarchical_result := facets._get_hierarchical_facets(
                p_table_id, 
                tdef, 
                root_facet, 
                filter_bitmap, 
                NULL, 
                n
            );
            
            -- Add to results array
            hierarchical_results := hierarchical_results || hierarchical_result;
        END LOOP;
    END IF;
    
    -- Combine all hierarchical results into a single array
    IF array_length(hierarchical_results, 1) > 0 THEN
        combined_hierarchical_result := jsonb_build_array();
        
        -- Add each hierarchical result to the combined array
        FOR i IN 1..array_length(hierarchical_results, 1) LOOP
            combined_hierarchical_result := combined_hierarchical_result || hierarchical_results[i];
        END LOOP;
    ELSE
        combined_hierarchical_result := '[]'::jsonb;
    END IF;
    
    -- Compute regular (non-hierarchical) facets
    regular_facets_result := '[]'::jsonb;
    IF hierarchies.regular_facets IS NOT NULL AND array_length(hierarchies.regular_facets, 1) > 0 THEN
        regular_facets_result := jsonb_build_array();
        
        -- For each regular facet, get its counts
        FOREACH root_facet IN ARRAY hierarchies.regular_facets
        LOOP
            DECLARE
                facet_counts jsonb;
                facet_item jsonb;
                facet_rec record;
            BEGIN
                -- Get facet counts for this regular facet
                facet_counts := '[]'::jsonb;
                
                FOR facet_rec IN 
                    SELECT * FROM facets.get_facet_counts(
                        p_table_id,
                        root_facet,
                        filter_bitmap,
                        n
                    )
                LOOP
                    facet_item := jsonb_build_object(
                        'facet_name', facet_rec.facet_name,
                        'facet_id', facet_rec.facet_id,
                        'value', facet_rec.facet_value,
                        'count', facet_rec.cardinality
                    );
                    facet_counts := facet_counts || jsonb_build_array(facet_item);
                END LOOP;
                
                -- Add to regular facets result if we have counts
                IF jsonb_array_length(facet_counts) > 0 THEN
                    regular_facets_result := regular_facets_result || jsonb_build_array(
                        jsonb_build_object(
                            'facet_name', root_facet,
                            'values', facet_counts
                        )
                    );
                END IF;
            END;
        END LOOP;
    END IF;
    
    -- Combine hierarchical and regular facets
    IF jsonb_array_length(combined_hierarchical_result) > 0 AND jsonb_array_length(regular_facets_result) > 0 THEN
        combined_result := combined_hierarchical_result || regular_facets_result;
    ELSIF jsonb_array_length(combined_hierarchical_result) > 0 THEN
        combined_result := combined_hierarchical_result;
    ELSIF jsonb_array_length(regular_facets_result) > 0 THEN
        combined_result := regular_facets_result;
    ELSE
        combined_result := '[]'::jsonb;
    END IF;
    
    RETURN combined_result;
END;
$$;

-- SECTION 3: Restore search_documents and search_documents_with_facets functions

-- Function to search documents without facet calculation
CREATE OR REPLACE FUNCTION facets.search_documents(
    p_schema_name text,
    p_table_name text,
    p_query text,
    p_vector_column text DEFAULT NULL,
    p_content_column text DEFAULT 'content',
    p_metadata_column text DEFAULT 'metadata',
    p_created_at_column text DEFAULT 'created_at',
    p_updated_at_column text DEFAULT 'updated_at',
    p_limit integer DEFAULT 10,
    p_offset integer DEFAULT 0,
    p_min_score double precision DEFAULT 0.0,
    p_vector_weight double precision DEFAULT 0.5,
    p_language text DEFAULT 'english'
) RETURNS TABLE(
    results jsonb,
    total_found bigint,
    search_time integer
) LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_start_time timestamp;
    v_results jsonb;
    v_total_found bigint;
    v_table_id oid;
    v_query_bitmap roaringbitmap;
    v_result_bitmap roaringbitmap;
    v_min_score_val double precision;
    v_limit_val int;
    v_tdef facets.faceted_table;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Pre-compute values
    v_min_score_val := COALESCE(p_min_score, 0.0);
    v_limit_val := COALESCE(NULLIF(p_limit, 0), 2147483647);
    
    -- Get the table definition
    SELECT ft.* INTO v_tdef
    FROM facets.faceted_table ft
    WHERE ft.schemaname = p_schema_name AND ft.tablename = p_table_name;
    
    IF v_tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table %.% not found in facets.faceted_table', p_schema_name, p_table_name;
    END IF;
    
    v_table_id := v_tdef.table_id;
    
    -- Handle Text Search / BM25
    IF p_query IS NOT NULL AND p_query != '' AND trim(p_query) != '' THEN
        -- Get bitmap of documents matching the query using BM25 index
        RAISE NOTICE '[SQL TRACE] About to call bm25_get_matches_bitmap_native with table_id=%, query=%, language=%', v_table_id, p_query, p_language;
        v_query_bitmap := facets.bm25_get_matches_bitmap_native(v_table_id, p_query, p_language);
        RAISE NOTICE '[SQL TRACE] bm25_get_matches_bitmap_native returned successfully';
        
        -- Check bitmap state carefully to avoid crash
        IF v_query_bitmap IS NULL THEN
            RAISE NOTICE '[SQL TRACE] bitmap is NULL';
        ELSE
            RAISE NOTICE '[SQL TRACE] bitmap is NOT NULL, checking if empty...';
            IF rb_is_empty(v_query_bitmap) THEN
                RAISE NOTICE '[SQL TRACE] bitmap is EMPTY';
            ELSE
                RAISE NOTICE '[SQL TRACE] bitmap is NOT EMPTY, cardinality=%', rb_cardinality(v_query_bitmap);
            END IF;
        END IF;
        
        -- If query results are empty, and no vector search fallback, we can return early
        RAISE NOTICE '[SQL TRACE] Checking if bitmap is empty for early return...';
        IF (v_query_bitmap IS NULL OR rb_is_empty(v_query_bitmap)) AND p_vector_column IS NULL THEN
            RAISE NOTICE '[SQL TRACE] Early return: bitmap is NULL or empty and no vector column';
            RETURN QUERY SELECT
                '[]'::jsonb,
                0::bigint,
                EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
            RETURN;
        END IF;
        
        RAISE NOTICE '[SQL TRACE] Setting v_result_bitmap = v_query_bitmap';
        v_result_bitmap := v_query_bitmap;
        RAISE NOTICE '[SQL TRACE] v_result_bitmap set successfully';
    ELSE
        -- Empty query case: no bitmap filter (all documents)
        v_result_bitmap := NULL;
    END IF;

    RAISE NOTICE '[SQL TRACE] About to calculate scores and results...';
    -- Calculate Scores and Results
    IF p_query IS NOT NULL AND p_query != '' AND trim(p_query) != '' THEN
        -- Text Search Case
        IF p_vector_column IS NOT NULL THEN
            -- With vector search
            EXECUTE format('
                WITH candidates AS (
                    SELECT %I AS id FROM %I.%I
                    WHERE %s%s
                ),
                scored AS (
                    SELECT
                        c.id,
                        t.%I AS content,
                        facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                        CASE WHEN $9 IS NOT NULL THEN 1 - ((t.%I <=> $2::vector) / 2) ELSE 0.0 END AS vector_score,
                        t.%I AS created_at,
                        t.%I AS updated_at,
                        t.%I AS metadata
                    FROM candidates c
                    JOIN %I.%I t ON t.%I = c.id
                ),
                combined AS (
                    SELECT *,
                        (bm25_score * (1 - $4) + vector_score * $4) AS combined_score
                    FROM scored
                ),
                filtered AS (
                    SELECT * FROM combined WHERE combined_score >= $5
                )
                SELECT
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            ''id'', id,
                            ''content'', content,
                            ''bm25_score'', bm25_score::float,
                            ''vector_score'', vector_score::float,
                            ''combined_score'', combined_score::float,
                            ''created_at'', created_at,
                            ''updated_at'', updated_at,
                            ''metadata'', metadata
                        )
                        ORDER BY combined_score DESC
                    ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $6 OFFSET $7) p),
                    (SELECT COUNT(*)::bigint FROM filtered)
            ',
                v_tdef.key, p_schema_name, p_table_name,
                CASE
                    WHEN v_result_bitmap IS NOT NULL THEN quote_ident(v_tdef.key) || ' = ANY(rb_to_array($8))'
                    ELSE 'TRUE'
                END,
                CASE
                    WHEN v_result_bitmap IS NOT NULL THEN ' OR ' || quote_ident(p_vector_column) || ' <=> $2::vector < 0.8'
                    ELSE quote_ident(p_vector_column) || ' <=> $2::vector < 0.8'
                END,
                p_content_column,
                quote_ident(p_vector_column),
                p_created_at_column, p_updated_at_column, p_metadata_column,
                p_schema_name, p_table_name, v_tdef.key
            ) INTO v_results, v_total_found
            USING v_table_id, p_query, p_language, p_vector_weight, v_min_score_val, v_limit_val, p_offset, v_result_bitmap, p_vector_column;
        ELSE
            -- BM25 only (no vector)
            EXECUTE format('
                WITH candidates AS (
                    SELECT %I AS id FROM %I.%I
                    WHERE %s
                ),
                scored AS (
                    SELECT
                        c.id,
                        t.%I AS content,
                        facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                        0.0 AS vector_score,
                        t.%I AS created_at,
                        t.%I AS updated_at,
                        t.%I AS metadata
                    FROM candidates c
                    JOIN %I.%I t ON t.%I = c.id
                ),
                combined AS (
                    SELECT *,
                        bm25_score AS combined_score
                    FROM scored
                ),
                filtered AS (
                    SELECT * FROM combined WHERE combined_score >= $4
                )
                SELECT
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            ''id'', id,
                            ''content'', content,
                            ''bm25_score'', bm25_score::float,
                            ''vector_score'', vector_score::float,
                            ''combined_score'', combined_score::float,
                            ''created_at'', created_at,
                            ''updated_at'', updated_at,
                            ''metadata'', metadata
                        )
                        ORDER BY combined_score DESC
                    ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $5 OFFSET $6) p),
                    (SELECT COUNT(*)::bigint FROM filtered)
            ',
                v_tdef.key, p_schema_name, p_table_name,
                CASE
                    WHEN v_result_bitmap IS NOT NULL THEN quote_ident(v_tdef.key) || ' = ANY(rb_to_array($7))'
                    ELSE 'TRUE'
                END,
                p_content_column,
                p_created_at_column, p_updated_at_column, p_metadata_column,
                p_schema_name, p_table_name, v_tdef.key
            ) INTO v_results, v_total_found
            USING v_table_id, p_query, p_language, v_min_score_val, v_limit_val, p_offset, v_result_bitmap;
        END IF;
    ELSE
        -- Browsing Case (No Query)
        IF v_result_bitmap IS NOT NULL THEN
            v_total_found := rb_cardinality(v_result_bitmap);
        ELSE
            EXECUTE format('SELECT count(*) FROM %I.%I', p_schema_name, p_table_name) INTO v_total_found;
        END IF;
        
        EXECUTE format('
            WITH ids AS (
                SELECT %s AS id
                LIMIT $2 OFFSET $3
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', t.%I,
                    ''content'', t.%I,
                    ''bm25_score'', 1.0::float,
                    ''vector_score'', 0.0::float,
                    ''combined_score'', 1.0::float,
                    ''created_at'', t.%I,
                    ''updated_at'', t.%I,
                    ''metadata'', t.%I
                )
            )
            FROM ids i
            JOIN %I.%I t ON t.%I = i.id
        ',
            CASE
                WHEN v_result_bitmap IS NOT NULL THEN 'unnest(rb_to_array($1))'
                ELSE quote_ident(v_tdef.key) || ' FROM ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name)
            END,
            v_tdef.key,
            p_content_column,
            p_created_at_column, p_updated_at_column, p_metadata_column,
            p_schema_name, p_table_name, v_tdef.key
        ) INTO v_results
        USING v_result_bitmap, v_limit_val, p_offset;
    END IF;

    -- If no results found, initialize to empty array
    IF v_results IS NULL THEN
        v_results := '[]'::jsonb;
    END IF;

    -- Return the final result without facets
    RETURN QUERY SELECT
        COALESCE(v_results, '[]'::jsonb),
        COALESCE(v_total_found, 0)::bigint,
        EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
END;
$$;

-- Function to search documents with facets and embeddings/BM25
-- OPTIMIZED VERSION: Uses roaring bitmaps to avoid array explosions with large result sets
-- Key optimizations:
-- 1. Uses bitmap for facet filtering (no 8M element arrays!)
-- 2. Only fetches actual rows for paginated results
-- 3. Uses bitmap directly for facet calculation
CREATE OR REPLACE FUNCTION facets.search_documents_with_facets(
    p_schema_name text,
    p_table_name text,
    p_query text,
    p_facets jsonb DEFAULT NULL,
    p_vector_column text DEFAULT NULL,
    p_content_column text DEFAULT 'content',
    p_metadata_column text DEFAULT 'metadata',
    p_created_at_column text DEFAULT 'created_at',
    p_updated_at_column text DEFAULT 'updated_at',
    p_limit integer DEFAULT 10,
    p_offset integer DEFAULT 0,
    p_min_score double precision DEFAULT 0.0,
    p_vector_weight double precision DEFAULT 0.5,
    p_facet_limit integer DEFAULT 5,
    p_language text DEFAULT NULL  -- NULL = use table's bm25_language, or 'english' as fallback
) RETURNS TABLE(
    results jsonb,
    facets jsonb,
    total_found bigint,
    search_time integer
) LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_start_time timestamp;
    v_results jsonb;
    v_facets jsonb;
    v_total_found bigint;
    v_table_id oid;
    v_filter_bitmap roaringbitmap;
    v_query_bitmap roaringbitmap;
    v_result_bitmap roaringbitmap;
    v_min_score_val double precision;
    v_limit_val int;
    v_tdef facets.faceted_table;
    v_effective_language text;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Pre-compute values
    v_min_score_val := COALESCE(p_min_score, 0.0);
    v_limit_val := COALESCE(NULLIF(p_limit, 0), 2147483647);
    
    -- Get the table definition
    SELECT ft.* INTO v_tdef
    FROM facets.faceted_table ft
    WHERE ft.schemaname = p_schema_name AND ft.tablename = p_table_name;
    
    IF v_tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table %.% not found in facets.faceted_table', p_schema_name, p_table_name;
    END IF;
    
    v_table_id := v_tdef.table_id;
    
    -- Determine effective language: use p_language if provided, else table's bm25_language
    v_effective_language := COALESCE(p_language, v_tdef.bm25_language, 'english');
    
    -- 1. Get filter bitmap from facet requirements
    IF p_facets IS NOT NULL AND jsonb_typeof(p_facets) = 'object' AND p_facets != '{}'::jsonb THEN
        v_filter_bitmap := facets.filter_documents_by_facets_bitmap(p_schema_name, p_facets, p_table_name);
        
        -- Early exit if facet filter returns empty
        IF v_filter_bitmap IS NOT NULL AND rb_is_empty(v_filter_bitmap) THEN
            RETURN QUERY SELECT 
                '[]'::jsonb,
                '[]'::jsonb,
                0::bigint,
                EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
            RETURN;
        END IF;
    END IF;
    
    -- 2. Handle Text Search / BM25
    IF p_query IS NOT NULL AND p_query != '' AND trim(p_query) != '' THEN
        -- Get bitmap of documents matching the query using BM25 index
        v_query_bitmap := facets.bm25_get_matches_bitmap_native(v_table_id, p_query, v_effective_language);
        
        -- If query results are empty, and no vector search fallback, we can return early
        IF (v_query_bitmap IS NULL OR rb_is_empty(v_query_bitmap)) AND p_vector_column IS NULL THEN
            RETURN QUERY SELECT 
                '[]'::jsonb,
                '[]'::jsonb,
                0::bigint,
                EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
            RETURN;
        END IF;
        
        -- Intersect query bitmap with facet filter
        IF v_filter_bitmap IS NOT NULL THEN
            IF v_query_bitmap IS NOT NULL THEN
                v_result_bitmap := rb_and(v_filter_bitmap, v_query_bitmap);
            ELSE
                v_result_bitmap := v_filter_bitmap; 
            END IF;
        ELSE
            v_result_bitmap := v_query_bitmap;
        END IF;
    ELSE
        -- Empty query case: result is just the filter bitmap
        v_result_bitmap := v_filter_bitmap;
    END IF;

    -- 3. Calculate Scores and Results
    RAISE NOTICE '[SQL TRACE v2] Calculating scores and results...';
    IF p_query IS NOT NULL AND p_query != '' AND trim(p_query) != '' THEN
        -- Text Search Case
        RAISE NOTICE '[SQL TRACE v2] Text search case, checking vector column...';
        IF p_vector_column IS NOT NULL THEN
            -- With vector search (simplified to BM25 only for now)
            -- Note: Full vector search integration would require a separate vector query parameter
            RAISE NOTICE '[SQL TRACE v2] Vector column is NOT NULL, checking result bitmap...';
            IF v_result_bitmap IS NOT NULL THEN
                -- With bitmap filter - use rb_contains for better performance
                RAISE NOTICE '[SQL TRACE v2] Executing query with rb_contains filter...';
                EXECUTE format('
                    WITH candidates AS (
                        SELECT %I AS id FROM %I.%I
                        WHERE rb_contains($7, %I::int4)
                    ),
                    scored AS (
                        SELECT
                            c.id,
                            t.%I AS content,
                            facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                            0.0 AS vector_score,
                            t.%I AS created_at,
                            t.%I AS updated_at,
                            t.%I AS metadata
                        FROM candidates c
                        JOIN %I.%I t ON t.%I = c.id
                    ),
                    combined AS (
                        SELECT *,
                            bm25_score AS combined_score
                        FROM scored
                    ),
                    filtered AS (
                        SELECT * FROM combined WHERE combined_score >= $4
                    )
                    SELECT 
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                ''id'', id,
                                ''content'', content,
                                ''bm25_score'', bm25_score::float,
                                ''vector_score'', vector_score::float,
                                ''combined_score'', combined_score::float,
                                ''created_at'', created_at,
                                ''updated_at'', updated_at,
                                ''metadata'', metadata
                            )
                            ORDER BY combined_score DESC
                        ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $5 OFFSET $6) p),
                        (SELECT COUNT(*)::bigint FROM filtered)
                ', 
                    v_tdef.key, p_schema_name, p_table_name, v_tdef.key,
                    p_content_column,
                    p_created_at_column, p_updated_at_column, p_metadata_column,
                    p_schema_name, p_table_name, v_tdef.key
                ) INTO v_results, v_total_found
                USING v_table_id, p_query, p_language, v_min_score_val, v_limit_val, p_offset, v_result_bitmap;
            ELSE
                -- No bitmap filter
                EXECUTE format('
                    WITH candidates AS (
                        SELECT %I AS id FROM %I.%I
                    ),
                    scored AS (
                        SELECT
                            c.id,
                            t.%I AS content,
                            facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                            0.0 AS vector_score,
                            t.%I AS created_at,
                            t.%I AS updated_at,
                            t.%I AS metadata
                        FROM candidates c
                        JOIN %I.%I t ON t.%I = c.id
                    ),
                    combined AS (
                        SELECT *,
                            bm25_score AS combined_score
                        FROM scored
                    ),
                    filtered AS (
                        SELECT * FROM combined WHERE combined_score >= $4
                    )
                SELECT 
                    (SELECT jsonb_agg(
                        jsonb_build_object(
                            ''id'', id,
                            ''content'', content,
                            ''bm25_score'', bm25_score::float,
                            ''vector_score'', vector_score::float,
                            ''combined_score'', combined_score::float,
                            ''created_at'', created_at,
                            ''updated_at'', updated_at,
                            ''metadata'', metadata
                        )
                        ORDER BY combined_score DESC
                    ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $5 OFFSET $6) p),
                    (SELECT COUNT(*)::bigint FROM filtered)
                ', 
                    v_tdef.key, p_schema_name, p_table_name,
                    p_content_column,
                    p_created_at_column, p_updated_at_column, p_metadata_column,
                    p_schema_name, p_table_name, v_tdef.key
                ) INTO v_results, v_total_found
                USING v_table_id, p_query, p_language, v_min_score_val, v_limit_val, p_offset;
            END IF;
        ELSE
            -- BM25 only (no vector)
            RAISE NOTICE '[SQL TRACE v2] BM25 only case (no vector)';
            IF v_result_bitmap IS NOT NULL THEN
                -- With bitmap filter - use rb_contains for better performance
                RAISE NOTICE '[SQL TRACE v2] v_result_bitmap is NOT NULL, executing query with rb_contains...';
                RAISE NOTICE '[SQL TRACE v2] v_result_bitmap cardinality=%', rb_cardinality(v_result_bitmap);
                EXECUTE format('
                    WITH candidates AS (
                        SELECT %I AS id FROM %I.%I
                        WHERE rb_contains($7, %I::int4)
                    ),
                    scored AS (
                        SELECT
                            c.id,
                            t.%I AS content,
                            facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                            0.0 AS vector_score,
                            t.%I AS created_at,
                            t.%I AS updated_at,
                            t.%I AS metadata
                        FROM candidates c
                        JOIN %I.%I t ON t.%I = c.id
                    ),
                    combined AS (
                        SELECT *,
                            bm25_score AS combined_score
                        FROM scored
                    ),
                    filtered AS (
                        SELECT * FROM combined WHERE combined_score >= $4
                    )
                    SELECT 
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                ''id'', id,
                                ''content'', content,
                                ''bm25_score'', bm25_score::float,
                                ''vector_score'', vector_score::float,
                                ''combined_score'', combined_score::float,
                                ''created_at'', created_at,
                                ''updated_at'', updated_at,
                                ''metadata'', metadata
                            )
                            ORDER BY combined_score DESC
                        ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $5 OFFSET $6) p),
                        (SELECT COUNT(*)::bigint FROM filtered)
                ', 
                    v_tdef.key, p_schema_name, p_table_name, v_tdef.key,
                    p_content_column,
                    p_created_at_column, p_updated_at_column, p_metadata_column,
                    p_schema_name, p_table_name, v_tdef.key
                ) INTO v_results, v_total_found
                USING v_table_id, p_query, p_language, v_min_score_val, v_limit_val, p_offset, v_result_bitmap;
            ELSE
                -- No bitmap filter (all documents)
                EXECUTE format('
                    WITH candidates AS (
                        SELECT %I AS id FROM %I.%I
                    ),
                    scored AS (
                        SELECT
                            c.id,
                            t.%I AS content,
                            facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                            0.0 AS vector_score,
                            t.%I AS created_at,
                            t.%I AS updated_at,
                            t.%I AS metadata
                        FROM candidates c
                        JOIN %I.%I t ON t.%I = c.id
                    ),
                    combined AS (
                        SELECT *,
                            bm25_score AS combined_score
                        FROM scored
                    ),
                    filtered AS (
                        SELECT * FROM combined WHERE combined_score >= $4
                    )
                    SELECT 
                        (SELECT jsonb_agg(
                            jsonb_build_object(
                                ''id'', id,
                                ''content'', content,
                                ''bm25_score'', bm25_score::float,
                                ''vector_score'', vector_score::float,
                                ''combined_score'', combined_score::float,
                                ''created_at'', created_at,
                                ''updated_at'', updated_at,
                                ''metadata'', metadata
                            )
                            ORDER BY combined_score DESC
                        ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $5 OFFSET $6) p),
                        (SELECT COUNT(*)::bigint FROM filtered)
                ', 
                    v_tdef.key, p_schema_name, p_table_name,
                    p_content_column,
                    p_created_at_column, p_updated_at_column, p_metadata_column,
                    p_schema_name, p_table_name, v_tdef.key
                ) INTO v_results, v_total_found
                USING v_table_id, p_query, p_language, v_min_score_val, v_limit_val, p_offset;
            END IF;
        END IF;
    ELSE
        -- Browsing Case (No Query)
        IF v_result_bitmap IS NOT NULL THEN
            v_total_found := rb_cardinality(v_result_bitmap);
        ELSE
            EXECUTE format('SELECT count(*) FROM %I.%I', p_schema_name, p_table_name) INTO v_total_found;
        END IF;
        
        EXECUTE format('
            WITH ids AS (
                SELECT %s AS id
                LIMIT $2 OFFSET $3
            )
            SELECT jsonb_agg(
                jsonb_build_object(
                    ''id'', t.%I,
                    ''content'', t.%I,
                    ''bm25_score'', 1.0::float,
                    ''vector_score'', 0.0::float,
                    ''combined_score'', 1.0::float,
                    ''created_at'', t.%I,
                    ''updated_at'', t.%I,
                    ''metadata'', t.%I
                )
            )
            FROM ids i
            JOIN %I.%I t ON t.%I = i.id
        ',
            CASE 
                WHEN v_result_bitmap IS NOT NULL THEN 'unnest(rb_to_array($1))'
                ELSE quote_ident(v_tdef.key) || ' FROM ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name)
            END,
            v_tdef.key,
            p_content_column,
            p_created_at_column, p_updated_at_column, p_metadata_column,
            p_schema_name, p_table_name, v_tdef.key
        ) INTO v_results
        USING v_result_bitmap, v_limit_val, p_offset;
    END IF;

    -- 4. Merge deltas before getting facet counts (ensures counts are up-to-date)
    PERFORM facets.merge_deltas(v_table_id);

    -- 5. Get Facet Counts
    v_facets := facets.get_facet_counts_by_bitmap(v_table_id, v_result_bitmap, p_facet_limit);
    
    RETURN QUERY SELECT 
        COALESCE(v_results, '[]'::jsonb),
        COALESCE(v_facets, '[]'::jsonb),
        COALESCE(v_total_found, 0)::bigint,
        EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
END;
$$;

-- Create a helper function with a different name to avoid recursion
CREATE OR REPLACE FUNCTION facets.count_results(
    p_table_id oid, 
    filters facets.facet_filter[]
) RETURNS TABLE(
    facet_name text,
    facet_value text,
    cardinality bigint,
    facet_id int
) LANGUAGE plpgsql AS $$
DECLARE
    tdef facets.faceted_table;
    select_facets int[];
    sql text;
    has_filters boolean;
BEGIN
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    
    has_filters := array_length(filters, 1) > 0;
    
    SELECT array_agg(fd.facet_id) INTO select_facets
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id
    AND (NOT has_filters OR fd.facet_name NOT IN (SELECT f.facet_name FROM unnest(filters) f));
    
    IF has_filters THEN
        -- Original SQL for when filters exist
        sql := format($sql$
        WITH filters AS (
            SELECT facet_id, facet_name, facet_value
            FROM facets.facet_definition JOIN unnest($1) t USING (facet_name)
            WHERE table_id = $2
        ), lookup AS (
            SELECT chunk_id, rb_and_agg(postinglist) postinglist
            FROM %s d JOIN filters USING (facet_id, facet_value)
            GROUP BY chunk_id
        ), results AS (
            SELECT facet_id, facet_value, sum(rb_and_cardinality(lookup.postinglist, d.postinglist))::bigint cardinality
            FROM lookup JOIN %s d USING (chunk_id)
            WHERE facet_id = ANY ($3)
            GROUP BY facet_id, facet_value
        )
        SELECT fd.facet_name, results.facet_value, results.cardinality, fd.facet_id
        FROM results JOIN facets.facet_definition fd USING (facet_id)
        WHERE fd.table_id = $2
        ORDER BY facet_id, cardinality DESC, facet_value
        $sql$,
        facets._qualified(tdef.schemaname, tdef.facets_table),
        facets._qualified(tdef.schemaname, tdef.facets_table));
    ELSE
        -- Modified SQL for when no filters exist
        sql := format($sql$
        SELECT fd.facet_name, d.facet_value, 
            sum(rb_cardinality(d.postinglist))::bigint as cardinality, 
            fd.facet_id
        FROM %s d 
        JOIN facets.facet_definition fd 
        ON d.facet_id = fd.facet_id
        WHERE fd.table_id = $2
        AND fd.facet_id = ANY ($3)
        GROUP BY fd.facet_name, d.facet_value, fd.facet_id
        ORDER BY fd.facet_id, cardinality DESC, d.facet_value
        $sql$,
        facets._qualified(tdef.schemaname, tdef.facets_table));
    END IF;
    RAISE NOTICE 'sql: %', sql;
    RETURN QUERY EXECUTE sql USING filters, p_table_id, select_facets;
END;
$$;


-- =============================================================================
-- SECTION: ZIG NATIVE FUNCTIONS (High Performance)
-- These functions are implemented in Zig for maximum performance
-- =============================================================================

-- Native delta merging - processes facet updates efficiently
CREATE OR REPLACE FUNCTION merge_deltas_native(table_id oid)
RETURNS int
AS 'MODULE_PATHNAME', 'merge_deltas_native'
LANGUAGE C STRICT;

-- Native bitmap filter builder - creates roaring bitmap from facet filters
CREATE OR REPLACE FUNCTION build_filter_bitmap_native(
    table_id oid,
    filters facets.facet_filter[]
)
RETURNS roaringbitmap
AS 'MODULE_PATHNAME', 'build_filter_bitmap_native'
LANGUAGE C;

-- Native facet counting - fast bitmap-based counting
CREATE OR REPLACE FUNCTION get_facet_counts_native(
    table_id oid,
    filter_bitmap roaringbitmap DEFAULT NULL,
    facets text[] DEFAULT NULL,
    top_n int DEFAULT 5
)
RETURNS SETOF facets.facet_counts
AS 'MODULE_PATHNAME', 'get_facet_counts_native'
LANGUAGE C;

-- Native document search - returns filtered document IDs
CREATE OR REPLACE FUNCTION search_documents_native(
    table_id oid,
    filters facets.facet_filter[],
    limit_n int DEFAULT 100,
    offset_n int DEFAULT 0
)
RETURNS SETOF bigint
AS 'MODULE_PATHNAME', 'search_documents_native'
LANGUAGE C;

-- Native filter_documents_by_facets_bitmap with JSONB support - optimized implementation
CREATE OR REPLACE FUNCTION filter_documents_by_facets_bitmap_jsonb_native(
    schema_name text,
    facets jsonb,
    table_name text DEFAULT NULL
)
RETURNS roaringbitmap
AS 'MODULE_PATHNAME', 'filter_documents_by_facets_bitmap_jsonb_native'
LANGUAGE C;

-- Hardware support detection - returns composite type with support_code and description
-- Returns: (support_code integer, description text)
-- support_code values:
--   0 = No SIMD support
--   1 = AVX2 support only
--   2 = AVX-512 support only
--   3 = Both AVX2 and AVX-512 support
CREATE OR REPLACE FUNCTION facets.current_hardware(OUT support_code integer, OUT description text)
RETURNS record
AS 'MODULE_PATHNAME', 'current_hardware'
LANGUAGE C STRICT;



-- =============================================================================
-- MERGE DELTAS IMPLEMENTATIONS
-- =============================================================================
-- Two implementations available:
-- 1. SQL-based (facets.apply_deltas) - Uses temp tables and batch operations, FAST for large datasets
-- 2. Native Zig (merge_deltas_native) - Row-by-row operations, SLOW for large datasets
--
-- DEFAULT: SQL-based for performance. Use merge_deltas_native() directly if needed.
-- =============================================================================

-- Main merge_deltas function - uses fast SQL-based batching by default
CREATE OR REPLACE FUNCTION facets.merge_deltas(p_table_id oid)
RETURNS void AS $$
BEGIN
    -- Use SQL-based apply_deltas which batches operations using temp tables
    -- This is MUCH faster than native for large datasets (minutes vs hours)
    PERFORM facets.apply_deltas(p_table_id);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION facets.merge_deltas(p_table regclass)
RETURNS void AS $$
BEGIN
    PERFORM facets.apply_deltas(p_table::oid);
END;
$$ LANGUAGE plpgsql;

-- Explicit native version - available if needed for specific use cases
-- WARNING: This is slow for large datasets (does row-by-row operations)
CREATE OR REPLACE FUNCTION facets.merge_deltas_native_wrapper(p_table_id oid)
RETURNS void AS $$
BEGIN
    PERFORM merge_deltas_native(p_table_id);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION facets.merge_deltas_native_wrapper(p_table regclass)
RETURNS void AS $$
BEGIN
    PERFORM merge_deltas_native(p_table::oid);
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- ADDITIONAL HELPER: filter_documents_by_facets with JSONB interface
-- =============================================================================

CREATE OR REPLACE FUNCTION facets.filter_documents_by_facets(
    p_schema_name text,
    p_facets jsonb,
    p_table_name text DEFAULT NULL
) RETURNS SETOF bigint LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table_id oid;
    v_filters facets.facet_filter[];
    v_tdef facets.faceted_table;
BEGIN
    -- Find the table
    IF p_table_name IS NOT NULL THEN
        SELECT ft.* INTO v_tdef
        FROM facets.faceted_table ft
        WHERE ft.schemaname = p_schema_name AND ft.tablename = p_table_name;
    ELSE
        SELECT ft.* INTO v_tdef
        FROM facets.faceted_table ft
        WHERE ft.schemaname = p_schema_name
        LIMIT 1;
    END IF;
    
    IF v_tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table not found in schema %', p_schema_name;
    END IF;
    
    v_table_id := v_tdef.table_id;

    IF p_facets IS NULL OR p_facets = '{}'::jsonb THEN
        RETURN;
    END IF;

    -- Convert JSONB to facet_filter[]
    SELECT array_agg((key, value)::facets.facet_filter)
    INTO v_filters
    FROM jsonb_each_text(p_facets);

    IF v_filters IS NULL OR array_length(v_filters, 1) = 0 THEN
        RETURN;
    END IF;

    -- Use Zig native function for filtering
    RETURN QUERY SELECT search_documents_native(v_table_id, v_filters, 2147483647, 0);
END;
$$;

-- =============================================================================
-- BITMAP-BASED VERSION: Returns roaringbitmap instead of individual IDs
-- This is MUCH more efficient for large result sets (millions of documents)
-- =============================================================================

CREATE OR REPLACE FUNCTION facets.filter_documents_by_facets_bitmap(
    p_schema_name text,
    p_facets jsonb,
    p_table_name text DEFAULT NULL
) RETURNS roaringbitmap LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_result_bitmap roaringbitmap;
BEGIN
    -- Try to use native implementation first (faster)
    -- Fall back to SQL implementation if native is not available
    BEGIN
        SELECT filter_documents_by_facets_bitmap_jsonb_native(p_schema_name, p_facets, p_table_name)
        INTO v_result_bitmap;
        RETURN v_result_bitmap;
    EXCEPTION
        WHEN OTHERS THEN
            -- Fall back to SQL implementation
            NULL;
    END;
    
    -- SQL implementation (fallback)
    DECLARE
        v_tdef facets.faceted_table;
        v_facet_bitmap roaringbitmap;
        v_facet_name text;
        v_facet_value text;
        v_facet_id int;
    BEGIN
        -- Find the table
        IF p_table_name IS NOT NULL THEN
            SELECT ft.* INTO v_tdef
            FROM facets.faceted_table ft
            WHERE ft.schemaname = p_schema_name AND ft.tablename = p_table_name;
        ELSE
            SELECT ft.* INTO v_tdef
            FROM facets.faceted_table ft
            WHERE ft.schemaname = p_schema_name
            LIMIT 1;
        END IF;
        
        IF v_tdef.table_id IS NULL THEN
            RAISE EXCEPTION 'Table not found in schema %', p_schema_name;
        END IF;

        IF p_facets IS NULL OR p_facets = '{}'::jsonb THEN
            RETURN NULL;
        END IF;

        v_result_bitmap := NULL;

        -- Process each facet filter and AND them together
        FOR v_facet_name, v_facet_value IN SELECT key, value FROM jsonb_each_text(p_facets)
        LOOP
            -- Get facet_id for this facet name
            SELECT facet_id INTO v_facet_id
            FROM facets.facet_definition
            WHERE table_id = v_tdef.table_id AND facet_name = v_facet_name;
            
            IF v_facet_id IS NULL THEN
                -- Unknown facet, return empty bitmap
                RETURN rb_build(ARRAY[]::int[]);
            END IF;
            
            -- Get bitmap for this facet value (OR multiple values if comma-separated)
            -- IMPORTANT: Reconstruct original IDs from (chunk_id << chunk_bits) | in_chunk_id
            -- OPTIMIZED: First aggregate postinglists by chunk_id, then reconstruct IDs
            -- This reduces the number of array conversions needed
            EXECUTE format(
                'SELECT rb_build_agg(((chunk_id::bigint << $3) | pl.in_chunk_id)::int4)
                 FROM (
                     SELECT chunk_id, rb_or_agg(postinglist) AS postinglist
                     FROM %I.%I
                     WHERE facet_id = $1 AND facet_value = ANY(string_to_array($2, '',''))
                     GROUP BY chunk_id
                 ) f
                 CROSS JOIN LATERAL unnest(rb_to_array(f.postinglist)) AS pl(in_chunk_id)',
                v_tdef.schemaname, v_tdef.facets_table
            ) INTO v_facet_bitmap USING v_facet_id, v_facet_value, v_tdef.chunk_bits;
            
            IF v_facet_bitmap IS NULL OR rb_is_empty(v_facet_bitmap) THEN
                -- No matches for this facet value, return empty
                RETURN rb_build(ARRAY[]::int[]);
            END IF;
            
            -- AND with existing result
            IF v_result_bitmap IS NULL THEN
                v_result_bitmap := v_facet_bitmap;
            ELSE
                v_result_bitmap := rb_and(v_result_bitmap, v_facet_bitmap);
                IF rb_is_empty(v_result_bitmap) THEN
                    RETURN v_result_bitmap;
                END IF;
            END IF;
        END LOOP;
        
        RETURN v_result_bitmap;
    END;
END;
$$;

-- =============================================================================
-- Helper function to get regular (non-hierarchical) facet counts
-- Used by hierarchical_facets_bitmap to avoid code duplication
-- =============================================================================

CREATE OR REPLACE FUNCTION facets._get_regular_facets(
    p_table_id oid,
    tdef facets.faceted_table,
    regular_facet_names text[],
    filter_bitmap roaringbitmap,
    n integer
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    regular_facets_result jsonb;
    root_facet text;
    facet_counts jsonb;
    facet_item jsonb;
    facet_rec record;
BEGIN
    -- Initialize result
    regular_facets_result := '[]'::jsonb;
    
    -- Early exit if no regular facets
    IF regular_facet_names IS NULL OR array_length(regular_facet_names, 1) IS NULL THEN
        RETURN regular_facets_result;
    END IF;
    
    regular_facets_result := jsonb_build_array();
    
    -- For each regular facet, get its counts
    FOREACH root_facet IN ARRAY regular_facet_names
    LOOP
        -- Get facet counts for this regular facet
        facet_counts := '[]'::jsonb;
        
        FOR facet_rec IN 
            SELECT * FROM facets.get_facet_counts(
                p_table_id,
                root_facet,
                filter_bitmap,
                n
            )
        LOOP
            facet_item := jsonb_build_object(
                'facet_name', facet_rec.facet_name,
                'facet_id', facet_rec.facet_id,
                'value', facet_rec.facet_value,
                'count', facet_rec.cardinality
            );
            facet_counts := facet_counts || jsonb_build_array(facet_item);
        END LOOP;
        
        -- Add to regular facets result if we have counts
        IF jsonb_array_length(facet_counts) > 0 THEN
            regular_facets_result := regular_facets_result || jsonb_build_array(
                jsonb_build_object(
                    'facet_name', root_facet,
                    'values', facet_counts
                )
            );
        END IF;
    END LOOP;
    
    RETURN regular_facets_result;
END;
$$;

-- =============================================================================
-- BITMAP-BASED hierarchical_facets overload
-- Accepts roaringbitmap directly instead of int[] to avoid array conversions
-- =============================================================================

CREATE OR REPLACE FUNCTION facets.hierarchical_facets_bitmap(
    p_table_id oid,
    n integer DEFAULT 5,
    p_filter_bitmap roaringbitmap DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    tdef facets.faceted_table;
    hierarchies record;
    filter_bitmap roaringbitmap;
    regular_facets_result jsonb;
    hierarchical_results jsonb[];
    combined_hierarchical_result jsonb;
    nested_hierarchical_result jsonb;
    combined_result jsonb;
    root_facets text[];
    root_facet text;
    hierarchical_result jsonb;
    active_document_count bigint;
    i int;
    j int;
    k int;
    facet jsonb;
    item jsonb;
    item_array jsonb;
    child jsonb;
    child_array jsonb;
    empty_jsonb jsonb := '[]'::jsonb;
BEGIN
    -- Get table definition
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF tdef.table_id IS NULL THEN
        RAISE EXCEPTION 'Table % not found', p_table_id;
    END IF;
    
    -- Get hierarchical information
    SELECT h.hierarchical_facets, h.regular_facets, h.main_hierarchy_root 
    INTO hierarchies 
    FROM facets._get_facet_hierarchies(p_table_id) h;
    
    -- Store the array of root facets
    root_facets := hierarchies.main_hierarchy_root;
    
    -- Use provided bitmap directly (no conversion needed!)
    filter_bitmap := p_filter_bitmap;
    
    IF filter_bitmap IS NOT NULL THEN
        active_document_count := rb_cardinality(filter_bitmap);
    ELSE
        active_document_count := NULL;
    END IF;
    
    -- Initialize array for hierarchical results
    hierarchical_results := ARRAY[]::jsonb[];
   
    -- Get hierarchical facets for each root
    IF root_facets IS NOT NULL AND array_length(root_facets, 1) > 0 THEN
        FOREACH root_facet IN ARRAY root_facets
        LOOP
            -- Get hierarchical facets for this root
            hierarchical_result := facets._get_hierarchical_facets(
                p_table_id,
                tdef,
                root_facet,
                filter_bitmap,
                NULL,
                n
            );

            -- Add to results array
            hierarchical_results := hierarchical_results || hierarchical_result;
        END LOOP;
    END IF;
    
    -- Get regular facets
    regular_facets_result := facets._get_regular_facets(
        p_table_id,
        tdef,
        hierarchies.regular_facets,
        filter_bitmap,
        n
    );
    
    -- Combine hierarchical results
    IF array_length(hierarchical_results, 1) > 0 THEN
        combined_hierarchical_result := jsonb_build_array();
        FOR i IN 1..array_length(hierarchical_results, 1) LOOP
            combined_hierarchical_result := combined_hierarchical_result || hierarchical_results[i];
        END LOOP;
    ELSE
        combined_hierarchical_result := '[]'::jsonb;
    END IF;
    
    -- Combine all results
    IF jsonb_array_length(combined_hierarchical_result) > 0 AND jsonb_array_length(regular_facets_result) > 0 THEN
        combined_result := combined_hierarchical_result || regular_facets_result;
    ELSIF jsonb_array_length(combined_hierarchical_result) > 0 THEN
        combined_result := combined_hierarchical_result;
    ELSIF jsonb_array_length(regular_facets_result) > 0 THEN
        combined_result := regular_facets_result;
    ELSE
        combined_result := '[]'::jsonb;
    END IF;
    
    RETURN combined_result;
END;
$$;

-- ============================================================================
-- SECTION: BM25 FULL-TEXT SEARCH
-- ============================================================================

-- BM25 Tables

-- Inverted index: term -> documents
CREATE TABLE IF NOT EXISTS facets.bm25_index (
    table_id oid NOT NULL,
    term_hash bigint NOT NULL,           -- Hash of lexeme (from to_tsvector)
    term_text text NOT NULL,             -- Original lexeme (for debugging/prefix matching)
    doc_ids roaringbitmap NOT NULL,      -- Documents containing this term
    term_freqs jsonb,                     -- Map: doc_id -> term_frequency
    language text DEFAULT 'english',      -- Text search config used
    PRIMARY KEY (table_id, term_hash),
    FOREIGN KEY (table_id) REFERENCES facets.faceted_table(table_id) ON DELETE CASCADE
);

-- Document metadata
CREATE TABLE IF NOT EXISTS facets.bm25_documents (
    table_id oid NOT NULL,
    doc_id bigint NOT NULL,               -- Document ID (matches faceted table's ID)
    doc_length int NOT NULL,              -- Number of tokens in document
    language text DEFAULT 'english',
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now(),
    PRIMARY KEY (table_id, doc_id),
    FOREIGN KEY (table_id) REFERENCES facets.faceted_table(table_id) ON DELETE CASCADE
);

-- Collection statistics (per table)
CREATE TABLE IF NOT EXISTS facets.bm25_statistics (
    table_id oid PRIMARY KEY,
    total_documents bigint NOT NULL,      -- N
    avg_document_length float NOT NULL,   -- avgdl
    last_updated timestamp DEFAULT now(),
    FOREIGN KEY (table_id) REFERENCES facets.faceted_table(table_id) ON DELETE CASCADE
);

-- BM25 Indexes

-- B-tree index for exact term lookups (standard BM25)
CREATE INDEX IF NOT EXISTS bm25_index_term_btree ON facets.bm25_index 
    (table_id, term_hash);

-- GIN index with trigram ops for prefix/fuzzy matching (optional, requires pg_trgm)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE INDEX IF NOT EXISTS bm25_index_term_prefix ON facets.bm25_index 
            USING gin (term_text gin_trgm_ops);
    END IF;
END $$;

-- Indexes for document metadata
CREATE INDEX IF NOT EXISTS bm25_documents_table_idx ON facets.bm25_documents (table_id);
CREATE INDEX IF NOT EXISTS bm25_documents_id_idx ON facets.bm25_documents (table_id, doc_id);

-- BM25 Native Function Declarations

-- Index a document for BM25 search
CREATE OR REPLACE FUNCTION facets.bm25_index_document_native(
    table_id oid,
    doc_id bigint,
    content text,
    language text DEFAULT 'english'
) RETURNS void
AS '$libdir/pg_facets', 'bm25_index_document_native'
LANGUAGE C STRICT;

-- Delete a document from BM25 index
CREATE OR REPLACE FUNCTION facets.bm25_delete_document_native(
    table_id oid,
    doc_id bigint
) RETURNS void
AS '$libdir/pg_facets', 'bm25_delete_document_native'
LANGUAGE C STRICT;

-- Search documents using BM25
CREATE OR REPLACE FUNCTION facets.bm25_search_native(
    table_id oid,
    query text,
    language text DEFAULT 'english',
    prefix_match boolean DEFAULT false,
    fuzzy_match boolean DEFAULT false,
    fuzzy_threshold float DEFAULT 0.3,
    k1 float DEFAULT 1.2,
    b float DEFAULT 0.75,
    limit_count int DEFAULT 10
) RETURNS TABLE(doc_id bigint, score float)
AS '$libdir/pg_facets', 'bm25_search_native'
LANGUAGE C STABLE;

-- Get roaring bitmap of documents matching query
CREATE OR REPLACE FUNCTION facets.bm25_get_matches_bitmap_native(
    table_id oid,
    query text,
    language text DEFAULT 'english',
    prefix_match boolean DEFAULT false,
    fuzzy_match boolean DEFAULT false,
    fuzzy_threshold float DEFAULT 0.3
) RETURNS roaringbitmap
AS '$libdir/pg_facets', 'bm25_get_matches_bitmap_native'
LANGUAGE C STABLE;

-- Calculate BM25 score for a single document
CREATE OR REPLACE FUNCTION facets.bm25_score_native(
    table_id oid,
    query text,
    doc_id bigint,
    language text DEFAULT 'english',
    k1 float DEFAULT 1.2,
    b float DEFAULT 0.75
) RETURNS float
AS '$libdir/pg_facets', 'bm25_score_native'
LANGUAGE C STABLE;

-- BM25 SQL Wrapper Functions

-- Index a document for BM25 search
CREATE OR REPLACE FUNCTION facets.bm25_index_document(
    p_table_id regclass,
    p_doc_id bigint,
    p_content text,
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT NULL  -- NULL = use table's bm25_language
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_effective_language text;
BEGIN
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    -- Check if table is registered in facets.faceted_table
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table % (oid: %) is not registered in facets.faceted_table. Run facets.add_faceting_to_table() first.', p_table_id, v_table_oid;
    END IF;
    
    -- Determine effective language
    v_effective_language := COALESCE(p_language, facets.bm25_get_language(p_table_id), 'english');
    
    PERFORM facets.bm25_index_document_native(v_table_oid, p_doc_id, p_content, v_effective_language);
END;
$$;

-- Delete a document from BM25 index
CREATE OR REPLACE FUNCTION facets.bm25_delete_document(
    p_table_id regclass,
    p_doc_id bigint
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
BEGIN
    v_table_oid := p_table_id::oid;
    
    PERFORM facets.bm25_delete_document_native(v_table_oid, p_doc_id);
END;
$$;

-- Search documents using BM25
-- This implementation uses SQL to find candidate documents and scores them with the native function
CREATE OR REPLACE FUNCTION facets.bm25_search(
    p_table_id regclass,
    p_query text,
    p_language text DEFAULT 'english',
    p_prefix_match boolean DEFAULT false,
    p_fuzzy_match boolean DEFAULT false,
    p_fuzzy_threshold float DEFAULT 0.3,
    p_k1 float DEFAULT 1.2,
    p_b float DEFAULT 0.75,
    p_limit int DEFAULT 10
) RETURNS TABLE(doc_id bigint, score float)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY SELECT * FROM facets.bm25_search_native(
        p_table_id::oid,
        p_query,
        p_language,
        p_prefix_match,
        p_fuzzy_match,
        p_fuzzy_threshold,
        p_k1,
        p_b,
        p_limit
    );
END;
$$;

-- Calculate BM25 score for a single document
CREATE OR REPLACE FUNCTION facets.bm25_score(
    p_table_id regclass,
    p_query text,
    p_doc_id bigint,
    p_language text DEFAULT 'english',
    p_k1 float DEFAULT 1.2,
    p_b float DEFAULT 0.75
) RETURNS float
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN facets.bm25_score_native(
        p_table_id::oid,
        p_query,
        p_doc_id,
        p_language,
        p_k1,
        p_b
    );
END;
$$;

-- Recalculate collection statistics
CREATE OR REPLACE FUNCTION facets.bm25_recalculate_statistics(
    p_table_id regclass
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_total_docs bigint;
    v_avg_length float;
BEGIN
    v_table_oid := p_table_id::oid;
    
    SELECT 
        COUNT(*)::bigint,
        COALESCE(AVG(doc_length), 0)::float
    INTO v_total_docs, v_avg_length
    FROM facets.bm25_documents
    WHERE table_id = v_table_oid;
    
    INSERT INTO facets.bm25_statistics (table_id, total_documents, avg_document_length)
    VALUES (v_table_oid, v_total_docs, v_avg_length)
    ON CONFLICT (table_id) DO UPDATE SET
        total_documents = EXCLUDED.total_documents,
        avg_document_length = EXCLUDED.avg_document_length,
        last_updated = now();
END;
$$;

-- Get collection statistics
CREATE OR REPLACE FUNCTION facets.bm25_get_statistics(
    p_table_id regclass
) RETURNS TABLE(total_docs bigint, avg_length float)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table_oid oid;
BEGIN
    v_table_oid := p_table_id::oid;
    
    RETURN QUERY
    SELECT 
        s.total_documents,
        s.avg_document_length
    FROM facets.bm25_statistics s
    WHERE s.table_id = v_table_oid;
END;
$$;

-- Native function to recalculate statistics from scratch
CREATE OR REPLACE FUNCTION facets.bm25_recalculate_statistics_native(
    table_id oid
) RETURNS void
AS '$libdir/pg_facets', 'bm25_recalculate_statistics_native'
LANGUAGE C STRICT;

-- Recalculate BM25 statistics from scratch (useful after batch operations)
CREATE OR REPLACE FUNCTION facets.bm25_recalculate_statistics(
    p_table_id regclass
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
BEGIN
    v_table_oid := p_table_id::oid;
    PERFORM facets.bm25_recalculate_statistics_native(v_table_oid);
END;
$$;

-- Set BM25 language for a table
-- This language will be used by default for text search if not specified explicitly
CREATE OR REPLACE FUNCTION facets.bm25_set_language(
    p_table_id regclass,
    p_language text DEFAULT 'english'
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
BEGIN
    v_table_oid := p_table_id::oid;
    
    -- Validate language by checking if it's a valid text search configuration
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_ts_config 
        WHERE cfgname = p_language
    ) THEN
        RAISE WARNING 'Language "%" is not a valid PostgreSQL text search configuration. Common options: english, french, german, spanish, simple', p_language;
    END IF;
    
    UPDATE facets.faceted_table 
    SET bm25_language = p_language 
    WHERE table_id = v_table_oid;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % (oid: %) is not registered in facets.faceted_table', p_table_id, v_table_oid;
    END IF;
END;
$$;

-- Get BM25 language for a table
CREATE OR REPLACE FUNCTION facets.bm25_get_language(
    p_table_id regclass
) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table_oid oid;
    v_language text;
BEGIN
    v_table_oid := p_table_id::oid;
    
    SELECT bm25_language INTO v_language
    FROM facets.faceted_table 
    WHERE table_id = v_table_oid;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % (oid: %) is not registered in facets.faceted_table', p_table_id, v_table_oid;
    END IF;
    
    RETURN COALESCE(v_language, 'english');
END;
$$;

-- Batch index multiple documents (more efficient than single document indexing)
-- Defers statistics update until the end for better performance
CREATE OR REPLACE FUNCTION facets.bm25_index_documents_batch(
    p_table_id regclass,
    p_documents jsonb,  -- Array of objects with doc_id and content keys
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT NULL,  -- NULL = use table's bm25_language
    p_batch_size int DEFAULT 1000
) RETURNS TABLE(indexed_count int, elapsed_ms float)
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_start_time timestamptz;
    v_doc jsonb;
    v_count int := 0;
    v_batch_count int := 0;
    v_effective_language text;
BEGIN
    v_table_oid := p_table_id::oid;
    v_start_time := clock_timestamp();
    
    -- Check if table is registered
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table % (oid: %) is not registered in facets.faceted_table. Run facets.add_faceting_to_table() first.', p_table_id, v_table_oid;
    END IF;
    
    -- Determine effective language
    v_effective_language := COALESCE(p_language, facets.bm25_get_language(p_table_id), 'english');
    
    -- Process each document
    FOR v_doc IN SELECT * FROM jsonb_array_elements(p_documents)
    LOOP
        -- Index the document using effective language (uses incremental stats internally now)
        PERFORM facets.bm25_index_document_native(
            v_table_oid,
            (v_doc->>'doc_id')::bigint,
            v_doc->>'content',
            v_effective_language
        );
        
        v_count := v_count + 1;
        v_batch_count := v_batch_count + 1;
        
        -- Recalculate statistics every batch_size documents for accuracy
        IF v_batch_count >= p_batch_size THEN
            PERFORM facets.bm25_recalculate_statistics(p_table_id);
            v_batch_count := 0;
        END IF;
    END LOOP;
    
    -- Final statistics recalculation for accuracy
    IF v_count > 0 THEN
        PERFORM facets.bm25_recalculate_statistics(p_table_id);
    END IF;
    
    indexed_count := v_count;
    elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    RETURN NEXT;
END;
$$;

-- Index a range of documents (used by parallel workers)
-- p_worker_id is 1-based, specifies which partition of documents to process
CREATE OR REPLACE FUNCTION facets.bm25_index_worker(
    p_table_id regclass,
    p_source_query text,  -- Query that returns (doc_id bigint, content text)
    p_content_column text,
    p_language text,
    p_total_docs bigint,
    p_num_workers int,
    p_worker_id int  -- 1-based worker ID
) RETURNS TABLE(docs_indexed int, elapsed_ms float)
LANGUAGE plpgsql AS $$
DECLARE
    v_docs_per_worker bigint;
    v_start_offset bigint;
    v_limit bigint;
    v_doc record;
    v_count int := 0;
    v_start_time timestamptz;
    v_query text;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Calculate range for this worker
    v_docs_per_worker := CEIL(p_total_docs::float / p_num_workers);
    v_start_offset := (p_worker_id - 1) * v_docs_per_worker;
    v_limit := LEAST(v_docs_per_worker, p_total_docs - v_start_offset);
    
    -- Skip if no documents for this worker
    IF v_start_offset >= p_total_docs OR v_limit <= 0 THEN
        docs_indexed := 0;
        elapsed_ms := 0;
        RETURN NEXT;
        RETURN;
    END IF;
    
    -- Build and execute query for this worker's partition
    v_query := format(
        'SELECT doc_id, content FROM (%s) AS src ORDER BY doc_id OFFSET %s LIMIT %s',
        p_source_query, v_start_offset, v_limit
    );
    
    FOR v_doc IN EXECUTE v_query
    LOOP
        PERFORM facets.bm25_index_document(
            p_table_id,
            v_doc.doc_id,
            v_doc.content,
            p_content_column,
            p_language
        );
        v_count := v_count + 1;
    END LOOP;
    
    docs_indexed := v_count;
    elapsed_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- SECTION: LOCK-FREE PARALLEL BM25 INDEXING
-- Optimized parallel indexing using per-worker staging tables (no lock contention)
-- 90-95% faster than the old OFFSET-based approach for large datasets
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

-- Native Zig worker function (much faster - uses direct C API calls, no SPI overhead)
-- This replaces bm25_index_worker_lockfree for better performance in parallel indexing
CREATE OR REPLACE FUNCTION facets.bm25_index_worker_native(
    p_table_id oid,
    p_source_staging text,  -- Source document staging table (facets schema)
    p_output_staging text,  -- Output term staging table for THIS worker (facets schema)
    p_language text,
    p_total_docs bigint,
    p_num_workers int,
    p_worker_id int  -- 1-based worker ID
) RETURNS TABLE(docs_indexed int, terms_extracted bigint, elapsed_ms float)
LANGUAGE c STRICT PARALLEL SAFE
AS '$libdir/pg_facets', 'bm25_index_worker_native';

-- Native Zig tokenizer function (production use)
-- Uses the same tokenization logic as bm25_index_worker_native
-- This is the recommended way to tokenize text for BM25 indexing
CREATE OR REPLACE FUNCTION facets.tokenize_native(
    p_text text,
    p_config_name text
)
RETURNS TABLE(lexeme text, freq integer)
LANGUAGE c STRICT
AS '$libdir/pg_facets', 'test_tokenize_only';

-- Alias for backward compatibility (test function)
CREATE OR REPLACE FUNCTION facets.test_tokenize_only(
    p_text text,
    p_config_name text
)
RETURNS TABLE(lexeme text, freq integer)
LANGUAGE c STRICT
AS '$libdir/pg_facets', 'test_tokenize_only';

-- BM25 Debug/Analysis Functions

-- Get top N terms by frequency
CREATE OR REPLACE FUNCTION facets.bm25_term_stats(
    p_table_id oid,
    p_limit int DEFAULT 100
)
RETURNS TABLE(term_text text, ndoc bigint, nentry bigint)
LANGUAGE c STABLE
AS '$libdir/pg_facets', 'bm25_term_stats';

COMMENT ON FUNCTION facets.bm25_term_stats IS 
'Returns top N terms by frequency for BM25 indexes.
 - term_text: the lexeme
 - ndoc: number of documents containing this term
 - nentry: total occurrences across all documents';

-- Get top N documents by length
CREATE OR REPLACE FUNCTION facets.bm25_doc_stats(
    p_table_id oid,
    p_limit int DEFAULT 100
)
RETURNS TABLE(doc_id bigint, doc_length int, unique_terms int)
LANGUAGE c STABLE
AS '$libdir/pg_facets', 'bm25_doc_stats';

COMMENT ON FUNCTION facets.bm25_doc_stats IS 
'Returns top N documents sorted by document length (descending).
 - doc_id: document identifier
 - doc_length: total terms in document
 - unique_terms: number of distinct terms (placeholder, currently 0)';

-- Get collection-wide statistics
CREATE OR REPLACE FUNCTION facets.bm25_collection_stats(
    p_table_id oid
)
RETURNS TABLE(total_documents bigint, avg_document_length float8, total_terms bigint, unique_terms bigint)
LANGUAGE c STABLE
AS '$libdir/pg_facets', 'bm25_collection_stats';

COMMENT ON FUNCTION facets.bm25_collection_stats IS 
'Returns overall collection statistics for a BM25 index.
 - total_documents: number of indexed documents
 - avg_document_length: average document length
 - total_terms: total term occurrences (placeholder)
 - unique_terms: number of distinct terms in the collection';

-- Explain BM25 weights for a specific document
CREATE OR REPLACE FUNCTION facets.bm25_explain_doc(
    p_table_id oid,
    p_doc_id bigint,
    p_k1 float DEFAULT 1.2,
    p_b float DEFAULT 0.75
)
RETURNS TABLE(term_text text, tf int, df bigint, idf float8, bm25_weight float8)
LANGUAGE c STABLE
AS '$libdir/pg_facets', 'bm25_explain_doc';

COMMENT ON FUNCTION facets.bm25_explain_doc IS 
'Explains the BM25 weight contribution of each term in a document.
 Useful for debugging why a document ranks high/low for certain queries.
 - term_text: the lexeme
 - tf: term frequency (occurrences in this document)
 - df: document frequency (documents containing this term)
 - idf: inverse document frequency = log((N+1)/(df+0.5))
 - bm25_weight: actual BM25 contribution for this term';

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

-- OPTIMIZED parallel batch indexing using lock-free staging tables
-- This function spawns parallel workers using dblink with ROW_NUMBER partitioning
-- 90-95% faster than the old OFFSET-based approach
-- Requires: CREATE EXTENSION IF NOT EXISTS dblink;
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
        
        -- Build worker query using the native Zig worker function (much faster)
        -- Falls back to SQL worker if native function is not available
        v_worker_query := format(
            'SELECT * FROM facets.bm25_index_worker_native(%s, %L, %L, %L, %s, %s, %s)',
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

-- Simple parallel indexing helper that works without dblink
-- Uses generate_series to partition work across multiple calls
CREATE OR REPLACE FUNCTION facets.bm25_get_worker_range(
    p_total_docs bigint,
    p_num_workers int,
    p_worker_id int  -- 1-based
) RETURNS TABLE(start_offset bigint, end_offset bigint, doc_count bigint)
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    v_docs_per_worker bigint;
BEGIN
    v_docs_per_worker := CEIL(p_total_docs::float / p_num_workers);
    start_offset := (p_worker_id - 1) * v_docs_per_worker;
    end_offset := LEAST(p_worker_id * v_docs_per_worker, p_total_docs);
    doc_count := GREATEST(0, end_offset - start_offset);
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- SECTION: BM25 HELPER FUNCTIONS
-- Diagnostic and utility functions for BM25 indexing
-- ============================================================================

-- Helper function to check if a table is registered for BM25
CREATE OR REPLACE FUNCTION facets.bm25_is_table_registered(
    p_table_id regclass
) RETURNS boolean
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table_oid oid;
    v_registered boolean;
BEGIN
    v_table_oid := p_table_id::oid;
    SELECT EXISTS (
        SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid
    ) INTO v_registered;
    RETURN v_registered;
END;
$$;

-- Helper function to get BM25 index statistics for a table
CREATE OR REPLACE FUNCTION facets.bm25_get_index_stats(
    p_table_id regclass
) RETURNS TABLE(
    documents_indexed bigint,
    terms_indexed bigint,
    statistics_records int,
    total_documents bigint,
    avg_document_length float
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table_oid oid;
BEGIN
    v_table_oid := p_table_id::oid;
    
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM facets.bm25_documents WHERE table_id = v_table_oid)::bigint,
        (SELECT COUNT(*) FROM facets.bm25_index WHERE table_id = v_table_oid)::bigint,
        (SELECT COUNT(*) FROM facets.bm25_statistics WHERE table_id = v_table_oid)::int,
        COALESCE((SELECT total_documents FROM facets.bm25_statistics WHERE table_id = v_table_oid), 0)::bigint,
        COALESCE((SELECT avg_document_length FROM facets.bm25_statistics WHERE table_id = v_table_oid), 0.0)::float;
END;
$$;

-- ============================================================================
-- SECTION: BM25 SYNC TRIGGER HELPERS
-- Automatically create/drop triggers to keep BM25 index in sync with table changes
-- ============================================================================

-- Create a trigger to keep BM25 index in sync with table changes
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
                PERFORM facets.bm25_delete_document(v_table, (OLD.%I)::bigint);
                RETURN OLD;
            ELSIF TG_OP = 'UPDATE' THEN
                PERFORM facets.bm25_delete_document(v_table, (OLD.%I)::bigint);
                IF NEW.%I IS NOT NULL AND NEW.%I <> '' THEN
                    PERFORM facets.bm25_index_document(v_table, (NEW.%I)::bigint, NEW.%I, %L, %L);
                END IF;
                RETURN NEW;
            ELSE
                IF NEW.%I IS NOT NULL AND NEW.%I <> '' THEN
                    PERFORM facets.bm25_index_document(v_table, (NEW.%I)::bigint, NEW.%I, %L, %L);
                END IF;
                RETURN NEW;
            END IF;
        END;
        $trigger$
    $func$,
        v_trigger_func_name,
        p_id_column,
        p_id_column,
        p_content_column, p_content_column,
        p_id_column, p_content_column, p_content_column, v_effective_language,
        p_content_column, p_content_column,
        p_id_column, p_content_column, p_content_column, v_effective_language
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
-- SECTION: GENERIC BM25 REBUILD FUNCTION
-- Rebuild BM25 index for any registered table
-- ============================================================================

CREATE OR REPLACE FUNCTION facets.bm25_rebuild_index(
    p_table regclass,
    p_id_column text DEFAULT 'id',
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT 'english',
    p_num_workers int DEFAULT 0,
    p_connection_string text DEFAULT NULL,
    p_progress_step_size int DEFAULT 50000
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
    v_source_query text;
    v_total_docs bigint;
    v_has_dblink boolean;
    v_workers int;
    v_result record;
    v_doc record;
    v_count bigint := 0;
    v_batch_count bigint := 0;
    v_start_time timestamptz;
    v_elapsed_seconds numeric;
    v_estimated_remaining numeric;
    v_docs_per_second numeric;
    v_indexed_docs bigint;
    v_indexed_terms bigint;
    v_conn_string text;
BEGIN
    v_table_oid := p_table::oid;
    v_start_time := clock_timestamp();

    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table % is not registered in facets.faceted_table. Run facets.add_faceting_to_table() first.', p_table;
    END IF;

    RAISE NOTICE '[BM25 REBUILD] %: Starting rebuild at %', p_table::text, v_start_time;

    DELETE FROM facets.bm25_index WHERE table_id = v_table_oid;
    DELETE FROM facets.bm25_documents WHERE table_id = v_table_oid;
    DELETE FROM facets.bm25_statistics WHERE table_id = v_table_oid;

    v_source_query := format(
        'SELECT (%1$I)::bigint AS doc_id, %2$I AS content FROM %3$s WHERE %2$I IS NOT NULL AND %2$I <> '''' ORDER BY (%1$I)::bigint',
        p_id_column, p_content_column, p_table
    );

    EXECUTE format('SELECT COUNT(*) FROM (%s) AS src', v_source_query) INTO v_total_docs;
    IF v_total_docs = 0 THEN
        RAISE NOTICE '[BM25 REBUILD] %: no documents to index', p_table::text;
        RETURN;
    END IF;

    RAISE NOTICE '[BM25 REBUILD] %: Found % documents to index', p_table::text, v_total_docs;

    v_has_dblink := EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink');

    IF p_num_workers = 0 THEN
        v_workers := CASE WHEN v_has_dblink THEN 4 ELSE 1 END;
    ELSE
        v_workers := GREATEST(1, p_num_workers);
    END IF;

    IF v_workers > 1 AND v_has_dblink THEN
        RAISE NOTICE '[BM25 REBUILD] %: starting parallel rebuild with % workers', p_table::text, v_workers;
        
        IF p_connection_string IS NULL THEN
            v_conn_string := format('dbname=%s user=%s', current_database(), current_user);
        ELSE
            v_conn_string := p_connection_string;
        END IF;
        
        FOR v_result IN
            SELECT * FROM facets.bm25_index_documents_parallel(
                p_table, v_source_query, p_content_column, p_language, v_workers, v_conn_string
            )
        LOOP
            RAISE NOTICE '[BM25 REBUILD] %: worker % -> % docs in % ms (%s)',
                p_table::text, v_result.worker_id, v_result.docs_indexed,
                round(v_result.elapsed_ms::numeric, 1), v_result.status;
        END LOOP;
    ELSE
        RAISE NOTICE '[BM25 REBUILD] %: starting sequential rebuild (% documents)', p_table::text, v_total_docs;

        FOR v_doc IN EXECUTE v_source_query
        LOOP
            PERFORM facets.bm25_index_document(p_table, v_doc.doc_id, v_doc.content, p_content_column, p_language);
            
            v_count := v_count + 1;
            v_batch_count := v_batch_count + 1;
            
            IF v_batch_count >= p_progress_step_size THEN
                v_elapsed_seconds := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
                v_docs_per_second := v_count / NULLIF(v_elapsed_seconds, 0);
                
                IF v_count < v_total_docs AND v_docs_per_second > 0 THEN
                    v_estimated_remaining := (v_total_docs - v_count) / v_docs_per_second;
                    RAISE NOTICE '[BM25 REBUILD] %: Progress: % / % (%.1f%%) - ETA: % min',
                        p_table::text, v_count, v_total_docs,
                        (v_count::numeric / v_total_docs * 100),
                        round((v_estimated_remaining / 60)::numeric, 1);
                END IF;
                
                v_batch_count := 0;
            END IF;
        END LOOP;

        PERFORM facets.bm25_recalculate_statistics(p_table);
    END IF;

    SELECT COUNT(*) INTO v_indexed_docs FROM facets.bm25_documents WHERE table_id = v_table_oid;
    SELECT COUNT(*) INTO v_indexed_terms FROM facets.bm25_index WHERE table_id = v_table_oid;
    
    v_elapsed_seconds := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    RAISE NOTICE '[BM25 REBUILD] %: Complete! % docs, % terms in % seconds',
        p_table::text, v_indexed_docs, v_indexed_terms, round(v_elapsed_seconds::numeric, 1);
END;
$$;

-- ============================================================================
-- SECTION: BM25 MONITORING AND CLEANUP FUNCTIONS
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
    LEFT JOIN (SELECT table_id, COUNT(*) as doc_count FROM facets.bm25_documents GROUP BY table_id) d ON s.table_id = d.table_id
    LEFT JOIN (SELECT table_id, COUNT(DISTINCT term_hash) as term_count FROM facets.bm25_index GROUP BY table_id) i ON s.table_id = i.table_id
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

-- Quick progress check
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
        RETURN QUERY
        SELECT 
            ft.table_id::regclass::text as table_name,
            COALESCE(d.cnt, 0)::bigint as documents_indexed,
            NULL::bigint as source_documents,
            NULL::numeric as progress_pct,
            COALESCE(i.cnt, 0)::bigint as unique_terms
        FROM facets.faceted_table ft
        LEFT JOIN (SELECT table_id, COUNT(*) as cnt FROM facets.bm25_documents GROUP BY table_id) d ON ft.table_id = d.table_id
        LEFT JOIN (SELECT table_id, COUNT(DISTINCT term_hash) as cnt FROM facets.bm25_index GROUP BY table_id) i ON ft.table_id = i.table_id
        ORDER BY ft.table_id;
    ELSE
        v_table_oid := p_table::oid;
        
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
            CASE WHEN v_source_count > 0 THEN 
                round((COALESCE((SELECT COUNT(*) FROM facets.bm25_documents WHERE table_id = v_table_oid), 0)::numeric / v_source_count * 100), 2)
            ELSE NULL END as progress_pct,
            COALESCE((SELECT COUNT(DISTINCT term_hash) FROM facets.bm25_index WHERE table_id = v_table_oid), 0) as unique_terms;
    END IF;
END;
$$;

-- Check active BM25-related processes
CREATE OR REPLACE FUNCTION facets.bm25_active_processes()
RETURNS TABLE(pid int, state text, duration interval, wait_event text, operation_type text, query_preview text)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pa.pid::int, pa.state::text, (now() - pa.query_start)::interval as duration,
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
    WHERE (pa.query ILIKE '%bm25%' OR pa.query ILIKE '%dblink%' OR pa.query ILIKE '%staging%')
    AND pa.pid != pg_backend_pid() AND pa.state != 'idle'
    ORDER BY pa.query_start;
    
    IF NOT FOUND THEN
        pid := NULL; state := 'No active BM25 processes'; duration := NULL;
        wait_event := NULL; operation_type := NULL; query_preview := NULL;
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
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') THEN
        connection_name := NULL; status := 'dblink extension not installed';
        RETURN NEXT; RETURN;
    END IF;
    
    SELECT array_agg(unnest) INTO v_conns FROM (SELECT unnest(dblink_get_connections())) AS t(unnest);
    
    IF v_conns IS NULL OR array_length(v_conns, 1) IS NULL THEN
        connection_name := NULL; status := 'No dblink connections found';
        RETURN NEXT; RETURN;
    END IF;
    
    FOREACH v_conn IN ARRAY v_conns LOOP
        BEGIN
            PERFORM dblink_disconnect(v_conn);
            connection_name := v_conn; status := 'disconnected'; RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            connection_name := v_conn; status := 'error: ' || SQLERRM; RETURN NEXT;
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
    FOR v_table IN 
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'facets' AND (tablename LIKE 'bm25_staging%' OR tablename LIKE 'bm25_src_%' OR tablename LIKE 'bm25_w%')
    LOOP
        v_found := true;
        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_table);
            table_name := 'facets.' || v_table; status := 'dropped'; RETURN NEXT;
        EXCEPTION WHEN OTHERS THEN
            table_name := 'facets.' || v_table; status := 'error: ' || SQLERRM; RETURN NEXT;
        END;
    END LOOP;
    
    IF NOT v_found THEN
        table_name := NULL; status := 'No staging tables found'; RETURN NEXT;
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
        SELECT pa.pid FROM pg_stat_activity pa
        WHERE (pa.query ILIKE '%bm25%' OR pa.query ILIKE '%rebuild%' OR pa.query ILIKE '%staging%')
        AND pa.state IN ('active', 'idle', 'idle in transaction')
        AND pa.pid != pg_backend_pid() AND now() - pa.query_start > p_min_duration
    LOOP
        BEGIN
            pid := v_pid;
            SELECT now() - query_start INTO duration FROM pg_stat_activity WHERE pg_stat_activity.pid = v_pid;
            PERFORM pg_terminate_backend(v_pid);
            status := 'terminated'; RETURN NEXT; v_killed := v_killed + 1;
        EXCEPTION WHEN OTHERS THEN
            status := 'error: ' || SQLERRM; RETURN NEXT;
        END;
    END LOOP;
    
    IF v_killed = 0 THEN
        pid := NULL; duration := NULL;
        status := format('No stuck processes found (threshold: %s)', p_min_duration);
        RETURN NEXT;
    END IF;
END;
$$;

-- Full cleanup
CREATE OR REPLACE FUNCTION facets.bm25_full_cleanup(p_kill_threshold interval DEFAULT '5 minutes')
RETURNS TABLE(operation text, details text)
LANGUAGE plpgsql AS $$
DECLARE
    v_rec record;
    v_results text[];
BEGIN
    operation := 'Disconnect dblinks'; v_results := ARRAY[]::text[];
    FOR v_rec IN SELECT * FROM facets.bm25_cleanup_dblinks() LOOP
        v_results := array_append(v_results, COALESCE(v_rec.connection_name || ': ', '') || v_rec.status);
    END LOOP;
    details := array_to_string(v_results, ', '); RETURN NEXT;
    
    operation := 'Drop staging tables'; v_results := ARRAY[]::text[];
    FOR v_rec IN SELECT * FROM facets.bm25_cleanup_staging() LOOP
        v_results := array_append(v_results, COALESCE(v_rec.table_name || ': ', '') || v_rec.status);
    END LOOP;
    details := array_to_string(v_results, ', '); RETURN NEXT;
    
    operation := 'Kill stuck processes'; v_results := ARRAY[]::text[];
    FOR v_rec IN SELECT * FROM facets.bm25_kill_stuck(p_kill_threshold) LOOP
        v_results := array_append(v_results, COALESCE('pid ' || v_rec.pid::text || ': ', '') || v_rec.status);
    END LOOP;
    details := array_to_string(v_results, ', '); RETURN NEXT;
    
    operation := 'Current status';
    SELECT string_agg(s.table_name || ': ' || s.documents_indexed || ' docs', ', ') INTO details
    FROM facets.bm25_status() s WHERE s.table_name != 'No BM25 indexes found';
    IF details IS NULL THEN details := 'No BM25 indexes'; END IF;
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- SECTION: SIMPLIFIED SETUP FUNCTION
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
    p_bm25_workers int DEFAULT 0
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_effective_facets facets.facet_definition[];
BEGIN
    RAISE NOTICE '[SETUP] Starting setup for table %', p_table;
    
    v_effective_facets := COALESCE(p_facets, ARRAY[]::facets.facet_definition[]);
    
    RAISE NOTICE '[SETUP] Adding faceting to table...';
    PERFORM facets.add_faceting_to_table(
        p_table, p_id_column::name, v_effective_facets, p_chunk_bits, true, p_populate_facets
    );
    
    RAISE NOTICE '[SETUP] Setting BM25 language to %', p_language;
    PERFORM facets.bm25_set_language(p_table, p_language);
    
    IF p_create_trigger THEN
        RAISE NOTICE '[SETUP] Creating BM25 sync trigger...';
        PERFORM facets.bm25_create_sync_trigger(p_table, p_id_column, p_content_column, p_language);
    END IF;
    
    IF p_build_bm25_index THEN
        RAISE NOTICE '[SETUP] Building BM25 index...';
        PERFORM facets.bm25_rebuild_index(p_table, p_id_column, p_content_column, p_language, p_bm25_workers);
    END IF;
    
    RAISE NOTICE '[SETUP] Setup complete for %', p_table;
END;
$$;

-- Log version activation on extension creation
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    RAISE NOTICE 'pg_facets extension version % activated', v_version;
END $$;

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
    IF array_length(v_facet_defs, 1) > 0 THEN
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
    END IF;

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

-- Safe wrapper for bm25_index_document with validation
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
    v_exists_after boolean;
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

    -- Verify atomicity: document should exist after indexing
    SELECT EXISTS(
        SELECT 1 FROM facets.bm25_documents
        WHERE table_id = p_table_id::oid AND doc_id = p_doc_id
    ) INTO v_exists_after;

    IF NOT v_exists_after THEN
        RAISE EXCEPTION 'bm25_index_document_safe: Atomicity violation - document % was not indexed', p_doc_id;
    END IF;
END;
$$;

-- Safe wrapper for bm25_delete_document with validation
CREATE OR REPLACE FUNCTION facets.bm25_delete_document_safe(
    p_table regclass,
    p_doc_id bigint
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_exists_before boolean;
    v_exists_after boolean;
BEGIN
    -- Check if document exists before deletion
    SELECT EXISTS(
        SELECT 1 FROM facets.bm25_documents
        WHERE table_id = p_table::oid AND doc_id = p_doc_id
    ) INTO v_exists_before;

    -- Call the actual delete function
    PERFORM facets.bm25_delete_document(p_table, p_doc_id);

    -- Verify atomicity: if document existed before, it should be gone now
    SELECT EXISTS(
        SELECT 1 FROM facets.bm25_documents
        WHERE table_id = p_table::oid AND doc_id = p_doc_id
    ) INTO v_exists_after;

    IF v_exists_before AND v_exists_after THEN
        RAISE EXCEPTION 'bm25_delete_document_safe: Atomicity violation - document % still exists after deletion', p_doc_id;
    END IF;
END;
$$;

-- Safe wrapper for merge_deltas with validation
CREATE OR REPLACE FUNCTION facets.merge_deltas_safe(
    p_table regclass
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_result int;
BEGIN
    -- Call the optimized native Zig implementation
    SELECT merge_deltas_native(p_table::oid) INTO v_result;

    -- Check if merge completed successfully (0 = success)
    IF v_result != 0 THEN
        RAISE EXCEPTION 'merge_deltas_safe: Native merge function failed with code %', v_result;
    END IF;
EXCEPTION WHEN OTHERS THEN
    -- Re-raise with context
    RAISE EXCEPTION 'merge_deltas_safe: Merge failed: %', SQLERRM;
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


-- =============================================================================
-- 7. HELPER FUNCTION TO LIST ALL FACETS FOR A TABLE
-- =============================================================================

-- Function to get all facet definitions for a table
-- Returns all facets (T0 + business facets) that can be used for filtering/searching
CREATE OR REPLACE FUNCTION facets.list_table_facets(
    p_table regclass
)
RETURNS TABLE(
    facet_id int,
    facet_name text,
    facet_type text,
    base_column name,
    params jsonb,
    is_multi boolean,
    supports_delta boolean
) 
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
    SELECT 
        fd.facet_id,
        fd.facet_name,
        fd.facet_type,
        fd.base_column,
        fd.params,
        fd.is_multi,
        fd.supports_delta
    FROM facets.facet_definition fd
    JOIN facets.faceted_table ft ON fd.table_id = ft.table_id
    WHERE ft.table_id = p_table::oid
    ORDER BY fd.facet_id;
END;
$$;

COMMENT ON FUNCTION facets.list_table_facets(regclass) IS
    'Returns all facet definitions for a table, including T0 facets (direct columns) and business facets (function-based)';

-- Convenience function to get facet names only (useful for API responses)
CREATE OR REPLACE FUNCTION facets.list_table_facet_names(
    p_table regclass
)
RETURNS text[]
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_facet_names text[];
BEGIN
    SELECT array_agg(fd.facet_name ORDER BY fd.facet_id)
    INTO v_facet_names
    FROM facets.facet_definition fd
    JOIN facets.faceted_table ft ON fd.table_id = ft.table_id
    WHERE ft.table_id = p_table::oid;
    
    RETURN COALESCE(v_facet_names, ARRAY[]::text[]);
END;
$$;

COMMENT ON FUNCTION facets.list_table_facet_names(regclass) IS
    'Returns an array of all facet names for a table, useful for API endpoints';

-- Function to get facet names with their types as JSONB (most convenient for APIs)
-- Note: JSON object key order is not guaranteed; use list_table_facets_for_ui for ordered array format
CREATE OR REPLACE FUNCTION facets.list_table_facets_with_types(
    p_table regclass
)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_result jsonb;
BEGIN
    SELECT jsonb_object_agg(
        fd.facet_name,
        jsonb_build_object(
            'facet_id', fd.facet_id,
            'facet_type', fd.facet_type,
            'base_column', fd.base_column,
            'is_multi', fd.is_multi,
            'params', fd.params
        )
    )
    INTO v_result
    FROM facets.facet_definition fd
    JOIN facets.faceted_table ft ON fd.table_id = ft.table_id
    WHERE ft.table_id = p_table::oid;
    
    RETURN COALESCE(v_result, '{}'::jsonb);
END;
$$;

COMMENT ON FUNCTION facets.list_table_facets_with_types(regclass) IS
    'Returns all facets as a JSONB object with facet names as keys and metadata (type, base_column, etc.) as values. Note: JSON object key order is not guaranteed in most languages.';

-- Function to get facets as a simple table with name and type
CREATE OR REPLACE FUNCTION facets.list_table_facets_simple(
    p_table regclass
)
RETURNS TABLE(
    facet_name text,
    facet_type text
)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
    SELECT 
        fd.facet_name,
        fd.facet_type
    FROM facets.facet_definition fd
    JOIN facets.faceted_table ft ON fd.table_id = ft.table_id
    WHERE ft.table_id = p_table::oid
    ORDER BY fd.facet_id;
END;
$$;

COMMENT ON FUNCTION facets.list_table_facets_simple(regclass) IS
    'Returns a simple table with facet names and types only';

-- =============================================================================
-- 8. TABLE-LEVEL INTROSPECTION FUNCTIONS
-- =============================================================================

-- Function to get table-level metadata for a faceted table
CREATE OR REPLACE FUNCTION facets.describe_table(p_table regclass)
RETURNS TABLE(
    table_id oid,
    schemaname text,
    tablename text,
    key_column name,
    key_type text,
    chunk_bits int,
    bm25_language text,
    has_bm25_index boolean,
    has_delta_table boolean,
    facet_count int
) LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ft.table_id,
        ft.schemaname,
        ft.tablename,
        ft.key,
        ft.key_type,
        ft.chunk_bits,
        ft.bm25_language,
        EXISTS(
            SELECT 1 FROM facets.bm25_documents bd 
            WHERE bd.table_id = ft.table_id 
            LIMIT 1
        ) AS has_bm25_index,
        (ft.delta_table IS NOT NULL) AS has_delta_table,
        (SELECT COUNT(*)::int FROM facets.facet_definition fd WHERE fd.table_id = ft.table_id) AS facet_count
    FROM facets.faceted_table ft
    WHERE ft.table_id = p_table::oid;
END;
$$;

COMMENT ON FUNCTION facets.describe_table(regclass) IS
    'Returns table-level metadata including key column, BM25 language, and whether BM25/deltas are configured. Essential for frontend configuration.';

-- Function to list all registered faceted tables
CREATE OR REPLACE FUNCTION facets.list_tables()
RETURNS TABLE(
    table_id oid,
    qualified_name text,
    schemaname text,
    tablename text,
    facet_count int,
    has_bm25 boolean,
    has_delta boolean,
    bm25_language text
) LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ft.table_id,
        format('%I.%I', ft.schemaname, ft.tablename) AS qualified_name,
        ft.schemaname,
        ft.tablename,
        (SELECT COUNT(*)::int FROM facets.facet_definition fd WHERE fd.table_id = ft.table_id) AS facet_count,
        EXISTS(
            SELECT 1 FROM facets.bm25_documents bd 
            WHERE bd.table_id = ft.table_id 
            LIMIT 1
        ) AS has_bm25,
        (ft.delta_table IS NOT NULL) AS has_delta,
        ft.bm25_language
    FROM facets.faceted_table ft
    ORDER BY ft.schemaname, ft.tablename;
END;
$$;

COMMENT ON FUNCTION facets.list_tables() IS
    'Lists all registered faceted tables with their configuration summary. Useful for multi-table management UIs.';

-- Function to get hierarchical facet relationships
-- Exposes parent-child relationships for tree UI components
CREATE OR REPLACE FUNCTION facets.get_facet_hierarchy(p_table regclass)
RETURNS TABLE(
    facet_name text,
    facet_type text,
    parent_facet text,
    is_root boolean,
    is_hierarchical boolean,
    depth int
) LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE hierarchy AS (
        -- Base case: root facets (have no parent OR are parents themselves)
        SELECT 
            fd.facet_name,
            fd.facet_type,
            (fd.params->>'parent_facet')::text AS parent_facet,
            (fd.params->>'parent_facet' IS NULL AND EXISTS(
                SELECT 1 FROM facets.facet_definition fd2 
                WHERE fd2.table_id = p_table::oid 
                AND fd2.params->>'parent_facet' = fd.facet_name
            )) AS is_root,
            (fd.params->>'parent_facet' IS NOT NULL OR EXISTS(
                SELECT 1 FROM facets.facet_definition fd2 
                WHERE fd2.table_id = p_table::oid 
                AND fd2.params->>'parent_facet' = fd.facet_name
            )) AS is_hierarchical,
            CASE 
                WHEN fd.params->>'parent_facet' IS NULL THEN 0
                ELSE 1
            END AS depth
        FROM facets.facet_definition fd
        WHERE fd.table_id = p_table::oid
        
        UNION ALL
        
        -- Recursive case: find deeper children
        SELECT 
            fd.facet_name,
            fd.facet_type,
            (fd.params->>'parent_facet')::text,
            false AS is_root,
            true AS is_hierarchical,
            h.depth + 1
        FROM facets.facet_definition fd
        JOIN hierarchy h ON fd.params->>'parent_facet' = h.facet_name
        WHERE fd.table_id = p_table::oid
        AND h.depth < 10  -- Prevent infinite recursion
    )
    SELECT DISTINCT ON (hierarchy.facet_name)
        hierarchy.facet_name,
        hierarchy.facet_type,
        hierarchy.parent_facet,
        hierarchy.is_root,
        hierarchy.is_hierarchical,
        hierarchy.depth
    FROM hierarchy
    ORDER BY hierarchy.facet_name, hierarchy.depth DESC;
END;
$$;

COMMENT ON FUNCTION facets.get_facet_hierarchy(regclass) IS
    'Returns facet hierarchy information including parent-child relationships. Use for building tree UI components.';

-- Function to get facets with UI component hints
-- Returns a JSON array with ordering preserved and UI component suggestions
CREATE OR REPLACE FUNCTION facets.list_table_facets_for_ui(p_table regclass)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_result jsonb;
BEGIN
    SELECT jsonb_agg(
        jsonb_build_object(
            'facet_id', fd.facet_id,
            'name', fd.facet_name,
            'type', fd.facet_type,
            'base_column', fd.base_column,
            'is_multi', fd.is_multi,
            'params', fd.params,
            'parent_facet', fd.params->>'parent_facet',
            'is_hierarchical', (
                fd.params->>'parent_facet' IS NOT NULL OR EXISTS(
                    SELECT 1 FROM facets.facet_definition fd2 
                    WHERE fd2.table_id = p_table::oid 
                    AND fd2.params->>'parent_facet' = fd.facet_name
                )
            ),
            'ui_component', CASE 
                -- Hierarchical facets
                WHEN fd.params->>'parent_facet' IS NOT NULL OR EXISTS(
                    SELECT 1 FROM facets.facet_definition fd2 
                    WHERE fd2.table_id = p_table::oid 
                    AND fd2.params->>'parent_facet' = fd.facet_name
                ) THEN 'tree'
                -- Boolean facets
                WHEN fd.facet_type = 'boolean' THEN 'checkbox'
                -- Bucket/range facets
                WHEN fd.facet_type = 'bucket' THEN 'range'
                -- Date truncation facets
                WHEN fd.facet_type = 'datetrunc' THEN 'datepicker'
                -- Rating facets
                WHEN fd.facet_type = 'rating' THEN 'rating'
                -- Array/multi-value facets
                WHEN fd.is_multi THEN 'multiselect'
                -- Default to dropdown for plain and others
                ELSE 'dropdown'
            END
        )
        ORDER BY fd.facet_id
    )
    INTO v_result
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table::oid;
    
    RETURN COALESCE(v_result, '[]'::jsonb);
END;
$$;

COMMENT ON FUNCTION facets.list_table_facets_for_ui(regclass) IS
    'Returns facets as a JSON array with UI component hints (dropdown, checkbox, tree, etc.). Array format preserves ordering.';

-- Combined introspection function - single API endpoint for frontend configuration
-- Returns everything needed to build a filter UI for a faceted table
CREATE OR REPLACE FUNCTION facets.introspect(p_table regclass)
RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table_info jsonb;
    v_facets jsonb;
    v_hierarchy jsonb;
    v_hierarchical_roots text[];
    v_regular_facets text[];
BEGIN
    -- Get table info
    SELECT jsonb_build_object(
        'table_id', ft.table_id,
        'schema', ft.schemaname,
        'name', ft.tablename,
        'qualified_name', format('%I.%I', ft.schemaname, ft.tablename),
        'key_column', ft.key,
        'key_type', ft.key_type,
        'chunk_bits', ft.chunk_bits,
        'bm25_language', ft.bm25_language,
        'has_bm25_index', EXISTS(
            SELECT 1 FROM facets.bm25_documents bd 
            WHERE bd.table_id = ft.table_id 
            LIMIT 1
        ),
        'has_delta_table', (ft.delta_table IS NOT NULL)
    )
    INTO v_table_info
    FROM facets.faceted_table ft
    WHERE ft.table_id = p_table::oid;
    
    IF v_table_info IS NULL THEN
        RAISE EXCEPTION 'Table % is not registered for faceting', p_table;
    END IF;
    
    -- Get facets with UI hints
    v_facets := facets.list_table_facets_for_ui(p_table);
    
    -- Get hierarchy information
    WITH hierarchy_info AS (
        SELECT 
            fd.facet_name,
            (fd.params->>'parent_facet')::text AS parent_facet
        FROM facets.facet_definition fd
        WHERE fd.table_id = p_table::oid
        AND fd.params->>'parent_facet' IS NOT NULL
    ),
    roots AS (
        -- Facets that are parents but not children
        SELECT DISTINCT parent_facet AS facet_name
        FROM hierarchy_info
        WHERE parent_facet NOT IN (SELECT facet_name FROM hierarchy_info)
    ),
    all_hierarchical AS (
        SELECT facet_name FROM hierarchy_info
        UNION
        SELECT parent_facet FROM hierarchy_info
    ),
    regular AS (
        SELECT fd.facet_name
        FROM facets.facet_definition fd
        WHERE fd.table_id = p_table::oid
        AND fd.facet_name NOT IN (SELECT facet_name FROM all_hierarchical WHERE facet_name IS NOT NULL)
    )
    SELECT 
        COALESCE(array_agg(DISTINCT r.facet_name), ARRAY[]::text[]),
        COALESCE((SELECT array_agg(facet_name) FROM regular), ARRAY[]::text[])
    INTO v_hierarchical_roots, v_regular_facets
    FROM roots r;
    
    -- Build hierarchy object
    SELECT jsonb_build_object(
        'roots', COALESCE(to_jsonb(v_hierarchical_roots), '[]'::jsonb),
        'regular_facets', COALESCE(to_jsonb(v_regular_facets), '[]'::jsonb),
        'relationships', COALESCE((
            SELECT jsonb_object_agg(
                fd.facet_name,
                jsonb_build_object(
                    'parent', fd.params->>'parent_facet',
                    'facet_type', fd.facet_type
                )
            )
            FROM facets.facet_definition fd
            WHERE fd.table_id = p_table::oid
            AND fd.params->>'parent_facet' IS NOT NULL
        ), '{}'::jsonb)
    )
    INTO v_hierarchy;
    
    -- Return combined result
    RETURN jsonb_build_object(
        'table', v_table_info,
        'facets', v_facets,
        'hierarchy', v_hierarchy,
        'facet_count', jsonb_array_length(v_facets)
    );
END;
$$;

COMMENT ON FUNCTION facets.introspect(regclass) IS
    'Returns complete introspection data for frontend configuration: table metadata, facets with UI hints, and hierarchy info. Single API endpoint for filter UI setup.';
