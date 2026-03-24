-- pg_facets 0.3.9
-- PostgreSQL extension for efficient faceted search using Roaring Bitmaps
-- High-performance faceting with Zig native functions
-- Performance optimizations: bitmap intersection instead of rb_contains() per-row checks

-- SECTION 1: CORE DEFINITIONS AND UTILITY FUNCTIONS

-- Extension initialization
CREATE SCHEMA IF NOT EXISTS facets;

-- Version information
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.3.9';
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
    chunk_bits int
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
    insert_clauses := CASE WHEN array_length(insert_values, 1) > 0 THEN array['VALUES ' || array_to_string(insert_values, E',\n                       ')] ELSE '{}'::text[] END || insert_subqueries;
    delete_clauses := CASE WHEN array_length(delete_values, 1) > 0 THEN array['VALUES ' || array_to_string(delete_values, E',\n                       ')] ELSE '{}'::text[] END || delete_subqueries;

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
    AFTER INSERT OR UPDATE OF %s ON %s
    FOR EACH ROW EXECUTE FUNCTION %s();
$sql$,
        facets._qualified(tdef.schemaname, tfunc_name),
        tdef.key, tdef.key,
        (SELECT string_agg(format('OLD.%1$I IS DISTINCT FROM NEW.%1$I', col), ' OR ') FROM unnest(base_columns) AS col),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        array_to_string(insert_clauses, E'\n                    UNION ALL\n                '),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        (SELECT string_agg(format('OLD.%1$I IS DISTINCT FROM NEW.%1$I', col), ' OR ') FROM unnest(base_columns) AS col),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        array_to_string(delete_clauses, E'\n                    UNION ALL\n                '),
        facets._qualified(tdef.schemaname, tdef.delta_table),
        children_bitmap_update,
        -- Trigger for DELETE
        trg_name, -- Original trigger name
        facets._qualified(tdef.schemaname, tdef.tablename),
        facets._qualified(tdef.schemaname, tfunc_name),
        -- Trigger for INSERT/UPDATE
        trg_name, -- Original name + suffix
        array_to_string(base_columns, ', '),
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
    p_vector_weight double precision DEFAULT 0.5
) RETURNS TABLE(
    results jsonb,
    total_found bigint,
    search_time integer
) LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_start_time timestamp;
    v_search_cte text;
    v_search_query text;
    v_total_count_query text;
    v_results jsonb;
    v_total_found bigint;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Build the search CTE based on whether vector search is enabled
    IF p_vector_column IS NOT NULL THEN
        -- Combined BM25 and vector search
        v_search_cte := format('
            WITH search_results AS (
                SELECT
                    id AS document_id,
                    %I AS chunk,
                    ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', %L)) AS bm25_score,
                    1 - ((%I <=> %L::vector) / 2) AS vector_score,
                    (ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', %L)) * (1 - %s) + 
                     (1 - ((%I <=> %L::vector) / 2)) * %s) AS combined_score,
                    %I AS created_at,
                    %I AS updated_at,
                    %I AS metadata
                FROM %I.%I
                WHERE 
                    to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', %L)
                    OR %I <=> %L::vector < 0.8
            )',
            p_content_column,
            p_content_column, p_query,
            p_vector_column, p_query,
            p_content_column, p_query, p_vector_weight,
            p_vector_column, p_query, p_vector_weight,
            p_created_at_column,
            p_updated_at_column,
            p_metadata_column,
            p_schema_name, p_table_name,
            p_content_column, p_query,
            p_vector_column, p_query
        );
    ELSE
        -- BM25 search only
        v_search_cte := format('
            WITH search_results AS (
                SELECT
                    id AS document_id,
                    %I AS chunk,
                    ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', %L)) AS bm25_score,
                    0 AS vector_score,
                    ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', %L)) AS combined_score,
                    %I AS created_at,
                    %I AS updated_at,
                    %I AS metadata
                FROM %I.%I
                WHERE to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', %L)
            )',
            p_content_column,
            p_content_column, p_query,
            p_content_column, p_query,
            p_created_at_column,
            p_updated_at_column,
            p_metadata_column,
            p_schema_name, p_table_name,
            p_content_column, p_query
        );
    END IF;
    
    -- Total count query
    -- Note: COALESCE(p_min_score, 0.0) handles NULL min_score by defaulting to 0.0
    v_total_count_query := format('
        %s
        SELECT COUNT(*) FROM search_results WHERE combined_score >= %s',
        v_search_cte,
        COALESCE(p_min_score, 0.0)
    );

    -- Execute the total count query first
    EXECUTE v_total_count_query INTO v_total_found;

    -- Final search query to get results with pagination
    -- Note: When p_limit is 0 or NULL, use 'ALL' to return all results
    -- Note: COALESCE(p_min_score, 0.0) handles NULL min_score by defaulting to 0.0
    v_search_query := format('
        %s
        SELECT
            jsonb_agg(
                jsonb_build_object(
                    ''id'', document_id,
                    ''content'', chunk,
                    ''bm25_score'', bm25_score::float,
                    ''vector_score'', vector_score::float,
                    ''combined_score'', combined_score::float,
                    ''created_at'', created_at,
                    ''updated_at'', updated_at,
                    ''metadata'', metadata
                )
                ORDER BY combined_score DESC
            ) AS results
        FROM (
            SELECT * FROM search_results
            WHERE combined_score >= %s
            ORDER BY combined_score DESC
            LIMIT %s
            OFFSET %s
        ) subq
    ',
        v_search_cte,
        COALESCE(p_min_score, 0.0),
        COALESCE(NULLIF(p_limit, 0)::text, 'ALL'),
        p_offset
    );

    -- Execute the search query
    EXECUTE v_search_query INTO v_results;

    -- If no results found, initialize to empty array
    IF v_results IS NULL THEN
        v_results := '[]'::jsonb;
    END IF;

    -- Return the final result without facets
    RETURN QUERY SELECT
        v_results,
        v_total_found,
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
    p_facet_limit integer DEFAULT 5
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
    v_result_bitmap roaringbitmap;
    v_min_score_val double precision;
    v_limit_val int;
    v_tdef facets.faceted_table;
    v_filter_cardinality bigint;
    v_filter_ids int[];
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
    
    -- OPTIMIZATION: Get filter bitmap directly (no array conversion!)
    IF p_facets IS NOT NULL AND jsonb_typeof(p_facets) = 'object' AND p_facets != '{}'::jsonb THEN
        v_filter_bitmap := facets.filter_documents_by_facets_bitmap(p_schema_name, p_facets, p_table_name);
        
        -- Early exit if filter returns empty
        IF v_filter_bitmap IS NOT NULL AND rb_is_empty(v_filter_bitmap) THEN
            RETURN QUERY SELECT 
                '[]'::jsonb,
                '[]'::jsonb,
                0::bigint,
                EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
            RETURN;
        END IF;
    END IF;
    
    -- Handle different query scenarios
    IF p_query IS NULL OR p_query = '' OR trim(COALESCE(p_query, '')) = '' THEN
        -- =====================================================================
        -- EMPTY QUERY CASE: Return all documents matching facet filter
        -- This is the most common case for faceted browsing
        -- =====================================================================
        
        IF v_filter_bitmap IS NOT NULL THEN
            -- We have a facet filter - use bitmap directly
            v_total_found := rb_cardinality(v_filter_bitmap);
            
            -- Get paginated results by extracting IDs from bitmap
            -- Only fetch the rows we actually need (p_limit + p_offset at most)
            EXECUTE format('
                WITH bitmap_ids AS (
                    SELECT unnest(rb_to_array($1)) AS doc_id
                    LIMIT %s OFFSET %s
                )
                SELECT jsonb_agg(
                    jsonb_build_object(
                        ''id'', t.%I,
                        ''content'', t.%I,
                        ''bm25_score'', 1.0::float,
                        ''vector_score'', CASE WHEN %L IS NOT NULL THEN 1.0 ELSE 0.0 END::float,
                        ''combined_score'', 1.0::float,
                        ''created_at'', t.%I,
                        ''updated_at'', t.%I,
                        ''metadata'', t.%I
                    )
                )
                FROM %I.%I t
                JOIN bitmap_ids b ON t.%I = b.doc_id
            ',
                v_limit_val, p_offset,
                v_tdef.key,
                p_content_column,
                p_vector_column,
                p_created_at_column,
                p_updated_at_column,
                p_metadata_column,
                p_schema_name, p_table_name,
                v_tdef.key
            ) INTO v_results USING v_filter_bitmap;
            
            -- Use the filter bitmap directly for facet calculation
            v_result_bitmap := v_filter_bitmap;
        ELSE
            -- No filter - return all documents (facets will show all counts)
            EXECUTE format('
                SELECT COUNT(*)::bigint FROM %I.%I
            ', p_schema_name, p_table_name) INTO v_total_found;
            
            EXECUTE format('
                SELECT jsonb_agg(
                    jsonb_build_object(
                        ''id'', %I,
                        ''content'', %I,
                        ''bm25_score'', 1.0::float,
                        ''vector_score'', CASE WHEN %L IS NOT NULL THEN 1.0 ELSE 0.0 END::float,
                        ''combined_score'', 1.0::float,
                        ''created_at'', %I,
                        ''updated_at'', %I,
                        ''metadata'', %I
                    )
                )
                FROM (
                    SELECT * FROM %I.%I
                    ORDER BY %I
                    LIMIT %s OFFSET %s
                ) subq
            ',
                v_tdef.key,
                p_content_column,
                p_vector_column,
                p_created_at_column,
                p_updated_at_column,
                p_metadata_column,
                p_schema_name, p_table_name,
                v_tdef.key,
                v_limit_val, p_offset
            ) INTO v_results;
            
            -- No filter bitmap = show all facets
            v_result_bitmap := NULL;
        END IF;
    ELSE
        -- =====================================================================
        -- TEXT/VECTOR SEARCH CASE: Need to score and rank results
        -- For text search, results are typically much smaller than facet filters
        -- because text search is selective. We use the filter bitmap to intersect
        -- but the actual result set is based on text matches.
        -- =====================================================================
        
        IF p_vector_column IS NOT NULL THEN
            -- Combined BM25 and vector search
            IF v_filter_bitmap IS NOT NULL THEN
                -- OPTIMIZED: Apply facet filter FIRST to reduce search space
                -- Then do BM25/vector search only on the filtered subset
                v_filter_cardinality := rb_cardinality(v_filter_bitmap);
                    
                    -- For small filter sets (< 100k), convert to array and use IN clause (index-friendly)
                    -- For large sets, use rb_contains (bitmap is more efficient)
                    IF v_filter_cardinality > 0 AND v_filter_cardinality < 100000 THEN
                        -- Small filter: convert to array, use IN clause for better index usage
                        v_filter_ids := rb_to_array(v_filter_bitmap);
                        
                        EXECUTE format('
                            WITH filtered_table AS (
                                SELECT * FROM %I.%I WHERE %I = ANY($1)
                            ),
                            search_results AS (
                                SELECT
                                    %I AS id,
                                    %I AS content,
                                    ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) AS bm25_score,
                                    1 - ((%I <=> $2::vector) / 2) AS vector_score,
                                    (ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) * (1 - %s) + 
                                     (1 - ((%I <=> $2::vector) / 2)) * %s) AS combined_score,
                                    %I AS created_at,
                                    %I AS updated_at,
                                    %I AS metadata
                                FROM filtered_table
                                WHERE 
                                    to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                                    OR %I <=> $2::vector < 0.8
                            ),
                            filtered AS (
                                SELECT * FROM search_results WHERE combined_score >= $3
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
                                ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $4 OFFSET $5) p),
                                (SELECT COUNT(*)::bigint FROM filtered),
                                (SELECT COALESCE(rb_build_agg(id), rb_build(ARRAY[]::int[])) FROM filtered)
                        ',
                            p_schema_name, p_table_name, v_tdef.key,
                            v_tdef.key,
                            p_content_column,
                            p_content_column,
                            p_vector_column,
                            p_content_column, p_vector_weight,
                            p_vector_column, p_vector_weight,
                            p_created_at_column,
                            p_updated_at_column,
                            p_metadata_column,
                            p_content_column,
                            p_vector_column
                        ) INTO v_results, v_total_found, v_result_bitmap
                        USING v_filter_ids, p_query, v_min_score_val, v_limit_val, p_offset;
                ELSE
                    -- Large filter: use rb_contains (bitmap is more efficient than large array)
                    -- Apply filter FIRST, then search (rb_contains before text/vector search in WHERE)
                    EXECUTE format('
                        WITH search_results AS (
                            SELECT
                                %I AS id,
                                %I AS content,
                                ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) AS bm25_score,
                                1 - ((%I <=> $2::vector) / 2) AS vector_score,
                                (ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) * (1 - %s) + 
                                 (1 - ((%I <=> $2::vector) / 2)) * %s) AS combined_score,
                                %I AS created_at,
                                %I AS updated_at,
                                %I AS metadata
                            FROM %I.%I
                            WHERE rb_contains($1, %I)
                            AND (
                                to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                                OR %I <=> $2::vector < 0.8
                            )
                        ),
                        filtered AS (
                            SELECT * FROM search_results WHERE combined_score >= $3
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
                            ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $4 OFFSET $5) p),
                            (SELECT COUNT(*)::bigint FROM filtered),
                            (SELECT rb_build_agg(id) FROM filtered)
                    ',
                        v_tdef.key,
                        p_content_column,
                        p_content_column,
                        p_vector_column,
                        p_content_column, p_vector_weight,
                        p_vector_column, p_vector_weight,
                        p_created_at_column,
                        p_updated_at_column,
                        p_metadata_column,
                        p_schema_name, p_table_name,
                        v_tdef.key,
                        p_content_column,
                        p_vector_column
                    ) INTO v_results, v_total_found, v_result_bitmap
                    USING v_filter_bitmap, p_query, v_min_score_val, v_limit_val, p_offset;
                END IF;
            ELSE
                -- No facet filter
                EXECUTE format('
                    WITH search_results AS (
                        SELECT
                            %I AS id,
                            %I AS content,
                            ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $1)) AS bm25_score,
                            1 - ((%I <=> $1::vector) / 2) AS vector_score,
                            (ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $1)) * (1 - %s) + 
                             (1 - ((%I <=> $1::vector) / 2)) * %s) AS combined_score,
                            %I AS created_at,
                            %I AS updated_at,
                            %I AS metadata
                        FROM %I.%I
                        WHERE 
                            to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $1)
                            OR %I <=> $1::vector < 0.8
                    ),
                    filtered AS (
                        SELECT * FROM search_results WHERE combined_score >= $2
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
                        ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $3 OFFSET $4) p),
                        (SELECT COUNT(*)::bigint FROM filtered),
                        (SELECT rb_build_agg(id) FROM filtered)
                ',
                    v_tdef.key,
                    p_content_column,
                    p_content_column,
                    p_vector_column,
                    p_content_column, p_vector_weight,
                    p_vector_column, p_vector_weight,
                    p_created_at_column,
                    p_updated_at_column,
                    p_metadata_column,
                    p_schema_name, p_table_name,
                    p_content_column,
                    p_vector_column
                ) INTO v_results, v_total_found, v_result_bitmap
                USING p_query, v_min_score_val, v_limit_val, p_offset;
            END IF;
        ELSE
            -- BM25 search only
            IF v_filter_bitmap IS NOT NULL THEN
                -- OPTIMIZED: Apply facet filter FIRST to reduce search space
                -- Then do BM25 search only on the filtered subset
                -- Use array IN clause for small bitmaps (faster with index), rb_contains for large ones
                v_filter_cardinality := rb_cardinality(v_filter_bitmap);
                    
                    -- For small filter sets (< 100k), convert to array and use IN clause (index-friendly)
                    -- For large sets, use rb_contains (bitmap is more efficient)
                    IF v_filter_cardinality > 0 AND v_filter_cardinality < 100000 THEN
                        -- Small filter: convert to array, use IN clause for better index usage
                        v_filter_ids := rb_to_array(v_filter_bitmap);
                        
                        EXECUTE format('
                            WITH filtered_table AS (
                                SELECT * FROM %I.%I WHERE %I = ANY($1)
                            ),
                            search_results AS (
                                SELECT
                                    %I AS id,
                                    %I AS content,
                                    ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) AS bm25_score,
                                    0::float AS vector_score,
                                    ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) AS combined_score,
                                    %I AS created_at,
                                    %I AS updated_at,
                                    %I AS metadata
                                FROM filtered_table
                                WHERE to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                            ),
                            filtered AS (
                                SELECT * FROM search_results WHERE combined_score >= $3
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
                                ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $4 OFFSET $5) p),
                                (SELECT COUNT(*)::bigint FROM filtered),
                                (SELECT COALESCE(rb_build_agg(id), rb_build(ARRAY[]::int[])) FROM filtered)
                        ',
                            p_schema_name, p_table_name, v_tdef.key,
                            v_tdef.key,
                            p_content_column,
                            p_content_column,
                            p_content_column,
                            p_created_at_column,
                            p_updated_at_column,
                            p_metadata_column,
                            p_content_column
                        ) INTO v_results, v_total_found, v_result_bitmap
                        USING v_filter_ids, p_query, v_min_score_val, v_limit_val, p_offset;
                ELSE
                    -- Large filter: use rb_contains (bitmap is more efficient than large array)
                    -- Apply filter FIRST, then search (rb_contains before text search in WHERE)
                    EXECUTE format('
                        WITH search_results AS (
                            SELECT
                                %I AS id,
                                %I AS content,
                                ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) AS bm25_score,
                                0::float AS vector_score,
                                ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $2)) AS combined_score,
                                %I AS created_at,
                                %I AS updated_at,
                                %I AS metadata
                            FROM %I.%I
                            WHERE rb_contains($1, %I)
                            AND to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                        ),
                        filtered AS (
                            SELECT * FROM search_results WHERE combined_score >= $3
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
                            ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $4 OFFSET $5) p),
                            (SELECT COUNT(*)::bigint FROM filtered),
                            (SELECT rb_build_agg(id) FROM filtered)
                    ',
                        v_tdef.key,
                        p_content_column,
                        p_content_column,
                        p_content_column,
                        p_created_at_column,
                        p_updated_at_column,
                        p_metadata_column,
                        p_schema_name, p_table_name,
                        v_tdef.key,
                        p_content_column
                    ) INTO v_results, v_total_found, v_result_bitmap
                    USING v_filter_bitmap, p_query, v_min_score_val, v_limit_val, p_offset;
                END IF;
            ELSE
                -- No facet filter
                EXECUTE format('
                    WITH search_results AS (
                        SELECT
                            %I AS id,
                            %I AS content,
                            ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $1)) AS bm25_score,
                            0::float AS vector_score,
                            ts_rank_cd(to_tsvector(''english'', %I), plainto_tsquery(''english'', $1)) AS combined_score,
                            %I AS created_at,
                            %I AS updated_at,
                            %I AS metadata
                        FROM %I.%I
                        WHERE to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $1)
                    ),
                    filtered AS (
                        SELECT * FROM search_results WHERE combined_score >= $2
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
                        ) FROM (SELECT * FROM filtered ORDER BY combined_score DESC LIMIT $3 OFFSET $4) p),
                        (SELECT COUNT(*)::bigint FROM filtered),
                        (SELECT rb_build_agg(id) FROM filtered)
                ',
                    v_tdef.key,
                    p_content_column,
                    p_content_column,
                    p_content_column,
                    p_created_at_column,
                    p_updated_at_column,
                    p_metadata_column,
                    p_schema_name, p_table_name,
                    p_content_column
                ) INTO v_results, v_total_found, v_result_bitmap
                USING p_query, v_min_score_val, v_limit_val, p_offset;
            END IF;
        END IF;
    END IF;

    -- Initialize results if null
    IF v_results IS NULL THEN
        v_results := '[]'::jsonb;
    END IF;
    
    -- =====================================================================
    -- FACET CALCULATION: Use bitmap directly (no array conversion!)
    -- =====================================================================
    
    -- When p_facets is NULL, user wants ALL facets (not filtered by search results)
    -- When p_facets is NOT NULL, user has applied facet filters, so we should use v_result_bitmap
    -- to show facets only for the filtered/search results
    IF p_facets IS NULL THEN
        -- User wants all facets, not restricted to search results
        IF v_total_found > 0 THEN
            v_facets := facets.hierarchical_facets_bitmap(
                v_table_id,
                p_facet_limit,
                NULL
            );
        ELSE
            v_facets := '[]'::jsonb;
        END IF;
    ELSIF v_result_bitmap IS NOT NULL AND NOT rb_is_empty(v_result_bitmap) THEN
        -- User has applied facet filters, show facets only for filtered/search results
        v_facets := facets.hierarchical_facets_bitmap(
            v_table_id,
            p_facet_limit,
            v_result_bitmap
        );
    ELSIF v_result_bitmap IS NULL AND v_total_found > 0 THEN
        -- No result bitmap but we have results - this shouldn't happen but handle it gracefully
        -- When there's a query, we need to rebuild the bitmap from search results for facet calculation
        -- This is a fallback case - ideally v_result_bitmap should always be set correctly
        IF p_query IS NOT NULL AND p_query != '' AND trim(COALESCE(p_query, '')) != '' THEN
            -- Rebuild bitmap from search results (BM25 only case, no vector)
            IF p_vector_column IS NULL THEN
                IF v_filter_bitmap IS NOT NULL THEN
                    v_filter_cardinality := rb_cardinality(v_filter_bitmap);
                    IF v_filter_cardinality > 0 AND v_filter_cardinality < 100000 THEN
                        v_filter_ids := rb_to_array(v_filter_bitmap);
                        EXECUTE format('
                            WITH search_results AS (
                                SELECT %I AS id
                                FROM %I.%I
                                WHERE %I = ANY($1)
                                AND to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                            )
                            SELECT COALESCE(rb_build_agg(id), rb_build(ARRAY[]::int[]))
                            FROM search_results
                        ',
                            v_tdef.key,
                            p_schema_name, p_table_name,
                            v_tdef.key,
                            p_content_column
                        ) INTO v_result_bitmap
                        USING v_filter_ids, p_query;
                    ELSE
                        EXECUTE format('
                            WITH search_results AS (
                                SELECT %I AS id
                                FROM %I.%I
                                WHERE rb_contains($1, %I)
                                AND to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                            )
                            SELECT COALESCE(rb_build_agg(id), rb_build(ARRAY[]::int[]))
                            FROM search_results
                        ',
                            v_tdef.key,
                            p_schema_name, p_table_name,
                            v_tdef.key,
                            p_content_column
                        ) INTO v_result_bitmap
                        USING v_filter_bitmap, p_query;
                    END IF;
                ELSE
                    EXECUTE format('
                        WITH search_results AS (
                            SELECT %I AS id
                            FROM %I.%I
                            WHERE to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $1)
                        )
                        SELECT COALESCE(rb_build_agg(id), rb_build(ARRAY[]::int[]))
                        FROM search_results
                    ',
                        v_tdef.key,
                        p_schema_name, p_table_name,
                        p_content_column
                    ) INTO v_result_bitmap
                    USING p_query;
                END IF;
            END IF;
            
            -- Now use the rebuilt bitmap for facets
            IF v_result_bitmap IS NOT NULL AND NOT rb_is_empty(v_result_bitmap) THEN
                v_facets := facets.hierarchical_facets_bitmap(
                    v_table_id,
                    p_facet_limit,
                    v_result_bitmap
                );
            ELSE
                -- Still no bitmap - show all facets as fallback (better than empty)
                v_facets := facets.hierarchical_facets_bitmap(
                    v_table_id,
                    p_facet_limit,
                    NULL
                );
            END IF;
        ELSE
            -- No query = show all facets (unfiltered)
            v_facets := facets.hierarchical_facets_bitmap(
                v_table_id,
                p_facet_limit,
                NULL
            );
        END IF;
    ELSE
        v_facets := '[]'::jsonb;
    END IF;

    -- Return the final result with facets
    RETURN QUERY SELECT
        v_results,
        v_facets,
        COALESCE(v_total_found, 0),
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
RETURNS void
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

-- Log version activation on extension creation
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    RAISE NOTICE 'pg_facets extension version % activated', v_version;
END $$;

