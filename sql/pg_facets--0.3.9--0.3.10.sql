-- pg_facets 0.3.9 → 0.3.10
-- TOAST Bit Shift Trick Optimization
-- 
-- This migration applies the TOAST bit shift trick to force PostgreSQL to detoast
-- bitmaps once instead of repeatedly during joins. This can provide 10-13x performance
-- improvements for queries that do bitmap intersections.
--
-- The trick: Use `bitmap << 0 OFFSET 0` in a CTE to force detoasting without changing
-- the value, preventing repeated detoasting in joins.

-- Update version
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.3.10';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Optimize get_filtered_boolean_facet_counts: Force detoast of filter bitmap once
CREATE OR REPLACE FUNCTION facets.get_filtered_boolean_facet_counts(
    p_table_id oid,
    p_facet_name text,
    p_filter_bitmap roaringbitmap
) RETURNS TABLE(
    facet_id int,
    true_count bigint,
    false_count bigint,
    total_count bigint
) LANGUAGE plpgsql STABLE AS $$
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
    
    -- TOAST OPTIMIZATION: Force detoast once using bit shift trick
    EXECUTE format('
        WITH detoasted_filter AS (
            SELECT $1 << 0 AS filter_bitmap
            OFFSET 0
        )
        SELECT 
            COALESCE(SUM(rb_and_cardinality(postinglist, df.filter_bitmap)), 0)::bigint
        FROM %s, detoasted_filter df
        WHERE facet_id = $2
        AND facet_value = ''true''
        AND rb_intersect(postinglist, df.filter_bitmap)',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO v_true_count USING p_filter_bitmap, v_facet_id;
    
    -- Get false count with filter
    EXECUTE format('
        WITH detoasted_filter AS (
            SELECT $1 << 0 AS filter_bitmap
            OFFSET 0
        )
        SELECT 
            COALESCE(SUM(rb_and_cardinality(postinglist, df.filter_bitmap)), 0)::bigint
        FROM %s, detoasted_filter df
        WHERE facet_id = $2
        AND facet_value = ''false''
        AND rb_intersect(postinglist, df.filter_bitmap)',
        facets._qualified(tdef.schemaname, tdef.facets_table)
    ) INTO v_false_count USING p_filter_bitmap, v_facet_id;
    
    -- Handle case where counts might be NULL
    v_true_count := COALESCE(v_true_count, 0);
    v_false_count := COALESCE(v_false_count, 0);
    
    RETURN QUERY
    SELECT 
        v_facet_id,
        v_true_count,
        v_false_count,
        (v_true_count + v_false_count)::bigint;
END;
$$;

-- Optimize get_top_values_for_facet: Force detoast of filter bitmap once
CREATE OR REPLACE FUNCTION facets.get_top_values_for_facet(
    p_table_id oid,
    p_facet_name text,
    p_filter_bitmap roaringbitmap DEFAULT NULL,
    p_limit int DEFAULT 5
) RETURNS TABLE(
    facet_name text,
    facet_value text,
    cardinality bigint,
    facet_id int
) LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_facet_id int;
    v_facet_type text;
    tdef facets.faceted_table;
BEGIN
    -- Get table information
    SELECT t.* INTO tdef FROM facets.faceted_table t WHERE t.table_id = p_table_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table with ID % not found', p_table_id;
    END IF;
    
    -- Get facet information
    SELECT fd.facet_id, fd.facet_type INTO v_facet_id, v_facet_type
    FROM facets.facet_definition fd
    WHERE fd.table_id = p_table_id AND fd.facet_name = p_facet_name;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Facet % not found for table %', p_facet_name, p_table_id;
    END IF;
    
    -- Handle boolean facets specially
    IF v_facet_type = 'boolean' THEN
        RETURN QUERY
        SELECT 
            p_facet_name AS facet_name,
            CASE WHEN count > 0 THEN 'true' ELSE 'false' END AS facet_value,
            count AS cardinality,
            v_facet_id AS facet_id
        FROM (
            SELECT true_count AS count FROM facets.get_filtered_boolean_facet_counts(p_table_id, p_facet_name, p_filter_bitmap)
            UNION ALL
            SELECT false_count AS count FROM facets.get_filtered_boolean_facet_counts(p_table_id, p_facet_name, p_filter_bitmap)
        ) AS boolean_counts
        WHERE count > 0
        ORDER BY count DESC
        LIMIT p_limit;
        RETURN;
    END IF;
    
    IF p_filter_bitmap IS NULL THEN
        -- No filter, just get top values
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
        -- TOAST OPTIMIZATION: Force detoast filter bitmap once, then convert to chunk filters
        RETURN QUERY EXECUTE format($sql$
            WITH detoasted_filter AS (
                SELECT $1 << 0 AS filter_bitmap
                OFFSET 0
            ),
            filter_by_chunk AS (
                -- Convert full document IDs to (chunk_id, offset) pairs
                SELECT 
                    (doc_id >> %s)::int4 AS chunk_id,
                    rb_build_agg((doc_id & ((1 << %s) - 1))::int4) AS chunk_filter
                FROM detoasted_filter df
                CROSS JOIN LATERAL unnest(rb_to_array(df.filter_bitmap)) AS doc_id
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

-- Optimize count_results: Force detoast lookup bitmaps once
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
        -- TOAST OPTIMIZATION: Force detoast lookup bitmaps once using bit shift trick
        sql := format($sql$
        WITH filters AS (
            SELECT facet_id, facet_name, facet_value
            FROM facets.facet_definition JOIN unnest($1) t USING (facet_name)
            WHERE table_id = $2
        ), lookup_raw AS (
            SELECT chunk_id, rb_and_agg(postinglist) AS postinglist
            FROM %s d JOIN filters USING (facet_id, facet_value)
            GROUP BY chunk_id
        ), lookup AS (
            SELECT chunk_id, postinglist << 0 AS postinglist
            FROM lookup_raw
            OFFSET 0
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
    RETURN QUERY EXECUTE sql USING filters, p_table_id, select_facets;
END;
$$;
