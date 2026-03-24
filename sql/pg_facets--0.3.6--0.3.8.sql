-- Migration from pg_facets 0.3.6 to 0.3.8
-- This migration updates the version and ensures all functions are up to date
-- with the ID reconstruction fix for filter_documents_by_facets_bitmap

-- Update version function
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.3.8';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Ensure filter_documents_by_facets_bitmap has the ID reconstruction fix
-- This function must reconstruct IDs from (chunk_id << chunk_bits) | in_chunk_id
CREATE OR REPLACE FUNCTION facets.filter_documents_by_facets_bitmap(
    p_schema_name text,
    p_facets jsonb,
    p_table_name text DEFAULT NULL
) RETURNS roaringbitmap LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_tdef facets.faceted_table;
    v_result_bitmap roaringbitmap := NULL;
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
        -- The postinglist contains in_chunk_id values, we must combine with chunk_id to get original keys
        EXECUTE format(
            'SELECT rb_build_agg(((f.chunk_id::bigint << $3) | pl.in_chunk_id)::int4)
             FROM %I.%I f
             CROSS JOIN LATERAL unnest(rb_to_array(f.postinglist)) AS pl(in_chunk_id)
             WHERE f.facet_id = $1 AND f.facet_value = ANY(string_to_array($2, '',''))',
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
$$;

-- Log version activation
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    RAISE NOTICE 'pg_facets extension version % activated', v_version;
END $$;
