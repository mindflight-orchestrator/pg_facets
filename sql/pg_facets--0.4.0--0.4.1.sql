-- Update pg_facets from 0.4.0 to 0.4.1

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

-- Add the new native function to get matches bitmap
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

-- Update search_documents_with_facets to use BM25 index correctly
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
    p_language text DEFAULT 'english'
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
        v_query_bitmap := facets.bm25_get_matches_bitmap_native(v_table_id, p_query, p_language);
        
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
    IF p_query IS NOT NULL AND p_query != '' AND trim(p_query) != '' THEN
        -- Text Search Case
        EXECUTE format('
            WITH candidates AS (
                SELECT id FROM %I.%I
                WHERE %s
            ),
            scored AS (
                SELECT
                    c.id,
                    t.%I AS content,
                    facets.bm25_score_native($1, $2, c.id, $3) AS bm25_score,
                    CASE WHEN %L IS NOT NULL THEN 1 - ((t.%I <=> $2::vector) / 2) ELSE 0.0 END AS vector_score,
                    %I AS created_at,
                    %I AS updated_at,
                    %I AS metadata
                FROM candidates c
                JOIN %I.%I t ON t.id = c.id
            ),
            combined AS (
                SELECT *,
                    CASE 
                        WHEN %L IS NOT NULL THEN (bm25_score * (1 - $4) + vector_score * $4)
                        ELSE bm25_score
                    END AS combined_score
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
                (SELECT COUNT(*)::bigint FROM filtered),
                (SELECT rb_build_agg((id)::int4) FROM filtered)
        ', 
            p_schema_name, p_table_name,
            CASE 
                WHEN v_result_bitmap IS NOT NULL THEN 'id = ANY(rb_to_array($8))'
                ELSE 'TRUE'
            END,
            p_content_column,
            p_vector_column, p_vector_column,
            p_created_at_column, p_updated_at_column, p_metadata_column,
            p_schema_name, p_table_name,
            p_vector_column
        ) INTO v_results, v_total_found, v_result_bitmap
        USING v_table_id, p_query, p_language, p_vector_weight, v_min_score_val, v_limit_val, p_offset, v_result_bitmap;
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
                    ''id'', t.id,
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
            JOIN %I.%I t ON t.id = i.id
        ',
            CASE 
                WHEN v_result_bitmap IS NOT NULL THEN 'unnest(rb_to_array($1))'
                ELSE 'id FROM ' || quote_ident(p_schema_name) || '.' || quote_ident(p_table_name)
            END,
            p_content_column,
            p_created_at_column, p_updated_at_column, p_metadata_column,
            p_schema_name, p_table_name
        ) INTO v_results
        USING v_result_bitmap, v_limit_val, p_offset;
    END IF;

    -- 4. Get Facet Counts
    v_facets := facets.get_facet_counts_by_bitmap(v_table_id, v_result_bitmap, p_facet_limit);
    
    RETURN QUERY SELECT 
        COALESCE(v_results, '[]'::jsonb),
        COALESCE(v_facets, '[]'::jsonb),
        COALESCE(v_total_found, 0)::bigint,
        EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start_time))::INT;
END;
$$;

-- Update bm25_search to default to native implementation
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

-- Update bm25_score to use native implementation
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
