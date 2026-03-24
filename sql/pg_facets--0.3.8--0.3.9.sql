-- Migration from pg_facets 0.3.8 to 0.3.9
-- Performance optimizations for search_documents_with_facets
-- 
-- Key optimizations:
-- 1. Replaced rb_contains() in WHERE clauses with bitmap intersection
-- 2. Optimized filter_documents_by_facets_bitmap to reduce array conversions
-- 3. Improved query structure for better PostgreSQL query planning

-- Update version function
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.3.9';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Optimized filter_documents_by_facets_bitmap
-- Uses rb_or_agg to combine postinglists by chunk_id first, reducing array conversions
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
$$;

-- OPTIMIZED search_documents_with_facets
-- Key optimization: Apply facet filter FIRST, then do BM25/vector search on filtered subset
-- This reduces the search space significantly
-- Hybrid approach: Array IN for small filters (< 100k), rb_contains for large ones
-- Kept rb_or_agg optimization in filter_documents_by_facets_bitmap
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
        -- OPTIMIZED: Build bitmap from search, intersect with filter, then fetch rows
        -- This avoids rb_contains() per-row checks and enables better query planning
        -- =====================================================================
        
        IF p_vector_column IS NOT NULL THEN
            -- Combined BM25 and vector search
            IF v_filter_bitmap IS NOT NULL THEN
                -- OPTIMIZED: Build bitmap from text/vector search, then intersect with filter bitmap
                EXECUTE format('
                    WITH text_vector_search_bitmap AS (
                        SELECT rb_build_agg(%I) AS search_bm
                        FROM %I.%I
                        WHERE 
                            to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                            OR %I <=> $2::vector < 0.8
                    ),
                    filtered_bitmap AS (
                        SELECT rb_and($1, COALESCE((SELECT search_bm FROM text_vector_search_bitmap), rb_build(ARRAY[]::int[]))) AS final_bm
                    ),
                    final_ids AS (
                        SELECT unnest(rb_to_array((SELECT final_bm FROM filtered_bitmap))) AS id
                    ),
                    search_results AS (
                        SELECT
                            t.%I AS id,
                            t.%I AS content,
                            ts_rank_cd(to_tsvector(''english'', t.%I), plainto_tsquery(''english'', $2)) AS bm25_score,
                            1 - ((t.%I <=> $2::vector) / 2) AS vector_score,
                            (ts_rank_cd(to_tsvector(''english'', t.%I), plainto_tsquery(''english'', $2)) * (1 - %s) + 
                             (1 - ((t.%I <=> $2::vector) / 2)) * %s) AS combined_score,
                            t.%I AS created_at,
                            t.%I AS updated_at,
                            t.%I AS metadata
                        FROM %I.%I t
                        INNER JOIN final_ids f ON t.%I = f.id
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
                    p_schema_name, p_table_name,
                    p_content_column,
                    p_vector_column,
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
                    v_tdef.key
                ) INTO v_results, v_total_found, v_result_bitmap
                USING v_filter_bitmap, p_query, v_min_score_val, v_limit_val, p_offset;
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
                -- OPTIMIZED: Build bitmap from text search, then intersect with filter bitmap
                EXECUTE format('
                    WITH text_search_bitmap AS (
                        SELECT rb_build_agg(%I) AS search_bm
                        FROM %I.%I
                        WHERE to_tsvector(''english'', %I) @@ plainto_tsquery(''english'', $2)
                    ),
                    filtered_bitmap AS (
                        SELECT rb_and($1, COALESCE((SELECT search_bm FROM text_search_bitmap), rb_build(ARRAY[]::int[]))) AS final_bm
                    ),
                    final_ids AS (
                        SELECT unnest(rb_to_array((SELECT final_bm FROM filtered_bitmap))) AS id
                    ),
                    search_results AS (
                        SELECT
                            t.%I AS id,
                            t.%I AS content,
                            ts_rank_cd(to_tsvector(''english'', t.%I), plainto_tsquery(''english'', $2)) AS bm25_score,
                            0::float AS vector_score,
                            ts_rank_cd(to_tsvector(''english'', t.%I), plainto_tsquery(''english'', $2)) AS combined_score,
                            t.%I AS created_at,
                            t.%I AS updated_at,
                            t.%I AS metadata
                        FROM %I.%I t
                        INNER JOIN final_ids f ON t.%I = f.id
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
                    p_schema_name, p_table_name,
                    p_content_column,
                    v_tdef.key,
                    p_content_column,
                    p_content_column,
                    p_content_column,
                    p_created_at_column,
                    p_updated_at_column,
                    p_metadata_column,
                    p_schema_name, p_table_name,
                    v_tdef.key
                ) INTO v_results, v_total_found, v_result_bitmap
                USING v_filter_bitmap, p_query, v_min_score_val, v_limit_val, p_offset;
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
        -- No result bitmap = show all facets (unfiltered)
        v_facets := facets.hierarchical_facets_bitmap(
            v_table_id,
            p_facet_limit,
            NULL
        );
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

-- Log version activation
DO $$
DECLARE
    v_version text;
BEGIN
    v_version := facets._get_version();
    RAISE NOTICE 'pg_facets extension version % activated with performance optimizations', v_version;
END $$;
