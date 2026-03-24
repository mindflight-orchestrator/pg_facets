-- pg_facets 0.3.10 → 0.4.0
-- BM25 Full-Text Search Integration
-- 
-- This migration adds BM25 (Best Matching 25) full-text search capabilities
-- to pg_facets, replacing ts_rank_cd with proper BM25 ranking.
--
-- Features:
-- - BM25 inverted index using roaring bitmaps
-- - Language-aware tokenization using PostgreSQL text search configs
-- - Prefix and fuzzy prefix matching support
-- - Integration with existing faceting functionality

-- Update version
CREATE OR REPLACE FUNCTION facets._get_version()
RETURNS text AS $$
BEGIN
    RETURN '0.4.0';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- SECTION 1: BM25 TABLES
-- ============================================================================

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

-- ============================================================================
-- SECTION 2: INDEXES
-- ============================================================================

-- B-tree index for exact term lookups (standard BM25)
CREATE INDEX IF NOT EXISTS bm25_index_term_btree ON facets.bm25_index 
    (table_id, term_hash);

-- GIN index with trigram ops for prefix/fuzzy matching (optional, requires pg_trgm)
-- Note: This index is only created if pg_trgm extension is available
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

-- ============================================================================
-- SECTION 3: NATIVE FUNCTION DECLARATIONS
-- ============================================================================

-- These functions are implemented in Zig and exported from the extension

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

-- ============================================================================
-- SECTION 4: SQL WRAPPER FUNCTIONS
-- ============================================================================

-- Index a document for BM25 search
CREATE OR REPLACE FUNCTION facets.bm25_index_document(
    p_table_id regclass,
    p_doc_id bigint,
    p_content text,
    p_content_column text DEFAULT 'content',
    p_language text DEFAULT 'english'
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_table_oid oid;
BEGIN
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    -- Check if table is registered in facets.faceted_table
    -- Note: table_id in faceted_table is the OID of the table
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table % (oid: %) is not registered in facets.faceted_table. Run facets.add_faceting_to_table() first.', p_table_id, v_table_oid;
    END IF;
    
    -- Call native function
    PERFORM facets.bm25_index_document_native(v_table_oid, p_doc_id, p_content, p_language);
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
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    -- Call native function
    PERFORM facets.bm25_delete_document_native(v_table_oid, p_doc_id);
END;
$$;

-- Search documents using BM25
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
DECLARE
    v_table_oid oid;
BEGIN
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    -- Call native function
    RETURN QUERY
    SELECT * FROM facets.bm25_search_native(
        v_table_oid,
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
DECLARE
    v_table_oid oid;
    v_score float;
BEGIN
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    -- Call native function
    SELECT facets.bm25_score_native(
        v_table_oid,
        p_query,
        p_doc_id,
        p_language,
        p_k1,
        p_b
    ) INTO v_score;
    
    RETURN COALESCE(v_score, 0.0);
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
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    -- Calculate statistics
    SELECT 
        COUNT(*)::bigint,
        COALESCE(AVG(doc_length), 0)::float
    INTO v_total_docs, v_avg_length
    FROM facets.bm25_documents
    WHERE table_id = v_table_oid;
    
    -- Update statistics table
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
    -- Get table OID directly from regclass
    v_table_oid := p_table_id::oid;
    
    RETURN QUERY
    SELECT 
        s.total_documents,
        s.avg_document_length
    FROM facets.bm25_statistics s
    WHERE s.table_id = v_table_oid;
END;
$$;

-- ============================================================================
-- SECTION 5: UPDATE EXISTING SEARCH FUNCTIONS TO USE BM25
-- ============================================================================

-- Replace ts_rank_cd with BM25 in search_documents function
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
    v_search_cte text;
    v_search_query text;
    v_total_count_query text;
    v_results jsonb;
    v_total_found bigint;
    v_table_id oid;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Get table_id
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = p_schema_name AND tablename = p_table_name;
    
    IF v_table_id IS NULL THEN
        RAISE EXCEPTION 'Table %.% not found in facets.faceted_table', p_schema_name, p_table_name;
    END IF;
    
    -- Build the search CTE based on whether vector search is enabled
    IF p_vector_column IS NOT NULL THEN
        -- Combined BM25 and vector search
        v_search_cte := format('
            WITH search_results AS (
                SELECT
                    id AS document_id,
                    %I AS chunk,
                    facets.bm25_score(%L::regclass, %L, id, %L) AS bm25_score,
                    1 - ((%I <=> %L::vector) / 2) AS vector_score,
                    (facets.bm25_score(%L::regclass, %L, id, %L) * (1 - %s) + 
                     (1 - ((%I <=> %L::vector) / 2)) * %s) AS combined_score,
                    %I AS created_at,
                    %I AS updated_at,
                    %I AS metadata
                FROM %I.%I
                WHERE 
                    EXISTS (
                        SELECT 1 FROM facets.bm25_search(%L::regclass, %L, %L, false, false, 0.3, 1.2, 0.75, 1000) s
                        WHERE s.doc_id = id
                    )
                    OR %I <=> %L::vector < 0.8
            )',
            p_content_column,
            p_schema_name || '.' || p_table_name, p_query, p_language,
            p_vector_column, p_query,
            p_schema_name || '.' || p_table_name, p_query, p_language, p_vector_weight,
            p_vector_column, p_query, p_vector_weight,
            p_created_at_column,
            p_updated_at_column,
            p_metadata_column,
            p_schema_name, p_table_name,
            p_schema_name || '.' || p_table_name, p_query, p_language,
            p_vector_column, p_query
        );
    ELSE
        -- BM25 search only
        v_search_cte := format('
            WITH search_results AS (
                SELECT
                    id AS document_id,
                    %I AS chunk,
                    facets.bm25_score(%L::regclass, %L, id, %L) AS bm25_score,
                    0 AS vector_score,
                    facets.bm25_score(%L::regclass, %L, id, %L) AS combined_score,
                    %I AS created_at,
                    %I AS updated_at,
                    %I AS metadata
                FROM %I.%I
                WHERE EXISTS (
                    SELECT 1 FROM facets.bm25_search(%L::regclass, %L, %L, false, false, 0.3, 1.2, 0.75, 1000) s
                    WHERE s.doc_id = id
                )
            )',
            p_content_column,
            p_schema_name || '.' || p_table_name, p_query, p_language,
            p_schema_name || '.' || p_table_name, p_query, p_language,
            p_created_at_column,
            p_updated_at_column,
            p_metadata_column,
            p_schema_name, p_table_name,
            p_schema_name || '.' || p_table_name, p_query, p_language
        );
    END IF;
    
    -- Total count query
    v_total_count_query := format('
        %s
        SELECT COUNT(*) FROM search_results WHERE combined_score >= %s',
        v_search_cte,
        COALESCE(p_min_score, 0.0)
    );

    -- Execute the total count query first
    EXECUTE v_total_count_query INTO v_total_found;

    -- Final search query to get results with pagination
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

-- Note: search_documents_with_facets will be updated in a future migration
-- to maintain backward compatibility during transition period
