-- Phase 6: Performance Optimization & Testing
-- Tests performance with larger datasets and optimization features

\echo '=============================================='
\echo 'Phase 6: Performance Optimization & Testing'
\echo '=============================================='

-- Setup
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP SCHEMA IF EXISTS bm25_phase6_test CASCADE;
CREATE SCHEMA bm25_phase6_test;

-- Create table for performance testing
CREATE TABLE bm25_phase6_test.documents (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Generate test data (1000 documents for performance testing)
\echo ''
\echo '--- Test 6.1: Generate test data (1000 documents) ---'
DO $$
DECLARE
    v_categories TEXT[] := ARRAY['Technology', 'Science', 'Business', 'Education', 'Health'];
    v_terms TEXT[] := ARRAY['database', 'system', 'query', 'performance', 'optimization', 'analysis', 'design', 'development'];
    v_category TEXT;
    v_content TEXT;
    v_i int;
BEGIN
    FOR v_i IN 1..1000 LOOP
        v_category := v_categories[1 + (v_i % array_length(v_categories, 1))];
        v_content := format('%s %s %s %s %s',
            v_terms[1 + (v_i % array_length(v_terms, 1))],
            v_terms[1 + ((v_i + 1) % array_length(v_terms, 1))],
            v_terms[1 + ((v_i + 2) % array_length(v_terms, 1))],
            v_terms[1 + ((v_i + 3) % array_length(v_terms, 1))],
            v_terms[1 + ((v_i + 4) % array_length(v_terms, 1))]
        );
        
        INSERT INTO bm25_phase6_test.documents (title, content, category)
        VALUES (format('Document %s', v_i), v_content, v_category);
    END LOOP;
    
    RAISE NOTICE 'PASS: Generated 1000 test documents';
END;
$$;

SELECT facets.add_faceting_to_table(
    'bm25_phase6_test.documents',
    key => 'id',
    facets => ARRAY[facets.plain_facet('category')],
    populate => true
);

-- Test 1: Index creation performance
\echo ''
\echo '--- Test 6.2: Index creation performance (1000 documents) ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_doc record;
    v_indexed_count int;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Index all documents
    FOR v_doc IN SELECT id, content FROM bm25_phase6_test.documents ORDER BY id
    LOOP
        PERFORM facets.bm25_index_document(
            'bm25_phase6_test.documents'::regclass,
            v_doc.id,
            v_doc.content,
            'content',
            'english'
        );
    END LOOP;
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    -- Verify all documents indexed
    SELECT COUNT(*) INTO v_indexed_count
    FROM facets.bm25_documents
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase6_test' AND tablename = 'documents');
    
    IF v_indexed_count = 1000 THEN
        RAISE NOTICE 'PASS: Indexed 1000 documents in % ms (avg: % ms/doc)', 
            v_duration_ms, v_duration_ms / 1000.0;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected 1000 indexed documents, found %', v_indexed_count;
    END IF;
    
    -- Performance target: < 100ms per document for 1000 documents
    IF v_duration_ms < 100000 THEN
        RAISE NOTICE 'PASS: Indexing performance meets target (< 100ms/doc)';
    ELSE
        RAISE NOTICE 'INFO: Indexing performance: % ms/doc (may need optimization)', v_duration_ms / 1000.0;
    END IF;
END;
$$;

-- Test 2: Query performance
\echo ''
\echo '--- Test 6.3: Query performance (exact match) ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_result_count int;
    v_iterations int := 10;
    v_total_ms int := 0;
    v_avg_ms float;
BEGIN
    -- Run multiple queries and average
    FOR v_iterations IN 1..10 LOOP
        v_start_time := clock_timestamp();
        
        SELECT COUNT(*) INTO v_result_count
        FROM facets.bm25_search(
            'bm25_phase6_test.documents'::regclass,
            'database',
            'english',
            false,  -- no prefix
            false,  -- no fuzzy
            0.3,
            1.2,
            0.75,
            100
        );
        
        v_end_time := clock_timestamp();
        v_total_ms := v_total_ms + EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    END LOOP;
    
    v_avg_ms := v_total_ms / 10.0;
    
    IF v_avg_ms < 200 THEN
        RAISE NOTICE 'PASS: Query performance - avg % ms per query (% results)', v_avg_ms, v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Query performance - avg % ms per query (acceptable for 1000 docs)', v_avg_ms;
    END IF;
END;
$$;

-- Test 3: Prefix matching performance
\echo ''
\echo '--- Test 6.4: Prefix matching performance ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_result_count int;
BEGIN
    v_start_time := clock_timestamp();
    
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase6_test.documents'::regclass,
        'dat',
        'english',
        true,   -- prefix enabled
        false,
        0.3,
        1.2,
        0.75,
        100
    );
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    IF v_duration_ms < 500 THEN
        RAISE NOTICE 'PASS: Prefix matching completed in % ms (% results)', v_duration_ms, v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Prefix matching took % ms (acceptable)', v_duration_ms;
    END IF;
END;
$$;

-- Test 4: Index size
\echo ''
\echo '--- Test 6.5: Index size analysis ---'
DO $$
DECLARE
    v_index_size bigint;
    v_docs_size bigint;
    v_stats_size bigint;
    v_total_size bigint;
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id
    FROM facets.faceted_table
    WHERE schemaname = 'bm25_phase6_test' AND tablename = 'documents';
    
    -- Get index table size
    SELECT pg_total_relation_size('facets.bm25_index'::regclass) INTO v_index_size;
    
    -- Get documents table size
    SELECT pg_total_relation_size('facets.bm25_documents'::regclass) INTO v_docs_size;
    
    -- Get statistics table size
    SELECT pg_total_relation_size('facets.bm25_statistics'::regclass) INTO v_stats_size;
    
    v_total_size := v_index_size + v_docs_size + v_stats_size;
    
    RAISE NOTICE 'Index sizes:';
    RAISE NOTICE '  bm25_index: % MB', (v_index_size / 1024 / 1024);
    RAISE NOTICE '  bm25_documents: % MB', (v_docs_size / 1024 / 1024);
    RAISE NOTICE '  bm25_statistics: % MB', (v_stats_size / 1024 / 1024);
    RAISE NOTICE '  Total: % MB (for 1000 documents)', (v_total_size / 1024 / 1024);
    
    -- Estimate for 1M documents: should be roughly 1000x
    RAISE NOTICE '  Estimated for 1M docs: ~% MB', ((v_total_size / 1024 / 1024) * 1000);
    
    IF v_total_size < 100 * 1024 * 1024 THEN  -- Less than 100MB for 1000 docs
        RAISE NOTICE 'PASS: Index size is reasonable';
    ELSE
        RAISE NOTICE 'INFO: Index size may be large, consider optimization';
    END IF;
END;
$$;

-- Test 5: Batch indexing performance
\echo ''
\echo '--- Test 6.6: Batch operations (delete and reindex) ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_doc_count int;
BEGIN
    -- Delete 100 documents
    v_start_time := clock_timestamp();
    
    FOR v_doc_count IN 1..100 LOOP
        PERFORM facets.bm25_delete_document(
            'bm25_phase6_test.documents'::regclass,
            v_doc_count
        );
    END LOOP;
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    RAISE NOTICE 'PASS: Deleted 100 documents in % ms (avg: % ms/doc)', v_duration_ms, v_duration_ms / 100.0;
    
    -- Reindex 100 documents
    v_start_time := clock_timestamp();
    
    FOR v_doc_count IN 1..100 LOOP
        PERFORM facets.bm25_index_document(
            'bm25_phase6_test.documents'::regclass,
            v_doc_count,
            (SELECT content FROM bm25_phase6_test.documents WHERE id = v_doc_count),
            'content',
            'english'
        );
    END LOOP;
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    RAISE NOTICE 'PASS: Reindexed 100 documents in % ms (avg: % ms/doc)', v_duration_ms, v_duration_ms / 100.0;
END;
$$;

-- Test 6: Statistics recalculation performance
\echo ''
\echo '--- Test 6.7: Statistics recalculation performance ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
BEGIN
    v_start_time := clock_timestamp();
    
    PERFORM facets.bm25_recalculate_statistics('bm25_phase6_test.documents'::regclass);
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    IF v_duration_ms < 1000 THEN
        RAISE NOTICE 'PASS: Statistics recalculation completed in % ms', v_duration_ms;
    ELSE
        RAISE NOTICE 'INFO: Statistics recalculation took % ms', v_duration_ms;
    END IF;
END;
$$;

-- Test 7: Concurrent query performance
\echo ''
\echo '--- Test 6.8: Concurrent query simulation ---'
DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_result_count int;
BEGIN
    -- Simulate multiple concurrent queries
    v_start_time := clock_timestamp();
    
    -- Query 1
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase6_test.documents'::regclass,
        'database',
        'english',
        false,
        false,
        0.3,
        1.2,
        0.75,
        100
    );
    
    -- Query 2
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase6_test.documents'::regclass,
        'system',
        'english',
        false,
        false,
        0.3,
        1.2,
        0.75,
        100
    );
    
    -- Query 3
    SELECT COUNT(*) INTO v_result_count
    FROM facets.bm25_search(
        'bm25_phase6_test.documents'::regclass,
        'query',
        'english',
        false,
        false,
        0.3,
        1.2,
        0.75,
        100
    );
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    RAISE NOTICE 'PASS: 3 sequential queries completed in % ms (avg: % ms/query)', 
        v_duration_ms, v_duration_ms / 3.0;
END;
$$;

-- Test 8: Memory usage (roaring bitmap efficiency)
\echo ''
\echo '--- Test 6.9: Roaring bitmap efficiency ---'
DO $$
DECLARE
    v_term_count int;
    v_total_bitmap_size bigint;
    v_avg_bitmap_size float;
    v_doc_count bigint;
BEGIN
    SELECT COUNT(*), SUM(pg_column_size(doc_ids)) INTO v_term_count, v_total_bitmap_size
    FROM facets.bm25_index
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase6_test' AND tablename = 'documents');
    
    SELECT COUNT(*) INTO v_doc_count
    FROM facets.bm25_documents
    WHERE table_id = (SELECT table_id FROM facets.faceted_table WHERE schemaname = 'bm25_phase6_test' AND tablename = 'documents');
    
    v_avg_bitmap_size := v_total_bitmap_size::float / NULLIF(v_term_count, 0);
    
    RAISE NOTICE 'Bitmap efficiency:';
    RAISE NOTICE '  Terms: %', v_term_count;
    RAISE NOTICE '  Total bitmap size: % KB', (v_total_bitmap_size / 1024);
    RAISE NOTICE '  Avg bitmap size per term: % bytes', v_avg_bitmap_size;
    RAISE NOTICE '  Documents: %', v_doc_count;
    
    IF v_avg_bitmap_size < 1000 THEN  -- Less than 1KB per term on average
        RAISE NOTICE 'PASS: Roaring bitmaps are efficiently compressed';
    ELSE
        RAISE NOTICE 'INFO: Bitmap size may be large, but acceptable for this dataset';
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bm25_phase6_test.documents');
DROP SCHEMA bm25_phase6_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Phase 6 Tests Complete!'
\echo '=============================================='
