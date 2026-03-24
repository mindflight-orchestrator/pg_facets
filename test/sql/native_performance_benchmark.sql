-- pg_facets Native Tokenization Performance Benchmark
-- Compares native worker vs SQL worker performance on larger datasets
-- Run this to validate performance improvements

\echo '=============================================='
\echo 'Native Tokenization Performance Benchmark'
\echo '=============================================='
\echo ''

-- Setup benchmark schema
DROP SCHEMA IF EXISTS benchmark_native CASCADE;
CREATE SCHEMA benchmark_native;

-- Create benchmark table
CREATE TABLE benchmark_native.documents (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL
);

\echo '--- Generating Test Data ---'
-- Generate larger dataset for meaningful benchmarks
-- Adjust the generate_series range to control dataset size
INSERT INTO benchmark_native.documents (content)
SELECT 
    'Document number ' || i || '. ' ||
    CASE (i % 10)
        WHEN 0 THEN 'PostgreSQL database optimization techniques and best practices for performance tuning and query optimization.'
        WHEN 1 THEN 'Full text search implementation using pg_facets extension for fast retrieval and efficient indexing.'
        WHEN 2 THEN 'Machine learning algorithms and data science fundamentals introduction to neural networks and deep learning.'
        WHEN 3 THEN 'Web development frameworks comparison React Angular Vue JavaScript TypeScript modern frontend development.'
        WHEN 4 THEN 'Cloud computing services AWS Azure GCP infrastructure deployment containerization and microservices architecture.'
        WHEN 5 THEN 'Software engineering principles clean code design patterns SOLID principles and agile development methodologies.'
        WHEN 6 THEN 'Database administration backup recovery replication high availability disaster recovery planning and implementation.'
        WHEN 7 THEN 'Network security encryption authentication authorization firewall configuration and cybersecurity best practices.'
        WHEN 8 THEN 'DevOps practices continuous integration continuous deployment automation monitoring and infrastructure as code.'
        WHEN 9 THEN 'Data analytics business intelligence data visualization reporting dashboards and statistical analysis techniques.'
    END
FROM generate_series(1, 1000) AS s(i);  -- Start with 1000, increase for larger tests

\echo 'Generated ' || (SELECT COUNT(*) FROM benchmark_native.documents) || ' test documents';
\echo ''

-- Register table
SELECT facets.add_faceting_to_table(
    'benchmark_native.documents'::regclass,
    key => 'id',
    facets => ARRAY[]::facets.facet_definition[],
    populate => false
);

\echo '--- Benchmark 1: Native Worker Performance ---'
DO $$
DECLARE
    v_table_id oid;
    v_source_staging text;
    v_output_staging text;
    v_total_docs bigint;
    v_start_time timestamptz;
    v_elapsed interval;
    v_result record;
    v_terms_per_sec float;
    v_docs_per_sec float;
BEGIN
    v_table_id := 'benchmark_native.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM benchmark_native.documents);
    
    -- Create staging tables
    v_source_staging := 'bm25_src_bench_' || extract(epoch from now())::bigint;
    v_output_staging := 'bm25_w_native_bench_' || extract(epoch from now())::bigint;
    
    EXECUTE format('CREATE UNLOGGED TABLE facets.%I (doc_id bigint, content text, rn bigint)', v_source_staging);
    EXECUTE format(
        'INSERT INTO facets.%I (doc_id, content, rn) SELECT id, content, row_number() OVER (ORDER BY id) FROM benchmark_native.documents',
        v_source_staging
    );
    EXECUTE format(
        'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
        v_output_staging
    );
    
    -- Benchmark native worker
    v_start_time := clock_timestamp();
    
    SELECT * INTO v_result
    FROM facets.bm25_index_worker_native(
        v_table_id, v_source_staging, v_output_staging, 'english', v_total_docs, 1, 1
    );
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    v_docs_per_sec := v_result.docs_indexed::float / NULLIF(EXTRACT(EPOCH FROM v_elapsed), 0);
    v_terms_per_sec := v_result.terms_extracted::float / NULLIF(EXTRACT(EPOCH FROM v_elapsed), 0);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Native Worker Results:';
    RAISE NOTICE '  Documents processed: %', v_result.docs_indexed;
    RAISE NOTICE '  Terms extracted: %', v_result.terms_extracted;
    RAISE NOTICE '  Time elapsed: %', v_elapsed;
    RAISE NOTICE '  Throughput: %.2f docs/sec, %.2f terms/sec', v_docs_per_sec, v_terms_per_sec;
    RAISE NOTICE '========================================';
    
    -- Cleanup
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_source_staging);
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging);
END $$;

\echo ''
\echo '--- Benchmark 2: SQL Worker Performance (if available) ---'
DO $$
DECLARE
    v_has_sql_worker boolean;
    v_table_id oid;
    v_source_staging text;
    v_output_staging text;
    v_total_docs bigint;
    v_start_time timestamptz;
    v_elapsed interval;
    v_result record;
    v_terms_per_sec float;
    v_docs_per_sec float;
    v_speedup float;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_worker_lockfree'
    ) INTO v_has_sql_worker;
    
    IF NOT v_has_sql_worker THEN
        RAISE NOTICE 'SKIP: SQL worker not available for comparison';
        RETURN;
    END IF;
    
    v_table_id := 'benchmark_native.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM benchmark_native.documents);
    
    -- Create staging tables
    v_source_staging := 'bm25_src_bench_sql_' || extract(epoch from now())::bigint;
    v_output_staging := 'bm25_w_sql_bench_' || extract(epoch from now())::bigint;
    
    EXECUTE format('CREATE UNLOGGED TABLE facets.%I (doc_id bigint, content text, rn bigint)', v_source_staging);
    EXECUTE format(
        'INSERT INTO facets.%I (doc_id, content, rn) SELECT id, content, row_number() OVER (ORDER BY id) FROM benchmark_native.documents',
        v_source_staging
    );
    EXECUTE format(
        'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
        v_output_staging
    );
    
    -- Benchmark SQL worker
    v_start_time := clock_timestamp();
    
    SELECT * INTO v_result
    FROM facets.bm25_index_worker_lockfree(
        v_table_id, v_source_staging, v_output_staging, 'english', v_total_docs, 1, 1
    );
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    v_docs_per_sec := v_result.docs_indexed::float / NULLIF(EXTRACT(EPOCH FROM v_elapsed), 0);
    v_terms_per_sec := v_result.terms_extracted::float / NULLIF(EXTRACT(EPOCH FROM v_elapsed), 0);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'SQL Worker Results:';
    RAISE NOTICE '  Documents processed: %', v_result.docs_indexed;
    RAISE NOTICE '  Terms extracted: %', v_result.terms_extracted;
    RAISE NOTICE '  Time elapsed: %', v_elapsed;
    RAISE NOTICE '  Throughput: %.2f docs/sec, %.2f terms/sec', v_docs_per_sec, v_terms_per_sec;
    RAISE NOTICE '========================================';
    
    -- Get native worker time for comparison (would need to store from previous benchmark)
    -- For now, just report SQL worker performance
    
    -- Cleanup
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_source_staging);
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging);
END $$;

\echo ''
\echo '--- Benchmark 3: Batch Insert Efficiency ---'
DO $$
DECLARE
    v_table_id oid;
    v_source_staging text;
    v_output_staging text;
    v_total_docs bigint;
    v_start_time timestamptz;
    v_elapsed interval;
    v_result record;
    v_term_count bigint;
    v_estimated_inserts int;
    v_batch_size int := 10000;
BEGIN
    v_table_id := 'benchmark_native.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM benchmark_native.documents);
    
    -- Create staging tables
    v_source_staging := 'bm25_src_batch_' || extract(epoch from now())::bigint;
    v_output_staging := 'bm25_w_batch_' || extract(epoch from now())::bigint;
    
    EXECUTE format('CREATE UNLOGGED TABLE facets.%I (doc_id bigint, content text, rn bigint)', v_source_staging);
    EXECUTE format(
        'INSERT INTO facets.%I (doc_id, content, rn) SELECT id, content, row_number() OVER (ORDER BY id) FROM benchmark_native.documents',
        v_source_staging
    );
    EXECUTE format(
        'CREATE UNLOGGED TABLE facets.%I (term_hash bigint NOT NULL, term_text text NOT NULL, doc_id bigint NOT NULL, term_freq int NOT NULL, doc_length int NOT NULL DEFAULT 0)',
        v_output_staging
    );
    
    -- Run worker
    v_start_time := clock_timestamp();
    
    SELECT * INTO v_result
    FROM facets.bm25_index_worker_native(
        v_table_id, v_source_staging, v_output_staging, 'english', v_total_docs, 1, 1
    );
    
    v_elapsed := clock_timestamp() - v_start_time;
    
    -- Count terms inserted
    EXECUTE format('SELECT COUNT(*) FROM facets.%I', v_output_staging) INTO v_term_count;
    
    -- Estimate number of INSERT statements (with batch size of 10K)
    v_estimated_inserts := CEIL(v_term_count::float / v_batch_size);
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Batch Insert Analysis:';
    RAISE NOTICE '  Total terms inserted: %', v_term_count;
    RAISE NOTICE '  Estimated INSERT statements: % (batch size: %)', v_estimated_inserts, v_batch_size;
    RAISE NOTICE '  Without batching: would need % INSERT statements', v_term_count;
    RAISE NOTICE '  Reduction factor: %.1fx', v_term_count::float / NULLIF(v_estimated_inserts, 0);
    RAISE NOTICE '  Time elapsed: %', v_elapsed;
    RAISE NOTICE '========================================';
    
    -- Cleanup
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_source_staging);
    EXECUTE format('DROP TABLE IF EXISTS facets.%I', v_output_staging);
END $$;

\echo ''
\echo '--- Benchmark 4: Parallel Worker Scaling ---'
DO $$
DECLARE
    v_has_dblink boolean;
    v_table_id oid;
    v_total_docs bigint;
    v_num_workers int;
    v_start_time timestamptz;
    v_elapsed interval;
    v_results record;
BEGIN
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'dblink') INTO v_has_dblink;
    
    IF NOT v_has_dblink THEN
        RAISE NOTICE 'SKIP: dblink not available, cannot test parallel scaling';
        RETURN;
    END IF;
    
    v_table_id := 'benchmark_native.documents'::regclass::oid;
    v_total_docs := (SELECT COUNT(*) FROM benchmark_native.documents);
    
    -- Test with different worker counts
    FOR v_num_workers IN 1..4 LOOP
        RAISE NOTICE 'Testing with % workers...', v_num_workers;
        
        v_start_time := clock_timestamp();
        
        -- Run parallel indexing
        SELECT * INTO v_results
        FROM facets.bm25_index_documents_parallel(
            v_table_id,
            'SELECT id::bigint AS doc_id, content FROM benchmark_native.documents',
            'content',
            'english',
            v_num_workers
        );
        
        v_elapsed := clock_timestamp() - v_start_time;
        
        RAISE NOTICE '  Workers: %, Time: %, Throughput: %.2f docs/sec', 
            v_num_workers, 
            v_elapsed,
            v_total_docs::float / NULLIF(EXTRACT(EPOCH FROM v_elapsed), 0);
    END LOOP;
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Parallel scaling test complete';
    RAISE NOTICE '========================================';
END $$;

\echo ''
\echo '=============================================='
\echo 'Performance Benchmark Complete'
\echo '=============================================='
\echo ''
\echo 'To test with larger datasets, modify the generate_series range in the test file.'
\echo 'For production-scale testing (12M documents), use the actual production tables.'

