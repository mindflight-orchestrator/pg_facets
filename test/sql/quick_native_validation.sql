-- Quick validation test for native tokenization
-- Run this first to verify basic functionality before running full test suite

\echo 'Quick Native Tokenization Validation'
\echo '===================================='
\echo ''

-- Quick test: Verify function exists and can be called
DO $$
DECLARE
    v_has_native boolean;
    v_has_sql boolean;
BEGIN
    -- Check native worker
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_worker_native'
    ) INTO v_has_native;
    
    -- Check SQL worker (for comparison)
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p 
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'facets' 
        AND p.proname = 'bm25_index_worker_lockfree'
    ) INTO v_has_sql;
    
    IF v_has_native THEN
        RAISE NOTICE '✓ PASS: bm25_index_worker_native function exists';
    ELSE
        RAISE WARNING '✗ FAIL: bm25_index_worker_native function not found';
        RAISE WARNING '  Make sure you have built and installed the extension with native support';
    END IF;
    
    IF v_has_sql THEN
        RAISE NOTICE '✓ INFO: bm25_index_worker_lockfree (SQL fallback) exists';
    ELSE
        RAISE NOTICE 'ℹ INFO: bm25_index_worker_lockfree not found (this is OK, native is preferred)';
    END IF;
END $$;

\echo ''
\echo 'Validation complete. Run native_tokenization_test.sql for full test suite.'
\echo ''

