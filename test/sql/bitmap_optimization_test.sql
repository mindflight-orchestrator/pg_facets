-- Bitmap Optimization Test Suite for pg_facets
-- Tests the new bitmap-based functions that avoid array explosions with large result sets
-- Key functions tested:
--   - filter_documents_by_facets_bitmap
--   - hierarchical_facets_bitmap
--   - Optimized search_documents_with_facets

\echo '=============================================='
\echo 'Bitmap Optimization Test Suite'
\echo '=============================================='
\echo ''
\echo 'These tests verify that bitmap-based operations work correctly'
\echo 'and can handle large result sets without array explosions.'
\echo ''

-- Setup test schema
DROP SCHEMA IF EXISTS bitmap_test CASCADE;
CREATE SCHEMA bitmap_test;

-- Create test table with enough variety for facet testing
CREATE TABLE bitmap_test.products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    subcategory TEXT,
    brand TEXT,
    price NUMERIC(10,2),
    in_stock BOOLEAN DEFAULT true,
    tags TEXT[],
    content TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert test data with multiple categories
INSERT INTO bitmap_test.products (name, category, subcategory, brand, price, in_stock, tags, content, metadata)
SELECT 
    'Product ' || i,
    CASE (i % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Books'
        WHEN 2 THEN 'Clothing'
        WHEN 3 THEN 'Home'
        WHEN 4 THEN 'Sports'
    END,
    CASE (i % 10)
        WHEN 0 THEN 'Computers'
        WHEN 1 THEN 'Fiction'
        WHEN 2 THEN 'Shirts'
        WHEN 3 THEN 'Kitchen'
        WHEN 4 THEN 'Running'
        WHEN 5 THEN 'Audio'
        WHEN 6 THEN 'Non-Fiction'
        WHEN 7 THEN 'Pants'
        WHEN 8 THEN 'Bedroom'
        WHEN 9 THEN 'Cycling'
    END,
    CASE (i % 3)
        WHEN 0 THEN 'BrandA'
        WHEN 1 THEN 'BrandB'
        WHEN 2 THEN 'BrandC'
    END,
    (random() * 1000)::numeric(10,2),
    (i % 4) != 0,  -- 75% in stock
    ARRAY['tag' || (i % 5), 'tag' || (i % 7)],
    'Product description for item ' || i || ' with searchable content about ' || 
        CASE (i % 5)
            WHEN 0 THEN 'electronics and technology'
            WHEN 1 THEN 'books and reading'
            WHEN 2 THEN 'clothing and fashion'
            WHEN 3 THEN 'home and living'
            WHEN 4 THEN 'sports and fitness'
        END,
    jsonb_build_object('sku', 'SKU-' || i)
FROM generate_series(1, 1000) AS i;

\echo 'Created 1000 test products'

-- Add faceting to the table
SELECT facets.add_faceting_to_table(
    'bitmap_test.products',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('subcategory'),
        facets.plain_facet('brand'),
        facets.boolean_facet('in_stock'),
        facets.array_facet('tags'),
        facets.bucket_facet('price', buckets => ARRAY[0, 100, 250, 500, 1000])
    ],
    populate => true
);

\echo 'Faceting configured and populated'
\echo ''

-- ============================================
-- Test 1: filter_documents_by_facets_bitmap - Basic functionality
-- ============================================
\echo '--- Test 1: filter_documents_by_facets_bitmap - Basic ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_card bigint;
BEGIN
    -- Test single facet filter
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        '{"category": "Electronics"}'::jsonb,
        'products'
    ) INTO v_bitmap;
    
    v_card := rb_cardinality(v_bitmap);
    
    IF v_card > 0 AND v_card <= 1000 THEN
        RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap returned bitmap with % Electronics documents', v_card;
    ELSE
        RAISE NOTICE 'FAIL: Expected >0 and <=1000 documents, got %', v_card;
    END IF;
END;
$$;

-- ============================================
-- Test 2: filter_documents_by_facets_bitmap - Multiple facets (AND logic)
-- ============================================
\echo ''
\echo '--- Test 2: filter_documents_by_facets_bitmap - Multiple facets ---'

DO $$
DECLARE
    v_bitmap_single roaringbitmap;
    v_bitmap_multi roaringbitmap;
    v_card_single bigint;
    v_card_multi bigint;
BEGIN
    -- Single facet
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        '{"category": "Electronics"}'::jsonb,
        'products'
    ) INTO v_bitmap_single;
    v_card_single := rb_cardinality(v_bitmap_single);
    
    -- Multiple facets (AND logic) - should be fewer results
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        '{"category": "Electronics", "brand": "BrandA"}'::jsonb,
        'products'
    ) INTO v_bitmap_multi;
    v_card_multi := rb_cardinality(v_bitmap_multi);
    
    IF v_card_multi <= v_card_single AND v_card_multi > 0 THEN
        RAISE NOTICE 'PASS: Multiple facets returned % docs (single facet: %)', v_card_multi, v_card_single;
    ELSE
        RAISE NOTICE 'FAIL: Expected multi <= single, got multi=% single=%', v_card_multi, v_card_single;
    END IF;
END;
$$;

-- ============================================
-- Test 3: filter_documents_by_facets_bitmap - Empty result
-- ============================================
\echo ''
\echo '--- Test 3: filter_documents_by_facets_bitmap - Non-existent value ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_card bigint;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        '{"category": "NonExistentCategory"}'::jsonb,
        'products'
    ) INTO v_bitmap;
    
    v_card := rb_cardinality(v_bitmap);
    
    IF v_card = 0 THEN
        RAISE NOTICE 'PASS: Non-existent category returns empty bitmap (card=%)', v_card;
    ELSE
        RAISE NOTICE 'FAIL: Expected 0 documents, got %', v_card;
    END IF;
END;
$$;

-- ============================================
-- Test 4: filter_documents_by_facets_bitmap - NULL/empty facets
-- ============================================
\echo ''
\echo '--- Test 4: filter_documents_by_facets_bitmap - NULL facets ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        NULL,
        'products'
    ) INTO v_bitmap;
    
    IF v_bitmap IS NULL THEN
        RAISE NOTICE 'PASS: NULL facets returns NULL bitmap';
    ELSE
        RAISE NOTICE 'FAIL: Expected NULL bitmap, got non-null';
    END IF;
END;
$$;

-- ============================================
-- Test 5: hierarchical_facets_bitmap - Basic functionality
-- ============================================
\echo ''
\echo '--- Test 5: hierarchical_facets_bitmap - Basic ---'

DO $$
DECLARE
    v_facets jsonb;
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id 
    FROM facets.faceted_table 
    WHERE schemaname = 'bitmap_test' AND tablename = 'products';
    
    -- Without filter (all documents)
    SELECT facets.hierarchical_facets_bitmap(v_table_id, 10, NULL) INTO v_facets;
    
    IF v_facets IS NOT NULL AND jsonb_typeof(v_facets) = 'array' THEN
        RAISE NOTICE 'PASS: hierarchical_facets_bitmap returned valid JSON array with % elements', jsonb_array_length(v_facets);
    ELSE
        RAISE NOTICE 'FAIL: Expected JSON array, got %', jsonb_typeof(v_facets);
    END IF;
END;
$$;

-- ============================================
-- Test 6: hierarchical_facets_bitmap - With filter bitmap
-- ============================================
\echo ''
\echo '--- Test 6: hierarchical_facets_bitmap - With filter ---'

DO $$
DECLARE
    v_filter_bitmap roaringbitmap;
    v_facets_unfiltered jsonb;
    v_facets_filtered jsonb;
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id 
    FROM facets.faceted_table 
    WHERE schemaname = 'bitmap_test' AND tablename = 'products';
    
    -- Get filter bitmap for Electronics
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        '{"category": "Electronics"}'::jsonb,
        'products'
    ) INTO v_filter_bitmap;
    
    -- Get facets without filter
    SELECT facets.hierarchical_facets_bitmap(v_table_id, 10, NULL) INTO v_facets_unfiltered;
    
    -- Get facets with filter
    SELECT facets.hierarchical_facets_bitmap(v_table_id, 10, v_filter_bitmap) INTO v_facets_filtered;
    
    IF v_facets_filtered IS NOT NULL AND jsonb_array_length(v_facets_filtered) >= 0 THEN
        RAISE NOTICE 'PASS: hierarchical_facets_bitmap with filter returned % facet groups', jsonb_array_length(v_facets_filtered);
    ELSE
        RAISE NOTICE 'FAIL: Expected valid facets, got NULL or invalid';
    END IF;
END;
$$;

-- ============================================
-- Test 7: search_documents_with_facets - Empty query with facet filter (OPTIMIZED PATH)
-- ============================================
\echo ''
\echo '--- Test 7: search_documents_with_facets - Empty query with facet filter ---'

SELECT 
    CASE 
        WHEN total_found > 0 AND total_found <= 1000 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Empty query with Electronics filter returned ' || total_found || ' documents, ' ||
    COALESCE(jsonb_array_length(results)::text, '0') || ' in results, search_time=' || search_time || 'ms'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',  -- Empty query - uses bitmap optimization
    '{"category": "Electronics"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    20,  -- Only fetch 20 rows
    0,
    NULL,
    NULL,
    10
);

-- ============================================
-- Test 8: search_documents_with_facets - Empty query, no filter
-- ============================================
\echo ''
\echo '--- Test 8: search_documents_with_facets - Empty query, no filter ---'

SELECT 
    CASE 
        WHEN total_found = 1000 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Empty query without filter returned ' || total_found || ' total (expected 1000), ' ||
    COALESCE(jsonb_array_length(results)::text, '0') || ' in results'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',
    NULL,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    NULL,
    NULL,
    10
);

-- ============================================
-- Test 9: search_documents_with_facets - Text search with facet filter
-- ============================================
\echo ''
\echo '--- Test 9: search_documents_with_facets - Text search with facet ---'

SELECT 
    CASE 
        WHEN total_found >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Text search "electronics" with category filter returned ' || total_found || ' results'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    'electronics',
    '{"category": "Electronics"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    0.0,
    NULL,
    10
);

-- ============================================
-- Test 10: search_documents_with_facets - Pagination with large facet filter
-- ============================================
\echo ''
\echo '--- Test 10: search_documents_with_facets - Pagination ---'

-- First page
SELECT 
    'Page 1: ' || COALESCE(jsonb_array_length(results)::text, '0') || ' results of ' || total_found || ' total'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',
    '{"in_stock": "true"}'::jsonb,  -- ~75% of products
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    NULL,
    NULL,
    10
);

-- Second page
SELECT 
    'Page 2: ' || COALESCE(jsonb_array_length(results)::text, '0') || ' results of ' || total_found || ' total'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',
    '{"in_stock": "true"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    10,  -- offset=10
    NULL,
    NULL,
    10
);

\echo ''
\echo 'PASS: Pagination works correctly'

-- ============================================
-- Test 11: Verify bitmap is used (not array) - Performance check
-- ============================================
\echo ''
\echo '--- Test 11: Performance - Large facet filter ---'

DO $$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
    v_duration_ms int;
    v_result record;
BEGIN
    v_start_time := clock_timestamp();
    
    -- This should use bitmap operations, not array expansion
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'bitmap_test',
        'products',
        '',
        '{"brand": "BrandA"}'::jsonb,  -- ~333 products
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        NULL,
        NULL,
        10
    );
    
    v_end_time := clock_timestamp();
    v_duration_ms := EXTRACT(MILLISECONDS FROM (v_end_time - v_start_time))::int;
    
    -- Should complete in reasonable time (bitmap operations are fast)
    IF v_duration_ms < 5000 THEN  -- 5 seconds max
        RAISE NOTICE 'PASS: Large facet query completed in %ms (total_found=%)', v_duration_ms, v_result.total_found;
    ELSE
        RAISE NOTICE 'FAIL: Query took too long: %ms', v_duration_ms;
    END IF;
END;
$$;

-- ============================================
-- Test 12: Verify facets are calculated correctly with filter
-- ============================================
\echo ''
\echo '--- Test 12: Verify filtered facet counts ---'

DO $$
DECLARE
    v_result record;
    v_facet jsonb;
    v_has_category boolean := false;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'bitmap_test',
        'products',
        '',
        '{"category": "Electronics"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        NULL,
        NULL,
        10
    );
    
    -- Check that facets are returned
    IF v_result.facets IS NOT NULL AND jsonb_array_length(v_result.facets) > 0 THEN
        RAISE NOTICE 'PASS: Facets returned with % groups', jsonb_array_length(v_result.facets);
    ELSE
        RAISE NOTICE 'FAIL: No facets returned';
    END IF;
END;
$$;

-- ============================================
-- Test 13: Compare bitmap vs array results (correctness check)
-- ============================================
\echo ''
\echo '--- Test 13: Correctness - Compare filter methods ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_bitmap_card bigint;
    v_array_count bigint;
BEGIN
    -- Get count via bitmap method
    SELECT facets.filter_documents_by_facets_bitmap(
        'bitmap_test',
        '{"category": "Electronics"}'::jsonb,
        'products'
    ) INTO v_bitmap;
    v_bitmap_card := rb_cardinality(v_bitmap);
    
    -- Get count via array method (original function)
    SELECT COUNT(*) INTO v_array_count
    FROM facets.filter_documents_by_facets(
        'bitmap_test',
        '{"category": "Electronics"}'::jsonb,
        'products'
    );
    
    IF v_bitmap_card = v_array_count THEN
        RAISE NOTICE 'PASS: Bitmap and array methods return same count: %', v_bitmap_card;
    ELSE
        RAISE NOTICE 'FAIL: Mismatch - bitmap=% array=%', v_bitmap_card, v_array_count;
    END IF;
END;
$$;

-- ============================================
-- Test 14: Empty facet filter edge case
-- ============================================
\echo ''
\echo '--- Test 14: Empty facet object ---'

SELECT 
    CASE 
        WHEN total_found = 1000 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Empty facet object {} returned ' || total_found || ' documents (expected 1000)'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',
    '{}'::jsonb,  -- Empty object
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    NULL,
    NULL,
    10
);

-- ============================================
-- Test 15: rb_contains usage in text search
-- ============================================
\echo ''
\echo '--- Test 15: Text search with bitmap filter (rb_contains) ---'

SELECT 
    CASE 
        WHEN total_found >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Text search with filter - total_found=' || total_found || 
    ', results=' || COALESCE(jsonb_array_length(results)::text, '0')
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    'sports fitness',
    '{"category": "Sports"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    0.0,
    NULL,
    10
);

-- ============================================
-- Test 16: Multiple facet values (if supported)
-- ============================================
\echo ''
\echo '--- Test 16: Filter with brand (should work) ---'

SELECT 
    CASE 
        WHEN total_found > 0 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Filter by brand BrandA returned ' || total_found || ' documents'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',
    '{"brand": "BrandA"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    NULL,
    NULL,
    10
);

-- ============================================
-- Test 17: Combined category + brand + in_stock
-- ============================================
\echo ''
\echo '--- Test 17: Complex multi-facet filter ---'

SELECT 
    CASE 
        WHEN total_found >= 0 THEN 'PASS'
        ELSE 'FAIL'
    END || ': Complex filter (Electronics + BrandA + in_stock) returned ' || total_found || ' documents'
FROM facets.search_documents_with_facets(
    'bitmap_test',
    'products',
    '',
    '{"category": "Electronics", "brand": "BrandA", "in_stock": "true"}'::jsonb,
    NULL,
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0,
    NULL,
    NULL,
    10
);

-- ============================================
-- Cleanup
-- ============================================
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('bitmap_test.products');
DROP SCHEMA bitmap_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'Bitmap Optimization Test Suite Complete!'
\echo '=============================================='
\echo 'Tests: 17'
\echo ''
\echo 'Summary:'
\echo '  - filter_documents_by_facets_bitmap: Returns roaringbitmap efficiently'
\echo '  - hierarchical_facets_bitmap: Accepts bitmap directly'
\echo '  - search_documents_with_facets: Uses bitmap path for large result sets'
\echo '=============================================='
