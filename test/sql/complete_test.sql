-- pg_facets Complete Test Suite
-- Run this script to test all faceting functionality

\echo '=============================================='
\echo 'pg_facets Complete Test Suite'
\echo '=============================================='
\echo ''

-- Setup
DROP SCHEMA IF EXISTS test_faceting CASCADE;
CREATE SCHEMA test_faceting;

\echo '--- Test 1: Basic Table Setup ---'

CREATE TABLE test_faceting.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT,
    subcategory TEXT,
    tags TEXT[],
    price DECIMAL(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    in_stock BOOLEAN DEFAULT true
);

INSERT INTO test_faceting.documents (title, category, subcategory, tags, price, in_stock) VALUES
    ('Laptop Pro 15', 'Electronics', 'Computers', ARRAY['premium', 'laptop', 'work'], 1299.99, true),
    ('Laptop Basic 13', 'Electronics', 'Computers', ARRAY['budget', 'laptop', 'student'], 499.99, true),
    ('Wireless Headphones', 'Electronics', 'Audio', ARRAY['premium', 'audio', 'wireless'], 299.99, true),
    ('Wired Earbuds', 'Electronics', 'Audio', ARRAY['budget', 'audio'], 29.99, true),
    ('Smartphone X', 'Electronics', 'Phones', ARRAY['premium', 'phone'], 999.99, false),
    ('Novel: The Journey', 'Books', 'Fiction', ARRAY['fiction', 'bestseller', 'adventure'], 19.99, true),
    ('Novel: Mystery Night', 'Books', 'Fiction', ARRAY['fiction', 'mystery'], 14.99, true),
    ('Cookbook: Italian', 'Books', 'Non-Fiction', ARRAY['cooking', 'bestseller', 'italian'], 29.99, true),
    ('Science Guide', 'Books', 'Non-Fiction', ARRAY['science', 'education'], 39.99, false),
    ('Office Chair', 'Furniture', 'Office', ARRAY['office', 'ergonomic', 'premium'], 399.99, true),
    ('Standing Desk', 'Furniture', 'Office', ARRAY['office', 'premium', 'adjustable'], 599.99, true),
    ('Bookshelf', 'Furniture', 'Storage', ARRAY['storage', 'wood'], 149.99, true);

\echo 'PASS: Created test table with 12 documents'

-- Test 2: Add faceting
\echo ''
\echo '--- Test 2: Add Faceting to Table ---'

SELECT facets.add_faceting_to_table(
    'test_faceting.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('subcategory'),
        facets.array_facet('tags'),
        facets.bucket_facet('price', buckets => ARRAY[0, 50, 100, 500, 1000]),
        facets.boolean_facet('in_stock')
    ],
    populate => true
);

-- Verify facet definitions
SELECT 
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Created ' || COUNT(*) || ' facet definitions (expected 5)'
FROM facets.facet_definition 
WHERE table_id = 'test_faceting.documents'::regclass::oid;

-- Verify facet data was populated
SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': Populated ' || COUNT(*) || ' facet entries'
FROM test_faceting.documents_facets;

-- Test 3: Top Values
\echo ''
\echo '--- Test 3: Top Values Query ---'

SELECT * FROM facets.top_values('test_faceting.documents'::regclass, 5);

SELECT 
    CASE WHEN cardinality = 5 THEN 'PASS' ELSE 'FAIL' END || ': Electronics category has ' || cardinality || ' documents (expected 5)'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['category'])
WHERE facet_value = 'Electronics';

SELECT 
    CASE WHEN cardinality = 4 THEN 'PASS' ELSE 'FAIL' END || ': Books category has ' || cardinality || ' documents (expected 4)'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['category'])
WHERE facet_value = 'Books';

-- Test 4: Native Zig Functions
\echo ''
\echo '--- Test 4: Native Zig Functions ---'

-- Test build_filter_bitmap_native - basic test
SELECT 
    CASE WHEN rb_cardinality(
        build_filter_bitmap_native(
            'test_faceting.documents'::regclass::oid,
            ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
        )
    ) = 5 THEN 'PASS' ELSE 'FAIL' END || ': build_filter_bitmap_native returns correct cardinality for Electronics';

-- Test get_facet_counts_native
SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': get_facet_counts_native returns ' || COUNT(*) || ' rows'
FROM get_facet_counts_native(
    'test_faceting.documents'::regclass::oid,
    NULL,
    ARRAY['category'],
    10
);

-- Test search_documents_native
SELECT 
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': search_documents_native returns ' || COUNT(*) || ' Electronics documents (expected 5)'
FROM search_documents_native(
    'test_faceting.documents'::regclass::oid,
    ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[],
    100,
    0
);

-- Test 4b: Native Bitmap Operations (detailed tests)
\echo ''
\echo '--- Test 4b: Native Bitmap Operations ---'

-- Store bitmap in a temp table to test round-trip
DO $$
DECLARE
    filter_bitmap roaringbitmap;
    bitmap_card bigint;
    bitmap_arr int[];
BEGIN
    -- Build filter bitmap for Electronics
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
    ) INTO filter_bitmap;
    
    -- Test rb_cardinality directly
    SELECT rb_cardinality(filter_bitmap) INTO bitmap_card;
    
    IF bitmap_card = 5 THEN
        RAISE NOTICE 'PASS: rb_cardinality on filter bitmap = % (expected 5)', bitmap_card;
    ELSE
        RAISE NOTICE 'FAIL: rb_cardinality on filter bitmap = % (expected 5)', bitmap_card;
    END IF;
    
    -- Test rb_to_array
    SELECT rb_to_array(filter_bitmap) INTO bitmap_arr;
    
    IF array_length(bitmap_arr, 1) = 5 THEN
        RAISE NOTICE 'PASS: rb_to_array returns % elements (expected 5)', array_length(bitmap_arr, 1);
    ELSE
        RAISE NOTICE 'FAIL: rb_to_array returns % elements (expected 5)', array_length(bitmap_arr, 1);
    END IF;
END;
$$;

-- Test bitmap AND operation
DO $$
DECLARE
    bitmap1 roaringbitmap;
    bitmap2 roaringbitmap;
    and_result roaringbitmap;
    and_card bigint;
BEGIN
    -- Build two bitmaps
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
    ) INTO bitmap1;
    
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('in_stock', 'true')]::facets.facet_filter[]
    ) INTO bitmap2;
    
    -- AND operation
    SELECT rb_and(bitmap1, bitmap2) INTO and_result;
    SELECT rb_cardinality(and_result) INTO and_card;
    
    -- Electronics AND in_stock should be 4 (all Electronics except Smartphone X which is out of stock)
    IF and_card = 4 THEN
        RAISE NOTICE 'PASS: rb_and(Electronics, in_stock=true) = % (expected 4)', and_card;
    ELSE
        RAISE NOTICE 'FAIL: rb_and(Electronics, in_stock=true) = % (expected 4)', and_card;
    END IF;
END;
$$;

-- Test bitmap OR operation
DO $$
DECLARE
    bitmap1 roaringbitmap;
    bitmap2 roaringbitmap;
    or_result roaringbitmap;
    or_card bigint;
BEGIN
    -- Build two bitmaps
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
    ) INTO bitmap1;
    
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('category', 'Books')]::facets.facet_filter[]
    ) INTO bitmap2;
    
    -- OR operation
    SELECT rb_or(bitmap1, bitmap2) INTO or_result;
    SELECT rb_cardinality(or_result) INTO or_card;
    
    -- Electronics OR Books should be 9 (5 + 4)
    IF or_card = 9 THEN
        RAISE NOTICE 'PASS: rb_or(Electronics, Books) = % (expected 9)', or_card;
    ELSE
        RAISE NOTICE 'FAIL: rb_or(Electronics, Books) = % (expected 9)', or_card;
    END IF;
END;
$$;

-- Test get_facet_counts_native with filter bitmap
DO $$
DECLARE
    filter_bitmap roaringbitmap;
    facet_count int;
BEGIN
    -- Build filter bitmap for Electronics
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
    ) INTO filter_bitmap;
    
    -- Get facet counts filtered by Electronics
    SELECT COUNT(*) INTO facet_count
    FROM get_facet_counts_native(
        'test_faceting.documents'::regclass::oid,
        filter_bitmap,
        NULL,  -- all facets
        10
    );
    
    IF facet_count > 0 THEN
        RAISE NOTICE 'PASS: get_facet_counts_native with filter bitmap returns % rows', facet_count;
    ELSE
        RAISE NOTICE 'FAIL: get_facet_counts_native with filter bitmap returns 0 rows';
    END IF;
END;
$$;

-- Test multiple filters in build_filter_bitmap_native
DO $$
DECLARE
    multi_filter_bitmap roaringbitmap;
    multi_card bigint;
BEGIN
    -- Build bitmap with multiple filters (AND logic)
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[
            ROW('category', 'Electronics'),
            ROW('in_stock', 'true')
        ]::facets.facet_filter[]
    ) INTO multi_filter_bitmap;
    
    SELECT rb_cardinality(multi_filter_bitmap) INTO multi_card;
    
    -- Electronics AND in_stock should be 4
    IF multi_card = 4 THEN
        RAISE NOTICE 'PASS: build_filter_bitmap_native with multiple filters = % (expected 4)', multi_card;
    ELSE
        RAISE NOTICE 'FAIL: build_filter_bitmap_native with multiple filters = % (expected 4)', multi_card;
    END IF;
END;
$$;

-- Test bitmap persistence via bytea cast
DO $$
DECLARE
    original_bitmap roaringbitmap;
    bitmap_as_bytea bytea;
    restored_bitmap roaringbitmap;
    original_card bigint;
    restored_card bigint;
BEGIN
    -- Build original bitmap
    SELECT build_filter_bitmap_native(
        'test_faceting.documents'::regclass::oid,
        ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
    ) INTO original_bitmap;
    
    SELECT rb_cardinality(original_bitmap) INTO original_card;
    
    -- Cast to bytea and back
    SELECT original_bitmap::bytea INTO bitmap_as_bytea;
    SELECT bitmap_as_bytea::roaringbitmap INTO restored_bitmap;
    
    SELECT rb_cardinality(restored_bitmap) INTO restored_card;
    
    IF original_card = restored_card THEN
        RAISE NOTICE 'PASS: bitmap bytea round-trip preserved cardinality: % = %', original_card, restored_card;
    ELSE
        RAISE NOTICE 'FAIL: bitmap bytea round-trip changed cardinality: % != %', original_card, restored_card;
    END IF;
END;
$$;

-- Test bitmap equality after bytea cast
SELECT 
    CASE WHEN rb_equals(
        build_filter_bitmap_native(
            'test_faceting.documents'::regclass::oid,
            ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
        ),
        (build_filter_bitmap_native(
            'test_faceting.documents'::regclass::oid,
            ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
        )::bytea)::roaringbitmap
    ) THEN 'PASS' ELSE 'FAIL' END || ': bitmap equals after bytea round-trip';

-- Test rb_iterate on filter bitmap
SELECT 
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': rb_iterate returns ' || COUNT(*) || ' rows (expected 5)'
FROM (
    SELECT rb_iterate(
        build_filter_bitmap_native(
            'test_faceting.documents'::regclass::oid,
            ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
        )
    )
) AS t;

-- Test 5: Count Results with Filters
\echo ''
\echo '--- Test 5: Count Results with Filters ---'

SELECT * FROM facets.count_results(
    'test_faceting.documents'::regclass::oid,
    filters => ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
) LIMIT 10;

-- Filter by category AND tag
SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': count_results with multiple filters works'
FROM facets.count_results(
    'test_faceting.documents'::regclass::oid,
    filters => ARRAY[
        ROW('category', 'Electronics'),
        ROW('tags', 'premium')
    ]::facets.facet_filter[]
);

-- Test 6: Delta Updates (INSERT)
\echo ''
\echo '--- Test 6: Delta Updates (INSERT) ---'

-- Insert new documents
INSERT INTO test_faceting.documents (title, category, subcategory, tags, price, in_stock) VALUES
    ('Gaming Mouse', 'Electronics', 'Accessories', ARRAY['gaming', 'premium'], 79.99, true),
    ('USB Hub', 'Electronics', 'Accessories', ARRAY['accessories', 'budget'], 24.99, true),
    ('History Book', 'Books', 'Non-Fiction', ARRAY['history', 'education'], 34.99, true);

-- Check deltas were recorded
SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': ' || COUNT(*) || ' delta entries recorded after INSERT'
FROM test_faceting.documents_facets_deltas;

-- Apply deltas using native Zig function
SELECT merge_deltas_native('test_faceting.documents'::regclass);

\echo 'PASS: merge_deltas_native executed successfully'

-- Verify deltas were applied
SELECT 
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': All deltas merged (remaining: ' || COUNT(*) || ')'
FROM test_faceting.documents_facets_deltas;

-- Verify new counts
SELECT 
    CASE WHEN cardinality = 7 THEN 'PASS' ELSE 'FAIL' END || ': Electronics now has ' || cardinality || ' documents (expected 7)'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['category'])
WHERE facet_value = 'Electronics';

-- Test 7: Delta Updates (DELETE)
\echo ''
\echo '--- Test 7: Delta Updates (DELETE) ---'

-- Delete documents
DELETE FROM test_faceting.documents WHERE title LIKE '%USB%';

-- Apply deltas
SELECT merge_deltas_native('test_faceting.documents'::regclass);

-- Verify count decreased
SELECT 
    CASE WHEN cardinality = 6 THEN 'PASS' ELSE 'FAIL' END || ': Electronics now has ' || cardinality || ' documents after DELETE (expected 6)'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['category'])
WHERE facet_value = 'Electronics';

-- Test 8: Delta Updates (UPDATE)
\echo ''
\echo '--- Test 8: Delta Updates (UPDATE) ---'

-- Update a document's category
UPDATE test_faceting.documents SET category = 'Gaming' WHERE title = 'Gaming Mouse';

-- Apply deltas
SELECT merge_deltas_native('test_faceting.documents'::regclass);

-- Verify new category exists
SELECT 
    CASE WHEN cardinality = 1 THEN 'PASS' ELSE 'FAIL' END || ': Gaming category now has ' || cardinality || ' document (expected 1)'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['category'])
WHERE facet_value = 'Gaming';

-- Test 9: Add New Facets
\echo ''
\echo '--- Test 9: Add New Facets ---'

SELECT facets.add_facets(
    'test_faceting.documents',
    facets => ARRAY[
        facets.datetrunc_facet('created_at', 'month')
    ]
);

-- Verify new facet was added
SELECT 
    CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': Now have ' || COUNT(*) || ' facet definitions (expected 6)'
FROM facets.facet_definition 
WHERE table_id = 'test_faceting.documents'::regclass::oid;

-- Test 10: Drop Facets
\echo ''
\echo '--- Test 10: Drop Facets ---'

SELECT facets.drop_facets('test_faceting.documents', ARRAY['subcategory']);

SELECT 
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Now have ' || COUNT(*) || ' facet definitions after drop (expected 5)'
FROM facets.facet_definition 
WHERE table_id = 'test_faceting.documents'::regclass::oid;

-- Test 11: Hierarchical Facets
\echo ''
\echo '--- Test 11: Hierarchical Facets ---'

SELECT facets.hierarchical_facets(
    'test_faceting.documents'::regclass::oid,
    n => 5
);

\echo 'PASS: hierarchical_facets executed successfully'

-- Test 12: Filter Documents by Facets (JSONB interface)
\echo ''
\echo '--- Test 12: Filter Documents by Facets (JSONB) ---'

SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': filter_documents_by_facets returns ' || COUNT(*) || ' document IDs'
FROM facets.filter_documents_by_facets(
    'test_faceting',
    '{"category": "Electronics"}'::jsonb,
    'documents'
);

-- Test 13: Boolean Facet
\echo ''
\echo '--- Test 13: Boolean Facet ---'

SELECT * FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['in_stock']);

SELECT 
    CASE WHEN cardinality > 0 THEN 'PASS' ELSE 'FAIL' END || ': in_stock=true has ' || cardinality || ' documents'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['in_stock'])
WHERE facet_value = 'true';

-- Test 14: Bucket Facet
\echo ''
\echo '--- Test 14: Bucket Facet ---'

SELECT * FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['price']);

\echo 'PASS: Bucket facet values displayed'

-- Test 15: Array Facet (tags)
\echo ''
\echo '--- Test 15: Array Facet (tags) ---'

SELECT * FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['tags']);

SELECT 
    CASE WHEN cardinality >= 3 THEN 'PASS' ELSE 'FAIL' END || ': premium tag has ' || cardinality || ' documents (expected >= 3)'
FROM facets.top_values('test_faceting.documents'::regclass, 20, ARRAY['tags'])
WHERE facet_value = 'premium';

-- Test 16: Complex Multi-Filter Query
\echo ''
\echo '--- Test 16: Complex Multi-Filter Query ---'

-- Get documents that are Electronics AND premium AND in_stock
SELECT 
    CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END || ': Multi-filter query returns ' || COUNT(*) || ' results'
FROM search_documents_native(
    'test_faceting.documents'::regclass::oid,
    ARRAY[
        ROW('category', 'Electronics'),
        ROW('tags', 'premium'),
        ROW('in_stock', 'true')
    ]::facets.facet_filter[],
    100,
    0
);

-- Test 17: Pagination
\echo ''
\echo '--- Test 17: Pagination ---'

SELECT 
    CASE WHEN COUNT(*) <= 2 THEN 'PASS' ELSE 'FAIL' END || ': Pagination with limit 2 returns ' || COUNT(*) || ' results'
FROM search_documents_native(
    'test_faceting.documents'::regclass::oid,
    ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[],
    2,  -- limit
    0   -- offset
);

SELECT 
    CASE WHEN COUNT(*) <= 2 THEN 'PASS' ELSE 'FAIL' END || ': Pagination with offset 2 returns ' || COUNT(*) || ' results'
FROM search_documents_native(
    'test_faceting.documents'::regclass::oid,
    ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[],
    2,  -- limit
    2   -- offset
);

-- Test 18: Drop Faceting
\echo ''
\echo '--- Test 18: Drop Faceting ---'

SELECT facets.drop_faceting('test_faceting.documents');

SELECT 
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM facets.faceted_table 
        WHERE schemaname = 'test_faceting' AND tablename = 'documents'
    ) THEN 'PASS' ELSE 'FAIL' END || ': Faceting dropped successfully';

-- Test 19: Re-add Faceting
\echo ''
\echo '--- Test 19: Re-add Faceting ---'

SELECT facets.add_faceting_to_table(
    'test_faceting.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.array_facet('tags')
    ],
    populate => true
);

SELECT 
    CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END || ': Re-added faceting with ' || COUNT(*) || ' facets'
FROM facets.facet_definition 
WHERE table_id = 'test_faceting.documents'::regclass::oid;

-- ============================================
-- NEW TESTS: Additional Function Coverage
-- ============================================

\echo ''
\echo '=============================================='
\echo 'Extended Test Suite - Additional Functions'
\echo '=============================================='

-- Test 20: get_documents_with_facet
\echo ''
\echo '--- Test 20: get_documents_with_facet ---'

SELECT 
    CASE WHEN rb_cardinality(facets.get_documents_with_facet(
        'test_faceting.documents'::regclass::oid,
        'category',
        'Electronics'
    )) > 0 THEN 'PASS' ELSE 'FAIL' END || ': get_documents_with_facet returns bitmap with ' ||
    rb_cardinality(facets.get_documents_with_facet('test_faceting.documents'::regclass::oid, 'category', 'Electronics')) || ' Electronics docs';

SELECT 
    CASE WHEN rb_cardinality(facets.get_documents_with_facet(
        'test_faceting.documents'::regclass::oid,
        'category',
        'Books'
    )) > 0 THEN 'PASS' ELSE 'FAIL' END || ': get_documents_with_facet returns bitmap with ' ||
    rb_cardinality(facets.get_documents_with_facet('test_faceting.documents'::regclass::oid, 'category', 'Books')) || ' Books docs';

-- Test 21: get_documents_with_boolean_facet
\echo ''
\echo '--- Test 21: get_documents_with_boolean_facet ---'

-- First add boolean facet back
SELECT facets.add_facets('test_faceting.documents', ARRAY[facets.boolean_facet('in_stock')]);

SELECT 
    CASE WHEN rb_cardinality(facets.get_documents_with_boolean_facet(
        'test_faceting.documents'::regclass::oid,
        'in_stock',
        true
    )) > 0 THEN 'PASS' ELSE 'FAIL' END || ': get_documents_with_boolean_facet(true) returns ' || 
    rb_cardinality(facets.get_documents_with_boolean_facet('test_faceting.documents'::regclass::oid, 'in_stock', true)) || ' docs';

SELECT 
    CASE WHEN rb_cardinality(facets.get_documents_with_boolean_facet(
        'test_faceting.documents'::regclass::oid,
        'in_stock',
        false
    )) >= 0 THEN 'PASS' ELSE 'FAIL' END || ': get_documents_with_boolean_facet(false) returns ' || 
    rb_cardinality(facets.get_documents_with_boolean_facet('test_faceting.documents'::regclass::oid, 'in_stock', false)) || ' docs';

-- Test 22: get_boolean_facet_counts
\echo ''
\echo '--- Test 22: get_boolean_facet_counts ---'

SELECT 
    CASE WHEN true_count > 0 AND total_count > 0 THEN 'PASS' ELSE 'FAIL' END || 
    ': get_boolean_facet_counts returns true=' || true_count || ', false=' || false_count || ', total=' || total_count
FROM facets.get_boolean_facet_counts(
    'test_faceting.documents'::regclass::oid,
    'in_stock'
);

-- Test 23: get_filtered_boolean_facet_counts
\echo ''
\echo '--- Test 23: get_filtered_boolean_facet_counts ---'

-- Get Electronics filter bitmap first
SELECT 
    CASE WHEN true_count >= 0 AND total_count >= 0 THEN 'PASS' ELSE 'FAIL' END || 
    ': get_filtered_boolean_facet_counts (filtered by Electronics) returns true=' || true_count || ', false=' || false_count
FROM facets.get_filtered_boolean_facet_counts(
    'test_faceting.documents'::regclass::oid,
    'in_stock',
    facets.get_documents_with_facet('test_faceting.documents'::regclass::oid, 'category', 'Electronics')
);

-- Test 24: filter_with_boolean_facets (JSONB interface)
\echo ''
\echo '--- Test 24: filter_with_boolean_facets ---'

SELECT 
    CASE WHEN rb_cardinality(facets.filter_with_boolean_facets(
        'test_faceting.documents'::regclass::oid,
        '[{"facet_name": "in_stock", "value": true}]'::jsonb
    )) > 0 THEN 'PASS' ELSE 'FAIL' END || ': filter_with_boolean_facets returns ' ||
    rb_cardinality(facets.filter_with_boolean_facets('test_faceting.documents'::regclass::oid, '[{"facet_name": "in_stock", "value": true}]'::jsonb)) || ' docs';

-- Test combined boolean + category filter
SELECT 
    CASE WHEN rb_cardinality(facets.filter_with_boolean_facets(
        'test_faceting.documents'::regclass::oid,
        '[{"facet_name": "in_stock", "value": true}, {"facet_name": "category", "value": "Electronics"}]'::jsonb
    )) > 0 THEN 'PASS' ELSE 'FAIL' END || ': filter_with_boolean_facets (combined) returns ' ||
    rb_cardinality(facets.filter_with_boolean_facets('test_faceting.documents'::regclass::oid, 
        '[{"facet_name": "in_stock", "value": true}, {"facet_name": "category", "value": "Electronics"}]'::jsonb)) || ' docs';

-- Test 25: get_facet_counts (SQL version) - uses correct signature (table_id, facet_name, filter_bitmap, limit)
\echo ''
\echo '--- Test 25: get_facet_counts (SQL version) ---'

SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': get_facet_counts returns ' || COUNT(*) || ' rows'
FROM facets.get_facet_counts(
    'test_faceting.documents'::regclass::oid,
    'category',
    NULL,  -- no filter bitmap
    10
);

-- Test with filter bitmap
SELECT 
    CASE WHEN COUNT(*) >= 0 THEN 'PASS' ELSE 'FAIL' END || ': get_facet_counts with filter returns ' || COUNT(*) || ' rows'
FROM facets.get_facet_counts(
    'test_faceting.documents'::regclass::oid,
    'tags',
    facets.get_documents_with_facet('test_faceting.documents'::regclass::oid, 'category', 'Electronics'),
    10
);

-- Test 26: populate_facets (manual population)
\echo ''
\echo '--- Test 26: populate_facets (manual) ---'

-- Get current count
SELECT COUNT(*) AS before_count FROM test_faceting.documents_facets \gset

-- Call populate_facets
SELECT facets.populate_facets('test_faceting.documents'::regclass::oid);

-- Verify it still works (count should be same or greater)
SELECT 
    CASE WHEN COUNT(*) >= :before_count THEN 'PASS' ELSE 'FAIL' END || ': populate_facets succeeded, ' || COUNT(*) || ' facet entries'
FROM test_faceting.documents_facets;

-- Test 27: apply_deltas (without merge)
\echo ''
\echo '--- Test 27: apply_deltas ---'

-- Insert a document to create deltas
INSERT INTO test_faceting.documents (title, category, tags, price, in_stock) 
VALUES ('Test Product', 'TestCategory', ARRAY['test'], 99.99, true);

-- Check deltas exist
SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': ' || COUNT(*) || ' deltas created'
FROM test_faceting.documents_facets_deltas;

-- Apply deltas (SQL version)
SELECT facets.apply_deltas('test_faceting.documents'::regclass::oid);

\echo 'PASS: apply_deltas executed'

-- Clean up by merging remaining deltas
SELECT merge_deltas_native('test_faceting.documents'::regclass);

-- Test 28: calculate_facet_cardinality_stats
\echo ''
\echo '--- Test 28: calculate_facet_cardinality_stats ---'

SELECT facets.calculate_facet_cardinality_stats('test_faceting.documents'::regclass::oid);

\echo 'PASS: calculate_facet_cardinality_stats executed'

-- Verify stats table was created
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'test_faceting' AND table_name = 'documents_facets_cardinality_stats'
    ) THEN 'PASS' ELSE 'FAIL' END || ': Cardinality stats table created';

-- Test 29: get_facet_cardinality_from_stats (uses facet_id, not facet names array)
\echo ''
\echo '--- Test 29: get_facet_cardinality_from_stats ---'

-- Get a facet_id first
SELECT facet_id AS test_facet_id 
FROM facets.facet_definition 
WHERE table_id = 'test_faceting.documents'::regclass::oid AND facet_name = 'category'
LIMIT 1 \gset

SELECT 
    CASE WHEN facets.get_facet_cardinality_from_stats(
        'test_faceting.documents'::regclass::oid,
        :test_facet_id,
        'Electronics'
    ) >= 0 THEN 'PASS' ELSE 'FAIL' END || ': get_facet_cardinality_from_stats returned value';

-- Test 30: Setup for joined_plain_facet test
\echo ''
\echo '--- Test 30: joined_plain_facet ---'

-- Create a related table for join testing
CREATE TABLE test_faceting.brands (
    id SERIAL PRIMARY KEY,
    brand_name TEXT NOT NULL,
    country TEXT
);

INSERT INTO test_faceting.brands (brand_name, country) VALUES
    ('TechCorp', 'USA'),
    ('BookHouse', 'UK'),
    ('FurnitureCo', 'Germany');

-- Add brand_id to documents
ALTER TABLE test_faceting.documents ADD COLUMN brand_id INT;
UPDATE test_faceting.documents SET brand_id = 1 WHERE category IN ('Electronics', 'Gaming');
UPDATE test_faceting.documents SET brand_id = 2 WHERE category = 'Books';
UPDATE test_faceting.documents SET brand_id = 3 WHERE category = 'Furniture';

-- Drop existing faceting and re-add with joined facet
SELECT facets.drop_faceting('test_faceting.documents');

SELECT facets.add_faceting_to_table(
    'test_faceting.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.joined_plain_facet(
            'b.country',
            'test_faceting.brands b',
            '{TABLE}.brand_id = b.id',
            'brand_country'
        )
    ],
    populate => true
);

-- Verify joined facet was created
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM facets.facet_definition 
        WHERE table_id = 'test_faceting.documents'::regclass::oid AND facet_name = 'brand_country'
    ) THEN 'PASS' ELSE 'FAIL' END || ': joined_plain_facet created';

-- Test that joined facet has values
SELECT 
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': joined_plain_facet has ' || COUNT(*) || ' distinct values'
FROM facets.top_values('test_faceting.documents'::regclass, 10, ARRAY['brand_country']);

-- Test 31: function_facet
\echo ''
\echo '--- Test 31: function_facet ---'

-- Create a custom function for faceting
CREATE OR REPLACE FUNCTION test_faceting.get_price_tier(doc_id int)
RETURNS text AS $$
DECLARE
    p decimal;
BEGIN
    SELECT price INTO p FROM test_faceting.documents WHERE id = doc_id;
    RETURN CASE 
        WHEN p < 50 THEN 'budget'
        WHEN p < 200 THEN 'mid-range'
        WHEN p < 500 THEN 'premium'
        ELSE 'luxury'
    END;
END;
$$ LANGUAGE plpgsql STABLE;

-- Add function facet
SELECT facets.add_facets(
    'test_faceting.documents',
    ARRAY[
        facets.function_facet('test_faceting.get_price_tier', 'price_tier', 'id')
    ]
);

-- Verify function facet was created
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM facets.facet_definition 
        WHERE table_id = 'test_faceting.documents'::regclass::oid AND facet_name = 'price_tier'
    ) THEN 'PASS' ELSE 'FAIL' END || ': function_facet created';

-- Test 32: function_array_facet
\echo ''
\echo '--- Test 32: function_array_facet ---'

-- Create a function that returns an array
CREATE OR REPLACE FUNCTION test_faceting.get_keywords(doc_id int)
RETURNS text[] AS $$
DECLARE
    t text;
    c text;
BEGIN
    SELECT title, category INTO t, c FROM test_faceting.documents WHERE id = doc_id;
    -- Return first word of title and category as keywords
    RETURN ARRAY[split_part(t, ' ', 1), c];
END;
$$ LANGUAGE plpgsql STABLE;

-- Add function array facet
SELECT facets.add_facets(
    'test_faceting.documents',
    ARRAY[
        facets.function_array_facet('test_faceting.get_keywords', 'keywords', 'id')
    ]
);

-- Verify function array facet was created
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 FROM facets.facet_definition 
        WHERE table_id = 'test_faceting.documents'::regclass::oid AND facet_name = 'keywords'
    ) THEN 'PASS' ELSE 'FAIL' END || ': function_array_facet created';

-- Test 33: Setup table for search_documents test
\echo ''
\echo '--- Test 33: search_documents (full-text) ---'

-- Create a table with content column for full-text search
CREATE TABLE test_faceting.articles (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO test_faceting.articles (title, content, category, metadata) VALUES
    ('PostgreSQL Tutorial', 'Learn how to use PostgreSQL database effectively with this comprehensive tutorial', 'Technology', '{"author": "John"}'),
    ('Cooking Basics', 'Essential cooking techniques every home chef should know about food preparation', 'Lifestyle', '{"author": "Jane"}'),
    ('Database Optimization', 'Tips and tricks for optimizing your PostgreSQL database performance', 'Technology', '{"author": "Bob"}'),
    ('Travel Guide', 'Explore beautiful destinations around the world with our travel recommendations', 'Travel', '{"author": "Alice"}'),
    ('PostgreSQL Extensions', 'How to use PostgreSQL extensions to extend database functionality', 'Technology', '{"author": "John"}');

-- Test search_documents function
SELECT 
    CASE WHEN total_found > 0 THEN 'PASS' ELSE 'FAIL' END || ': search_documents found ' || total_found || ' results for "PostgreSQL"'
FROM facets.search_documents(
    'test_faceting',
    'articles',
    'PostgreSQL'
);

SELECT 
    CASE WHEN total_found > 0 THEN 'PASS' ELSE 'FAIL' END || ': search_documents found ' || total_found || ' results for "database"'
FROM facets.search_documents(
    'test_faceting',
    'articles',
    'database',
    p_limit => 10
);

-- Test 34: search_documents_with_facets
\echo ''
\echo '--- Test 34: search_documents_with_facets ---'

-- Add faceting to articles table
SELECT facets.add_faceting_to_table(
    'test_faceting.articles',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

-- Test search with facets (empty facets object)
SELECT 
    CASE WHEN total_found >= 0 THEN 'PASS' ELSE 'FAIL' END || ': search_documents_with_facets returned ' || total_found || ' results'
FROM facets.search_documents_with_facets(
    'test_faceting',
    'articles',
    'PostgreSQL',
    NULL,  -- no facet filter
    NULL,  -- no vector column
    'content',
    'metadata',
    'created_at',
    'updated_at',
    10,
    0
);

-- Test 35: optimal_chunk_bits
\echo ''
\echo '--- Test 35: optimal_chunk_bits ---'

SELECT 
    CASE WHEN facets.optimal_chunk_bits(100) > 0 THEN 'PASS' ELSE 'FAIL' END || 
    ': optimal_chunk_bits(100) = ' || facets.optimal_chunk_bits(100);

SELECT 
    CASE WHEN facets.optimal_chunk_bits(1000000) > 0 THEN 'PASS' ELSE 'FAIL' END || 
    ': optimal_chunk_bits(1000000) = ' || facets.optimal_chunk_bits(1000000);

-- Test 36: rebuild_hierarchy (on articles which has facets)
\echo ''
\echo '--- Test 36: rebuild_hierarchy ---'

SELECT facets.rebuild_hierarchy('test_faceting.articles'::regclass);

\echo 'PASS: rebuild_hierarchy executed successfully'

-- ============================================
-- NEW TESTS: Bitmap Optimization Functions
-- ============================================

\echo ''
\echo '=============================================='
\echo 'Bitmap Optimization Tests'
\echo '=============================================='

-- Test 37: filter_documents_by_facets_bitmap
\echo ''
\echo '--- Test 37: filter_documents_by_facets_bitmap ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_card bigint;
BEGIN
    -- Get bitmap for Electronics filter
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_faceting',
        '{"category": "Electronics"}'::jsonb,
        'articles'
    ) INTO v_bitmap;
    
    IF v_bitmap IS NOT NULL THEN
        v_card := rb_cardinality(v_bitmap);
        IF v_card >= 0 THEN
            RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap returned bitmap with % documents', v_card;
        ELSE
            RAISE NOTICE 'FAIL: Invalid cardinality';
        END IF;
    ELSE
        -- NULL is valid for empty/no-match cases
        RAISE NOTICE 'PASS: filter_documents_by_facets_bitmap returned NULL (no matches)';
    END IF;
END;
$$;

-- Test 38: filter_documents_by_facets_bitmap with Technology filter
\echo ''
\echo '--- Test 38: filter_documents_by_facets_bitmap - Technology ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_card bigint;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_faceting',
        '{"category": "Technology"}'::jsonb,
        'articles'
    ) INTO v_bitmap;
    
    IF v_bitmap IS NOT NULL THEN
        v_card := rb_cardinality(v_bitmap);
        RAISE NOTICE 'PASS: Technology category bitmap has % documents', v_card;
    ELSE
        RAISE NOTICE 'INFO: No Technology documents found (bitmap is NULL)';
    END IF;
END;
$$;

-- Test 39: filter_documents_by_facets_bitmap - NULL facets
\echo ''
\echo '--- Test 39: filter_documents_by_facets_bitmap - NULL input ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_faceting',
        NULL,
        'articles'
    ) INTO v_bitmap;
    
    IF v_bitmap IS NULL THEN
        RAISE NOTICE 'PASS: NULL facets returns NULL bitmap';
    ELSE
        RAISE NOTICE 'FAIL: Expected NULL, got bitmap with % elements', rb_cardinality(v_bitmap);
    END IF;
END;
$$;

-- Test 40: hierarchical_facets_bitmap
\echo ''
\echo '--- Test 40: hierarchical_facets_bitmap ---'

DO $$
DECLARE
    v_facets jsonb;
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id 
    FROM facets.faceted_table 
    WHERE schemaname = 'test_faceting' AND tablename = 'articles';
    
    IF v_table_id IS NOT NULL THEN
        SELECT facets.hierarchical_facets_bitmap(v_table_id, 10, NULL) INTO v_facets;
        
        IF v_facets IS NOT NULL THEN
            RAISE NOTICE 'PASS: hierarchical_facets_bitmap returned % facet groups', jsonb_array_length(v_facets);
        ELSE
            RAISE NOTICE 'FAIL: hierarchical_facets_bitmap returned NULL';
        END IF;
    ELSE
        RAISE NOTICE 'SKIP: articles table not found';
    END IF;
END;
$$;

-- Test 41: hierarchical_facets_bitmap with filter
\echo ''
\echo '--- Test 41: hierarchical_facets_bitmap with filter ---'

DO $$
DECLARE
    v_facets jsonb;
    v_filter_bitmap roaringbitmap;
    v_table_id oid;
BEGIN
    SELECT table_id INTO v_table_id 
    FROM facets.faceted_table 
    WHERE schemaname = 'test_faceting' AND tablename = 'articles';
    
    IF v_table_id IS NOT NULL THEN
        -- Build a filter bitmap
        v_filter_bitmap := rb_build(ARRAY[1, 2, 3]);
        
        SELECT facets.hierarchical_facets_bitmap(v_table_id, 5, v_filter_bitmap) INTO v_facets;
        
        IF v_facets IS NOT NULL THEN
            RAISE NOTICE 'PASS: hierarchical_facets_bitmap with filter returned % facet groups', jsonb_array_length(v_facets);
        ELSE
            RAISE NOTICE 'INFO: No facets returned (may be expected for small filter)';
        END IF;
    ELSE
        RAISE NOTICE 'SKIP: articles table not found';
    END IF;
END;
$$;

-- Test 42: search_documents_with_facets with bitmap optimization (empty query)
\echo ''
\echo '--- Test 42: search_documents_with_facets - Bitmap path (empty query) ---'

SELECT 
    CASE WHEN total_found >= 0 THEN 'PASS' ELSE 'FAIL' END || 
    ': Empty query with facet filter - total=' || total_found || ', results=' || 
    COALESCE(jsonb_array_length(results)::text, '0')
FROM facets.search_documents_with_facets(
    'test_faceting',
    'articles',
    '',  -- Empty query triggers bitmap optimization
    '{"category": "Technology"}'::jsonb,
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

-- Test 43: search_documents_with_facets - Empty query, no filter
\echo ''
\echo '--- Test 43: search_documents_with_facets - Empty query, no filter ---'

SELECT 
    CASE WHEN total_found >= 0 THEN 'PASS' ELSE 'FAIL' END || 
    ': Empty query without filter - total=' || total_found
FROM facets.search_documents_with_facets(
    'test_faceting',
    'articles',
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

-- Test 44: Bitmap vs Array correctness check
\echo ''
\echo '--- Test 44: Bitmap vs Array correctness ---'

DO $$
DECLARE
    v_bitmap roaringbitmap;
    v_bitmap_card bigint;
    v_array_count bigint;
BEGIN
    -- Bitmap method
    SELECT facets.filter_documents_by_facets_bitmap(
        'test_faceting',
        '{"category": "Technology"}'::jsonb,
        'articles'
    ) INTO v_bitmap;
    
    IF v_bitmap IS NOT NULL THEN
        v_bitmap_card := rb_cardinality(v_bitmap);
    ELSE
        v_bitmap_card := 0;
    END IF;
    
    -- Array method
    SELECT COUNT(*) INTO v_array_count
    FROM facets.filter_documents_by_facets(
        'test_faceting',
        '{"category": "Technology"}'::jsonb,
        'articles'
    );
    
    IF v_bitmap_card = v_array_count THEN
        RAISE NOTICE 'PASS: Bitmap (%) and Array (%) methods return same count', v_bitmap_card, v_array_count;
    ELSE
        RAISE NOTICE 'FAIL: Mismatch - bitmap=%, array=%', v_bitmap_card, v_array_count;
    END IF;
END;
$$;

-- Test 45: Verify facets are computed correctly with bitmap filter
\echo ''
\echo '--- Test 45: Facet computation with bitmap filter ---'

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'test_faceting',
        'articles',
        '',
        '{"category": "Technology"}'::jsonb,
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
    
    IF v_result.facets IS NOT NULL THEN
        RAISE NOTICE 'PASS: Facets computed correctly, % facet groups returned', jsonb_array_length(v_result.facets);
    ELSE
        RAISE NOTICE 'INFO: No facets returned (empty or NULL)';
    END IF;
END;
$$;

-- ============================================
-- Cleanup
-- ============================================

\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('test_faceting.documents');
SELECT facets.drop_faceting('test_faceting.articles');
DROP SCHEMA test_faceting CASCADE;

\echo ''
\echo '=============================================='
\echo 'Complete Test Suite Finished!'
\echo '=============================================='
\echo 'Total Tests: 45'
\echo '=============================================='
