\echo '=============================================='
\echo 'Minimal Facets Test Suite'
\echo '=============================================='

DROP SCHEMA IF EXISTS minimal_facets CASCADE;
CREATE SCHEMA minimal_facets;

CREATE TABLE minimal_facets.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL,
    region TEXT NOT NULL,
    tags TEXT[] NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    in_stock BOOLEAN NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO minimal_facets.documents
    (title, content, category, region, tags, price, in_stock, metadata)
VALUES
    ('PostgreSQL Performance', 'postgresql tuning indexes vacuum analyze', 'Technology', 'EU', ARRAY['postgresql', 'performance'], 29.99, true,  '{"author":"Alice"}'),
    ('PostgreSQL Administration', 'postgresql backup replication security', 'Technology', 'US', ARRAY['postgresql', 'admin'], 24.99, true,  '{"author":"Bob"}'),
    ('Bread Baking', 'bread baking starter flour oven', 'Cooking', 'EU', ARRAY['baking', 'bread'], 12.50, true,  '{"author":"Carla"}'),
    ('Trail Running', 'trail running fitness endurance outdoors', 'Sports', 'EU', ARRAY['running', 'fitness'], 18.00, false, '{"author":"Dan"}'),
    ('SQL Joins', 'sql joins cte planner database', 'Technology', 'US', ARRAY['sql', 'database'], 34.00, true, '{"author":"Eve"}');

SELECT facets.add_faceting_to_table(
    'minimal_facets.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.plain_facet('region'),
        facets.array_facet('tags'),
        facets.boolean_facet('in_stock'),
        facets.bucket_facet('price', buckets => ARRAY[0, 20, 30, 40])
    ],
    populate => true
);

DO $$
DECLARE
    v_bitmap roaringbitmap;
BEGIN
    SELECT facets.filter_documents_by_facets_bitmap(
        'minimal_facets',
        '{"category":"Technology"}'::jsonb,
        'documents'
    ) INTO v_bitmap;

    IF rb_cardinality(v_bitmap) != 3 THEN
        RAISE EXCEPTION 'FAIL: Expected 3 Technology documents, got %', rb_cardinality(v_bitmap);
    END IF;
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_facets',
        'documents',
        '',
        '{"tags":"postgresql"}'::jsonb,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        NULL,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 documents for tag postgresql, got %', v_result.total_found;
    END IF;
END;
$$;

\echo 'Minimal facets tests passed'
