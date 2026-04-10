\echo '=============================================='
\echo 'Minimal BM25 Test Suite'
\echo '=============================================='

DROP SCHEMA IF EXISTS minimal_bm25 CASCADE;
CREATE SCHEMA minimal_bm25;

CREATE TABLE minimal_bm25.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    category TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO minimal_bm25.documents
    (title, content, category, metadata)
VALUES
    ('PostgreSQL Performance Guide', 'postgresql database performance tuning vacuum analyze indexes', 'Technology', '{"author":"Alice"}'),
    ('PostgreSQL Administration', 'postgresql backup replication administration database security', 'Technology', '{"author":"Bob"}'),
    ('Sourdough Bread Basics', 'bread baking starter flour oven kitchen', 'Cooking', '{"author":"Carla"}'),
    ('Trail Running Program', 'trail running fitness endurance training sports', 'Sports', '{"author":"Dan"}'),
    ('SQL Joins Explained', 'sql database joins cte query planner execution', 'Technology', '{"author":"Eve"}');

SELECT facets.add_faceting_to_table(
    'minimal_bm25.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

SELECT facets.bm25_set_language('minimal_bm25.documents'::regclass, 'english');
SELECT facets.bm25_create_sync_trigger(
    'minimal_bm25.documents'::regclass,
    'id',
    'content',
    'english'
);
SELECT facets.bm25_rebuild_index(
    'minimal_bm25.documents'::regclass,
    'id',
    'content',
    'english',
    0
);

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents(
        'minimal_bm25',
        'documents',
        'PostgreSQL',
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );

    IF v_result.total_found != 2 THEN
        RAISE EXCEPTION 'FAIL: Expected 2 PostgreSQL BM25 matches, got %', v_result.total_found;
    END IF;
END;
$$;

DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result
    FROM facets.search_documents_with_facets(
        'minimal_bm25',
        'documents',
        'vacuum analyze',
        NULL,
        NULL,
        'content',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        NULL,
        10,
        'english'
    );

    IF v_result.total_found != 1 THEN
        RAISE EXCEPTION 'FAIL: Expected 1 result for vacuum analyze, got %', v_result.total_found;
    END IF;
END;
$$;

\echo 'Minimal BM25 tests passed'
