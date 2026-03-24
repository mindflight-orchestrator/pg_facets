"""Integration tests for the pg_facets Python client."""

import os

import pytest
import psycopg

from pgfacets import Config, FacetingZigSearch, SearchWithFacetsRequest

DEFAULT_DSN = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"


def get_conn():
    dsn = os.environ.get("TEST_DATABASE_URL", DEFAULT_DSN)
    return psycopg.connect(dsn)


def setup_test_schema(conn):
    queries = [
        "DROP SCHEMA IF EXISTS test_zig CASCADE",
        "CREATE SCHEMA test_zig",
        """CREATE TABLE test_zig.documents (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            category TEXT,
            subcategory TEXT,
            price NUMERIC,
            tags TEXT[],
            in_stock BOOLEAN DEFAULT true,
            metadata JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )""",
        """INSERT INTO test_zig.documents (id, title, content, category, subcategory, price, tags, in_stock) VALUES
            (1, 'Laptop Pro', 'High-end laptop for professionals', 'Electronics', 'Computers', 1299.99, ARRAY['premium', 'laptop', 'work'], true),
            (2, 'Budget Phone', 'Affordable smartphone', 'Electronics', 'Phones', 199.99, ARRAY['budget', 'phone'], true),
            (3, 'Wireless Headphones', 'Noise-cancelling headphones', 'Electronics', 'Audio', 299.99, ARRAY['premium', 'audio', 'wireless'], true),
            (4, 'Mystery Novel', 'Thrilling mystery story', 'Books', 'Fiction', 14.99, ARRAY['fiction', 'mystery', 'bestseller'], true),
            (5, 'Cooking Guide', 'Italian cuisine recipes', 'Books', 'Non-Fiction', 24.99, ARRAY['cooking', 'italian'], false)""",
        """SELECT facets.add_faceting_to_table(
            'test_zig.documents', 'id',
            ARRAY[
                facets.plain_facet('category'),
                facets.plain_facet('subcategory'),
                facets.array_facet('tags'),
                facets.boolean_facet('in_stock')
            ]
        )""",
        "SELECT facets.populate_facets('test_zig.documents'::regclass)",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 1, 'High-end laptop for professionals', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 2, 'Affordable smartphone', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 3, 'Noise-cancelling headphones', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 4, 'Thrilling mystery story', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 5, 'Italian cuisine recipes', 'content', 'english')",
        "SELECT facets.bm25_recalculate_statistics('test_zig.documents'::regclass)",
    ]
    with conn.cursor() as cur:
        for q in queries:
            try:
                cur.execute(q)
            except Exception:
                pass
    conn.commit()


def cleanup_test_schema(conn):
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS test_zig CASCADE")
    conn.commit()


@pytest.fixture
def client():
    try:
        conn = get_conn()
    except Exception as e:
        if os.environ.get("PGFACETS_TEST_FAIL_ON_NO_DB") == "true":
            pytest.fail(f"Failed to connect: {e}")
        pytest.skip(f"No database: {e}")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_facets")
    conn.commit()
    setup_test_schema(conn)
    try:
        yield FacetingZigSearch(conn, Config(schema_name="test_zig", document_table="documents"))
    finally:
        cleanup_test_schema(conn)
        conn.close()


def test_search_with_facets(client):
    req = SearchWithFacetsRequest(
        query="laptop",
        facets=None,
        content_column="content",
        limit=10,
        min_score=0.0,
        facet_limit=5,
    )
    resp = client.search_with_facets(req)
    assert resp.total_found >= 1
    assert len(resp.results) >= 1


def test_filter_documents_by_facets(client):
    ids = client.filter_documents_by_facets({"category": "Electronics"})
    assert len(ids) == 3


def test_filter_documents_by_facets_bitmap(client):
    bitmap = client.filter_documents_by_facets_bitmap({"category": "Electronics"})
    assert bitmap is not None
    card = client.get_bitmap_cardinality(bitmap)
    assert card == 3


def test_get_top_facet_values(client):
    results = client.get_top_facet_values(None, 10)
    # May have facets


def test_merge_deltas(client):
    client.merge_deltas()


def test_bm25_search(client):
    client.index_document(100, "Test document about machine learning", "english")
    client.recalculate_statistics()
    results = client.bm25_search("laptop", "english", 10)
    assert len(results) >= 1
    client.delete_document(100)
