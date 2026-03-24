//! Integration tests for the pg_facets Rust client.
//!
//! Run with: ./run_tests.sh or make test

use pgfacets::*;
use std::collections::HashMap;
use std::env;
use tokio_postgres::NoTls;

const DEFAULT_DSN: &str = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable";

async fn setup_test_schema(client: &tokio_postgres::Client) {
    let queries = [
        "DROP SCHEMA IF EXISTS test_zig CASCADE",
        "CREATE SCHEMA test_zig",
        r#"CREATE TABLE test_zig.documents (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            category TEXT,
            subcategory TEXT,
            price NUMERIC,
            tags TEXT[],
            in_stock BOOLEAN DEFAULT true,
            status TEXT DEFAULT 'active',
            metadata JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )"#,
        r#"INSERT INTO test_zig.documents (id, title, content, category, subcategory, price, tags, in_stock) VALUES
            (1, 'Laptop Pro', 'High-end laptop for professionals', 'Electronics', 'Computers', 1299.99, ARRAY['premium', 'laptop', 'work'], true),
            (2, 'Budget Phone', 'Affordable smartphone', 'Electronics', 'Phones', 199.99, ARRAY['budget', 'phone'], true),
            (3, 'Wireless Headphones', 'Noise-cancelling headphones', 'Electronics', 'Audio', 299.99, ARRAY['premium', 'audio', 'wireless'], true),
            (4, 'Mystery Novel', 'Thrilling mystery story', 'Books', 'Fiction', 14.99, ARRAY['fiction', 'mystery', 'bestseller'], true),
            (5, 'Cooking Guide', 'Italian cuisine recipes', 'Books', 'Non-Fiction', 24.99, ARRAY['cooking', 'italian'], false)"#,
        r#"SELECT facets.add_faceting_to_table(
            'test_zig.documents',
            'id',
            ARRAY[
                facets.plain_facet('category'),
                facets.plain_facet('subcategory'),
                facets.array_facet('tags'),
                facets.boolean_facet('in_stock')
            ]
        )"#,
        "SELECT facets.populate_facets('test_zig.documents'::regclass)",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 1, 'High-end laptop for professionals', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 2, 'Affordable smartphone', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 3, 'Noise-cancelling headphones', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 4, 'Thrilling mystery story', 'content', 'english')",
        "SELECT facets.bm25_index_document('test_zig.documents'::regclass, 5, 'Italian cuisine recipes', 'content', 'english')",
        "SELECT facets.bm25_recalculate_statistics('test_zig.documents'::regclass)",
    ];
    for q in queries {
        let _ = client.execute(q, &[]).await;
    }
}

async fn cleanup_test_schema(client: &tokio_postgres::Client) {
    let _ = client.execute("DROP SCHEMA IF EXISTS test_zig CASCADE", &[]).await;
}

#[tokio::test]
async fn test_faceting_search() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PGFACETS_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap", &[]).await.ok();
    client.execute("CREATE EXTENSION IF NOT EXISTS pg_facets", &[]).await.ok();

    setup_test_schema(&client).await;

    let search = FacetingZigSearch::new(
        client.clone(),
        Config {
            schema_name: "test_zig".to_string(),
            document_table: "documents".to_string(),
        },
    );

    // GetTopFacetValues
    let results = search.get_top_facet_values(None, 10).await.expect("get_top_facet_values");
    // May be empty or have facets

    // FilterDocumentsByFacets
    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "Electronics".to_string());
    let ids = search.filter_documents_by_facets(&filters).await.expect("filter_documents_by_facets");
    assert_eq!(ids.len(), 3, "expected 3 Electronics documents");

    // MergeDeltas
    search.merge_deltas().await.expect("merge_deltas");

    // SearchWithFacets
    let req = SearchWithFacetsRequest {
        query: "laptop".to_string(),
        facets: None,
        content_column: Some("content".to_string()),
        limit: Some(10),
        offset: Some(0),
        min_score: Some(0.0),
        facet_limit: Some(5),
    };
    let resp = search.search_with_facets(&req).await.expect("search_with_facets");
    assert!(resp.results.len() >= 1, "expected at least 1 result for 'laptop'");

    cleanup_test_schema(&client).await;
}

#[tokio::test]
async fn test_bitmap_optimization() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PGFACETS_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap", &[]).await.ok();
    client.execute("CREATE EXTENSION IF NOT EXISTS pg_facets", &[]).await.ok();

    setup_test_schema(&client).await;

    let search = FacetingZigSearch::new(
        client.clone(),
        Config {
            schema_name: "test_zig".to_string(),
            document_table: "documents".to_string(),
        },
    );

    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "Electronics".to_string());

    let bitmap = search
        .filter_documents_by_facets_bitmap(&filters)
        .await
        .expect("filter_documents_by_facets_bitmap");
    if let Some(ref b) = bitmap {
        let card = search.get_bitmap_cardinality(Some(b)).await.expect("get_bitmap_cardinality");
        assert_eq!(card, 3, "expected 3 Electronics documents in bitmap");
    }

    let facets = search
        .hierarchical_facets_bitmap(bitmap.as_deref(), 10)
        .await
        .expect("hierarchical_facets_bitmap");
    // Should return facet groups

    cleanup_test_schema(&client).await;
}

#[tokio::test]
async fn test_bm25_functions() {
    let dsn = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());
    let (client, connection) = match tokio_postgres::connect(&dsn, NoTls).await {
        Ok(c) => c,
        Err(e) => {
            if env::var("PGFACETS_TEST_FAIL_ON_NO_DB").unwrap_or_default() == "true" {
                panic!("Failed to connect: {}", e);
            }
            eprintln!("Skipping: no database ({})", e);
            return;
        }
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap", &[]).await.ok();
    client.execute("CREATE EXTENSION IF NOT EXISTS pg_facets", &[]).await.ok();

    setup_test_schema(&client).await;

    let search = FacetingZigSearch::new(
        client.clone(),
        Config {
            schema_name: "test_zig".to_string(),
            document_table: "documents".to_string(),
        },
    );

    search
        .index_document(100, "Test document about machine learning", "english")
        .await
        .expect("index_document");

    search.recalculate_statistics().await.expect("recalculate_statistics");

    let results = search.bm25_search("laptop", "english", 10).await.expect("bm25_search");
    assert!(!results.is_empty(), "expected at least 1 result for 'laptop'");

    search.delete_document(100).await.expect("delete_document");

    cleanup_test_schema(&client).await;
}
