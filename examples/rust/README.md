# pg_facets — Rust Client

Rust client library for the `pg_facets` PostgreSQL extension (version 0.4.x compatible).

## Overview

Provides a thin wrapper around the facets.* SQL functions for faceted search, BM25, and bitmap operations.

## Usage

```rust
use pgfacets::*;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (client, connection) = tokio_postgres::connect(
        "postgres://user:pass@localhost:5432/db?sslmode=disable",
        NoTls,
    ).await?;
    tokio::spawn(async move { let _ = connection.await; });

    let search = FacetingZigSearch::new(client, Config {
        schema_name: "my_schema".to_string(),
        document_table: "documents".to_string(),
    });

    let req = SearchWithFacetsRequest {
        query: "laptop".to_string(),
        facets: None,
        content_column: Some("content".to_string()),
        limit: Some(10),
        offset: Some(0),
        min_score: Some(0.0),
        facet_limit: Some(5),
    };
    let resp = search.search_with_facets(&req).await?;
    println!("Found {} results", resp.total_found);

    Ok(())
}
```

## API Reference

### Search
- `search_with_facets` — Full-text + facet search
- `filter_documents_by_facets` — Get document IDs matching facet filters
- `filter_documents_by_facets_bitmap` — Get bitmap of matching docs (efficient for large sets)
- `get_bitmap_cardinality` — Count documents in a bitmap
- `hierarchical_facets_bitmap` — Get facet counts from bitmap filter

### Facets
- `get_top_facet_values` — Top N values for specified facets
- `merge_deltas` — Apply pending delta updates
- `add_facet` — Add facet definition
- `drop_facet` — Remove facet

### BM25
- `index_document` — Index document for BM25 search
- `delete_document` — Remove from BM25 index
- `bm25_search` — BM25 search
- `recalculate_statistics` — Recalculate BM25 stats

## Build

If `cargo build` fails with "unknown proxy name: 'cursor'", use the toolchain cargo directly:

```bash
$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo build
```

The `make test` and `run_tests.sh` scripts automatically use the toolchain cargo when available.

## Testing

```bash
cd examples/rust
make test   # Builds Docker, starts Postgres, runs tests, tears down
```
