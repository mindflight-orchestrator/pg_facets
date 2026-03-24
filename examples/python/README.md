# pg_facets — Python Client

Python client library for the `pg_facets` PostgreSQL extension (version 0.4.x compatible).

## Overview

Provides a thin wrapper around the facets.* SQL functions for faceted search, BM25, and bitmap operations.

## Installation

```bash
pip install psycopg[binary]
```

## Quick Start

```python
import psycopg
from pgfacets import Config, FacetingZigSearch, SearchWithFacetsRequest

conn = psycopg.connect("postgres://user:pass@localhost:5432/db")
search = FacetingZigSearch(conn, Config(schema_name="my_schema", document_table="documents"))

req = SearchWithFacetsRequest(
    query="laptop",
    facets={"category": "Electronics"},
    limit=10,
)
resp = search.search_with_facets(req)
print(f"Found {resp.total_found} results")

conn.close()
```

## API Reference

### Search
- `search_with_facets` — Full-text + facet search
- `filter_documents_by_facets` — Get document IDs matching facet filters
- `filter_documents_by_facets_bitmap` — Get bitmap of matching docs
- `get_bitmap_cardinality` — Count documents in a bitmap

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

## Testing

```bash
cd examples/python
make test   # Builds Docker, starts Postgres, runs tests, tears down
```
