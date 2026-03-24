# pg_facets — Examples

Examples for the `pg_facets` PostgreSQL extension.

## Contents

| Path | Description |
|------|-------------|
| [`04_faceting_setup.sql`](04_faceting_setup.sql) | Faceting setup for IMDB titles and names |
| [`golang/`](golang/) | Go client library and integration tests |
| [`rust/`](rust/) | Rust client library and integration tests |
| [`python/`](python/) | Python client library and integration tests |

## Quick Start

### 1. Load the extension

```sql
CREATE EXTENSION IF NOT EXISTS roaringbitmap;
CREATE EXTENSION IF NOT EXISTS pg_facets;
```

### 2. Run examples / tests

```bash
# Go
cd examples/golang && make test

# Rust
cd examples/rust && make test

# Python
cd examples/python && make test
```

See [`golang/README.md`](golang/README.md), [`rust/README.md`](rust/README.md), and [`python/README.md`](python/README.md) for API references.
