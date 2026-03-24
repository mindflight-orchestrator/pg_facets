# pg_facets

A high-performance PostgreSQL extension for faceted search with BM25 full-text search, leveraging [Roaring Bitmaps](https://roaringbitmap.org/) and implemented in [Zig](https://ziglang.org/).

This project is based on the abandoned `pg_faceting` extension and continues its ideas with a modernized implementation.

**Version:** 0.4.0

## Overview

`pg_facets` provides efficient faceted search capabilities for PostgreSQL databases with integrated BM25 (Best Matching 25) full-text search. It maintains bitmap indexes for facet values and BM25 inverted indexes for text search, enabling fast filtering, counting, and ranking operations that are essential for e-commerce, document management, and search applications.

### Key Features

- **Native Zig Functions**: Performance-critical operations implemented in Zig for maximum speed
- **BM25 Full-Text Search**: Proper BM25 ranking algorithm (replaces PostgreSQL's `ts_rank_cd`)
- **Roaring Bitmaps**: Efficient compressed bitmap storage for posting lists and document sets
- **Multiple Facet Types**: Plain, array, bucket, date truncation, boolean, and joined facets
- **Language Support**: Uses PostgreSQL's text search configs (`english_stem`, `french_stem`, etc.)
- **Prefix & Fuzzy Matching**: Optional prefix and fuzzy prefix matching for typo tolerance
- **Delta Updates**: Incremental updates via triggers without full reindexing
- **Hybrid Search**: Combines BM25 with pg_vector for semantic search
- **API Compatibility**: Drop-in replacement for standard pgfaceting

## Requirements

- PostgreSQL 17+
- [pg_roaringbitmap](https://github.com/ChenHuajun/pg_roaringbitmap) extension
- [pg_trgm](https://www.postgresql.org/docs/current/pgtrgm.html) extension (optional, for prefix/fuzzy matching)
- [pg_vector](https://github.com/pgvector/pgvector) extension (optional, for hybrid search)
- Zig 0.15.2+ (for building from source)

## Quick Start with Docker

### 1. Clone and Initialize

```bash
git clone --recurse-submodules https://github.com/mindflight-orchestrator/mfo-postgres-ext.git
cd mfo-postgres-ext/extensions/pg_facets/docker
```

### 2. Build and Run

**Standard** (pgvector base — default):

```bash
docker compose build
docker compose up -d
```

**TimescaleDB HA base** (for TimescaleDB stacks):

```bash
docker compose -f docker-compose.ha.yml build
docker compose -f docker-compose.ha.yml up -d
```

**Vendored** (no apt, self-contained — run from repo root first):

```bash
./scripts/setup_pg17_submodule.sh   # one-time
cd extensions/pg_facets/docker
docker-compose -f docker-compose.vendored.yml build   # pgvector base
docker-compose -f docker-compose.vendored.yml up -d
# Or TimescaleDB HA vendored:
docker-compose -f docker-compose.ha.vendored.yml build
docker-compose -f docker-compose.ha.vendored.yml up -d
```

### 3. Verify

```bash
sleep 15
docker compose ps
```

### 4. Connect

```bash
# Using psql
psql -h localhost -p 5433 -U postgres -d postgres

# Or exec into the container
docker exec -it pg_facets psql -U postgres
```

### Connection Details

| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `5433` |
| User | `postgres` |
| Password | `postgres` |
| Database | `postgres` |

Connection string:
```
postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable
```

## Building from Source

### macOS (Intel and Apple Silicon)

See [README_MACOSX.md](../../README_MACOSX.md) for full instructions.

```bash
# From repo root (Postgres.app or Homebrew)
./scripts/build-macos.sh

# Vendored (no external PostgreSQL)
./scripts/setup_pg17_submodule.sh
./scripts/build-macos.sh
```

### Prerequisites

- Zig 0.15.2+
- PostgreSQL 17 dev headers (Postgres.app, Homebrew, or vendored `ext_pg17_src`)
- pg_roaringbitmap (submodule in `deps/`)

### Build Steps

```bash
git clone --recurse-submodules https://github.com/mindflight-orchestrator/mfo-postgres-ext.git
cd mfo-postgres-ext

# Build pg_roaringbitmap (in both extension deps), then pg_facets
./scripts/build-macos.sh

# Or manual:
cd extensions/pg_facets/deps/pg_roaringbitmap && make && make install && cd ../../..
cd extensions/pg_facets && zig build -Doptimize=ReleaseFast

# Install
cp zig-out/lib/libpg_facets.* $(pg_config --pkglibdir)/
cp pg_facets.control sql/pg_facets--*.sql $(pg_config --sharedir)/extension/
```

## Usage

### 1. Install Extension

```sql
CREATE EXTENSION roaringbitmap;
CREATE EXTENSION pg_facets;
```

### 2. Add Faceting to a Table

```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    price DECIMAL(10,2),
    tags TEXT[],
    in_stock BOOLEAN DEFAULT true
);

SELECT facets.add_faceting_to_table(
    'products',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category'),
        facets.bucket_facet('price', buckets => ARRAY[0, 50, 100, 500]),
        facets.array_facet('tags'),
        facets.boolean_facet('in_stock')
    ],
    populate => true
);
```

### 3. Query Facets

```sql
-- Get top facet values
SELECT * FROM facets.top_values('products'::regclass, 10);

-- Filter and count
SELECT * FROM facets.count_results(
    'products'::regclass::oid,
    filters => ARRAY[ROW('category', 'Electronics')]::facets.facet_filter[]
);

-- Search documents with filters
SELECT * FROM search_documents_native(
    'products'::regclass::oid,
    ARRAY[ROW('category', 'Electronics'), ROW('in_stock', 'true')]::facets.facet_filter[],
    100,  -- limit
    0     -- offset
);
```

### 4. BM25 Full-Text Search

```sql
-- Index documents for BM25 search
SELECT facets.bm25_index_document(
    'products'::regclass,
    doc_id := 1,
    content := 'Product description text here...',
    content_column := 'description',
    language := 'english'
);

-- Search with BM25 ranking
SELECT * FROM facets.bm25_search(
    'products'::regclass,
    query := 'laptop computer',
    language := 'english',
    limit := 10
);

-- Search with prefix matching (e.g., "run" matches "running")
SELECT * FROM facets.bm25_search(
    'products'::regclass,
    query := 'laptop',
    prefix_match := true,
    limit := 10
);

-- Search with fuzzy matching (typo tolerance)
SELECT * FROM facets.bm25_search(
    'products'::regclass,
    query := 'laptoop',  -- typo
    fuzzy_match := true,
    fuzzy_threshold := 0.3,
    limit := 10
);

-- Use BM25 in existing search_documents function (now uses BM25 instead of ts_rank_cd)
SELECT * FROM facets.search_documents(
    p_schema_name := 'public',
    p_table_name := 'products',
    p_query := 'laptop computer',
    p_content_column := 'description',
    p_language := 'english',
    p_limit := 10
);
```

### 5. Maintain Facets

```sql
-- After bulk inserts/updates, merge deltas
SELECT merge_deltas_native('products'::regclass);

-- Or use the wrapper function
SELECT facets.merge_deltas('products'::regclass);
```

## Native Zig Functions

The following functions are implemented in Zig for maximum performance:

### Faceting Functions

| Function | Description |
|----------|-------------|
| `merge_deltas_native(regclass)` | Apply pending delta updates to facet bitmaps |
| `build_filter_bitmap_native(oid, facet_filter[])` | Build a combined bitmap from filters |
| `get_facet_counts_native(oid, roaringbitmap, text[], int)` | Get facet value counts with optional filtering |
| `search_documents_native(oid, facet_filter[], int, int)` | Search documents matching filters with pagination |

### BM25 Functions

| Function | Description |
|----------|-------------|
| `bm25_index_document_native(oid, bigint, text, text)` | Index a document for BM25 search |
| `bm25_delete_document_native(oid, bigint)` | Remove a document from BM25 index |
| `bm25_search_native(oid, text, text, bool, bool, float, float, float, int)` | Search documents with BM25 ranking |
| `bm25_score_native(oid, text, bigint, text, float, float)` | Calculate BM25 score for a document |

## Running Tests

### Using Docker

```bash
# Copy and run the test file
docker cp test/sql/complete_test.sql pg_facets:/tmp/
docker exec pg_facets psql -U postgres -f /tmp/complete_test.sql
```

### Quick Verification

```bash
docker exec pg_facets psql -U postgres -c "
    SELECT facets._get_version();
"
```

## Project Structure

```
pg_facets/
├── src/                    # Zig source code
│   ├── main.zig           # Extension entry point
│   ├── deltas.zig         # Delta merging logic
│   ├── facets.zig         # Facet counting
│   ├── filters.zig        # Filter bitmap construction
│   ├── search.zig         # Document search
│   ├── utils.zig          # C-interop utilities
│   ├── test_utils.zig     # Unit tests
│   ├── filter_helper.c    # C helpers for PostgreSQL macros
│   └── bm25/              # BM25 search module
│       ├── tokenizer.zig  # Text tokenization using PostgreSQL configs
│       ├── index.zig      # Document indexing
│       ├── scoring.zig    # BM25 scoring algorithm
│       ├── search.zig     # BM25 search with prefix/fuzzy
│       └── roaring_index.zig  # Roaring bitmap operations
├── sql/
│   ├── pg_facets--0.3.6.sql  # Base SQL definitions
│   └── pg_facets--0.3.10--0.4.0.sql  # BM25 migration
├── test/
│   └── sql/               # Test SQL files
├── deps/
│   └── pg_roaringbitmap/  # Git submodule
├── docker/
│   ├── Dockerfile             # pgvector base (default)
│   ├── Dockerfile.ha          # TimescaleDB HA base
│   ├── Dockerfile.vendored    # pgvector base, vendored build
│   ├── Dockerfile.ha.vendored # TimescaleDB HA, vendored build
│   ├── docker-compose.yml
│   ├── docker-compose.ha.yml
│   ├── docker-compose.vendored.yml
│   ├── docker-compose.ha.vendored.yml
│   ├── init/              # Initialization scripts
│   └── config/            # PostgreSQL configuration
├── build.zig              # Zig build script
├── pg_facets.control    # Extension control file
├── USAGE.md               # Detailed usage guide
└── DOCUMENTATION.md       # Code documentation
```

## Facet Types

| Type | Description | Example |
|------|-------------|---------|
| `plain_facet` | Direct column value | `facets.plain_facet('category')` |
| `array_facet` | Array column values | `facets.array_facet('tags')` |
| `bucket_facet` | Numeric ranges | `facets.bucket_facet('price', ARRAY[0,100,500])` |
| `datetrunc_facet` | Truncated dates | `facets.datetrunc_facet('created_at', 'month')` |
| `boolean_facet` | Boolean values | `facets.boolean_facet('in_stock')` |
| `joined_plain_facet` | Values from joined tables | See documentation |

## Troubleshooting

### Extension not found

```bash
docker exec pg_facets psql -U postgres -c "
    SELECT * FROM pg_available_extensions 
    WHERE name LIKE '%facet%' OR name LIKE '%roaring%';
"
```

### Container exits immediately

```bash
docker logs pg_facets
```

### Port already in use

Edit `docker/docker-compose.yml` and change the port mapping.

## BM25 Search Features

### Tokenization & Language Support

BM25 uses PostgreSQL's built-in text search configurations for tokenization and stemming:
- **English**: `english_stem` (default)
- **French**: `french_stem`
- **German**: `german_stem`
- **Other languages**: Any PostgreSQL text search config

### Search Matching Strategies

1. **Exact Matching** (default): Standard BM25 with exact term matching
2. **Prefix Matching**: Query term "run" matches indexed terms "running", "runner" (requires `pg_trgm`)
3. **Fuzzy Prefix Matching**: Typo tolerance - "runing" matches "running" (requires `pg_trgm`)

### Performance Characteristics

- **Index Creation**: ~3-5 minutes for 1M documents (~1000 tokens each)
- **Index Size**: ~700MB-2.8GB for 1M documents
- **Query Speed**: ~50-200ms for typical searches (1M docs, 3 terms)
- **Prefix Matching**: +10-50ms overhead per query
- **Fuzzy Matching**: +50-200ms overhead per query

### Migration from ts_rank_cd

The `search_documents()` and `search_documents_with_facets()` functions now use BM25 instead of `ts_rank_cd`. The API remains the same, but ranking quality is improved.

**Before (using ts_rank_cd):**
```sql
ts_rank_cd(to_tsvector('english', content), plainto_tsquery('english', query))
```

**After (using BM25):**
```sql
facets.bm25_score('table'::regclass, query, doc_id, 'english')
```

## Performance Notes

- Native Zig functions are significantly faster than SQL equivalents
- Use `merge_deltas_native` after batch operations for best performance
- The `chunk_bits` parameter (default: auto-detected) controls bitmap chunking
- For large BM25 indexes (10M+ documents), consider partitioning by `table_id`
- Set `maintenance_work_mem = 1GB+` for faster BM25 index builds

## License

MIT

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `zig build test`
4. Submit a pull request

## Related Projects

- [pg_roaringbitmap](https://github.com/ChenHuajun/pg_roaringbitmap) - Roaring bitmap extension
- [pgfaceting](https://github.com/cybertec-postgresql/pgfaceting) - Original PostgreSQL faceting extension

