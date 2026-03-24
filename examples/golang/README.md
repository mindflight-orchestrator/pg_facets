# pgfaceting_zig Go Client

Go client library for interacting with the `pgfaceting_zig` PostgreSQL extension.

## Overview

This package provides two interfaces for working with `pgfaceting_zig`:

1. **`FacetingZigSearch`** - High-level interface for search and faceting operations
2. **`NativeFaceting`** - Low-level interface for direct access to Zig native functions

## Installation

```bash
go get github.com/jackc/pgx/v5
```

## Quick Start

### High-Level Interface

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()
    
    // Connect to PostgreSQL
    pool, _ := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/db")
    defer pool.Close()
    
    // Create search instance
    config := RagConfig{
        SchemaName:    "my_schema",
        DocumentTable: "documents",
    }
    search, _ := NewFacetingZigSearch(pool, config, nil, false)
    
    // Search with facets
    resp, _ := search.SearchWithFacets(ctx, SearchWithFacetsRequest{
        Query:  "laptop computer",
        Facets: map[string]string{"category": "Electronics"},
        Limit:  10,
    })
    
    fmt.Printf("Found %d results\n", resp.TotalFound)
    for _, r := range resp.Results {
        fmt.Printf("  - %d: %s (score: %.2f)\n", r.ID, r.Content[:50], r.CombinedScore)
    }
}
```

### Low-Level Native Interface

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
    ctx := context.Background()
    
    pool, _ := pgxpool.New(ctx, "postgres://user:pass@localhost:5432/db")
    defer pool.Close()
    
    // Create native faceting instance
    native := NewNativeFaceting(pool, "my_schema", "documents", false)
    
    // Build a filter bitmap
    bitmap, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
        {FacetName: "category", FacetValue: "Electronics"},
        {FacetName: "in_stock", FacetValue: "true"},
    })
    
    // Get cardinality
    count, _ := native.GetBitmapCardinality(ctx, bitmap)
    fmt.Printf("Matching documents: %d\n", count)
    
    // Get facet counts filtered by the bitmap
    counts, _ := native.GetFacetCounts(ctx, bitmap, nil, 10)
    for _, c := range counts {
        fmt.Printf("  %s: %s = %d\n", c.FacetName, c.FacetValue, c.Cardinality)
    }
}
```

## API Reference

### FacetingZigSearch

High-level search interface compatible with pgfaceting 0.4.2 API.

#### Constructor

```go
func NewFacetingZigSearch(
    pool *pgxpool.Pool,
    config RagConfig,
    embeddingService EmbeddingService,
    debug bool,
) (*FacetingZigSearch, error)
```

#### Methods

| Method | Description |
|--------|-------------|
| `SearchWithFacets(ctx, req)` | Full-text + vector search with facet filtering and counts |
| `SearchWithFacetsBitmap(ctx, req)` | **NEW** Bitmap-optimized search (better for large result sets) |
| `FilterDocumentsByFacets(ctx, facets)` | Get document IDs matching facet filters |
| `FilterDocumentsByFacetsBitmap(ctx, facets)` | **NEW** Get bitmap of matching docs (efficient for millions) |
| `GetBitmapCardinality(ctx, bitmap)` | **NEW** Get count of documents in a bitmap |
| `HierarchicalFacetsBitmap(ctx, bitmap, limit)` | **NEW** Get facets using bitmap filter directly |
| `GetTopFacetValues(ctx, facetNames, limit)` | Get top N values for specified facets |
| `MergeDeltas(ctx)` | Apply pending delta updates |
| `AddFacet(ctx, facetDef)` | Add a new facet definition |
| `DropFacet(ctx, facetName)` | Remove a facet |
| `CountResultsWithFilters(ctx, filters)` | Get facet counts with active filters |
| `RunMaintenance(ctx)` | Run global faceting maintenance |
| `BM25CreateSyncTrigger(ctx, idCol, contentCol, lang)` | **0.4.2** Create BM25 sync trigger |
| `BM25DropSyncTrigger(ctx)` | **0.4.2** Drop BM25 sync trigger |
| `BM25RebuildIndex(ctx, options)` | **0.4.2** Rebuild BM25 index |
| `BM25Status(ctx)` | **0.4.2** Get status of all BM25 indexes |
| `BM25Progress(ctx)` | **0.4.2** Get indexing progress |
| `BM25ActiveProcesses(ctx)` | **0.4.2** List active BM25 processes |
| `BM25CleanupDblinks(ctx)` | **0.4.2** Clean up dblink connections |
| `BM25CleanupStaging(ctx)` | **0.4.2** Clean up staging tables |
| `BM25KillStuck(ctx, minDuration)` | **0.4.2** Kill stuck processes |
| `BM25FullCleanup(ctx, killThreshold)` | **0.4.2** Full cleanup |
| `SetupTableWithBM25(ctx, options)` | **0.4.2** One-stop table setup |

### NativeFaceting

Low-level interface for direct access to Zig native functions.

#### Constructor

```go
func NewNativeFaceting(
    pool *pgxpool.Pool,
    schemaName string,
    tableName string,
    debug bool,
) *NativeFaceting
```

#### Methods

| Method | Description |
|--------|-------------|
| `GetTableOID(ctx)` | Get the OID of the table |
| `BuildFilterBitmap(ctx, filters)` | Build a roaring bitmap from facet filters |
| `GetFacetCounts(ctx, bitmap, facetNames, topN)` | Get facet counts with optional bitmap filter |
| `SearchDocuments(ctx, filters, limit, offset)` | Get document IDs matching filters |
| `MergeDeltasNative(ctx)` | Merge deltas using Zig native function |
| `GetBitmapCardinality(ctx, bitmap)` | Get the number of elements in a bitmap |
| `AndBitmaps(ctx, b1, b2)` | AND operation on two bitmaps |
| `OrBitmaps(ctx, b1, b2)` | OR operation on two bitmaps |
| `AndNotBitmaps(ctx, b1, b2)` | **NEW** ANDNOT operation (b1 AND NOT b2) |
| `XorBitmaps(ctx, b1, b2)` | **NEW** XOR operation on two bitmaps |
| `BitmapToArray(ctx, bitmap, limit)` | Convert bitmap to array of IDs |
| `FilterDocumentsByFacetsBitmap(ctx, facets)` | **NEW** JSONB facet filter returning bitmap |
| `HierarchicalFacetsBitmap(ctx, bitmap, limit)` | **NEW** Hierarchical facets with bitmap filter |
| `BitmapContains(ctx, bitmap, docID)` | **NEW** Check if bitmap contains document ID |
| `BuildBitmapFromIDs(ctx, ids)` | **NEW** Create bitmap from array of IDs |
| `IsBitmapEmpty(ctx, bitmap)` | **NEW** Check if bitmap is empty |

## Data Types

### SearchWithFacetsRequest

```go
type SearchWithFacetsRequest struct {
    Query         string            // Text query for BM25 search
    Facets        map[string]string // Facet filters (key-value pairs)
    VectorColumn  string            // Optional: column for vector search
    ContentColumn string            // Column for text content (default: 'content')
    Limit         int
    Offset        int
    MinScore      float64
    VectorWeight  float64
    FacetLimit    int
}
```

### FacetFilter

```go
type FacetFilter struct {
    FacetName  string
    FacetValue string
}
```

### FacetDefinition

```go
type FacetDefinition struct {
    Column     string
    Type       string    // plain, array, bucket, datetrunc, boolean
    CustomName string
    Buckets    []float64 // For bucket facets
    Precision  string    // For datetrunc facets (day, month, year)
}
```

## Examples

### Adding Different Facet Types

```go
// Plain facet
search.AddFacet(ctx, FacetDefinition{
    Column: "category",
    Type:   "plain",
})

// Array facet (for array columns like tags)
search.AddFacet(ctx, FacetDefinition{
    Column: "tags",
    Type:   "array",
})

// Bucket facet (for numeric ranges)
search.AddFacet(ctx, FacetDefinition{
    Column:  "price",
    Type:    "bucket",
    Buckets: []float64{0, 10, 50, 100, 500, 1000},
})

// Boolean facet
search.AddFacet(ctx, FacetDefinition{
    Column: "in_stock",
    Type:   "boolean",
})

// Date truncation facet
search.AddFacet(ctx, FacetDefinition{
    Column:    "created_at",
    Type:      "datetrunc",
    Precision: "month",
})
```

### Combining Multiple Filters

```go
// Using high-level interface
resp, _ := search.SearchWithFacets(ctx, SearchWithFacetsRequest{
    Query: "wireless headphones",
    Facets: map[string]string{
        "category": "Electronics",
        "in_stock": "true",
    },
    Limit: 20,
})

// Using low-level interface for complex operations
native := NewNativeFaceting(pool, "schema", "table", false)

// Build individual bitmaps
electronicsMap, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
    {FacetName: "category", FacetValue: "Electronics"},
})

inStockMap, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
    {FacetName: "in_stock", FacetValue: "true"},
})

// Combine with AND
combined, _ := native.AndBitmaps(ctx, electronicsMap, inStockMap)

// Get document IDs
ids, _ := native.BitmapToArray(ctx, combined, 1000)
```

### Handling Delta Updates

```go
// After inserting/updating/deleting documents, merge deltas
err := search.MergeDeltas(ctx)
if err != nil {
    log.Printf("Failed to merge deltas: %v", err)
}

// Or use the native function directly
native := NewNativeFaceting(pool, "schema", "table", false)
err = native.MergeDeltasNative(ctx)
```

## Testing

**Tests MUST run against a real PostgreSQL instance with pg_facets extension.**

### Recommended: Use the Test Runner

```bash
cd examples/golang

# Run full test suite (builds Docker, starts container, runs tests, cleans up)
make test

# Or use the shell script directly
./run_tests.sh
```

### Alternative: Manual Setup

```bash
# Start PostgreSQL with pg_facets
make start-db

# Run tests (will FAIL if database not available)
make test-fast

# View logs if something fails
make logs

# Clean up when done
make clean
```

### Test Modes

| Environment Variable | Effect |
|---------------------|--------|
| `TEST_DATABASE_URL` | Override database connection string |
| `PGFACETS_TEST_FAIL_ON_NO_DB=true` | FAIL instead of skip when no database (CI mode) |

### CI/CD Integration

```yaml
# .github/workflows/test.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      
      - name: Run tests
        working-directory: extensions/pg_facets/examples/golang
        run: make test
```

### What Tests Verify

The test suite validates:
- ✅ All SQL functions exist and are callable
- ✅ Bitmap optimization functions work correctly
- ✅ Bitmap vs Array results match (correctness check)
- ✅ Function signatures match Go client expectations
- ✅ Extension can be created and used

## Bitmap Optimization (NEW in 0.3.6)

For large datasets with millions of documents, use the bitmap-optimized functions to avoid memory issues:

### The Problem with Array-Based Filtering

When a facet filter matches millions of documents:
- Array-based methods create huge arrays (8M docs = ~32MB)
- `ANY(array)` queries are slow with large arrays
- Memory pressure can cause OOM or crashes

### The Solution: Bitmap-Based Functions

```go
// BEFORE (problematic for large result sets):
ids, _ := search.FilterDocumentsByFacets(ctx, filters)  // Returns []int64
// If filter matches 8M docs → 8M element array!

// AFTER (efficient for any size):
bitmap, _ := search.FilterDocumentsByFacetsBitmap(ctx, filters)  // Returns []byte
// 8M docs → ~few MB compressed bitmap

// Get count without expanding to array
count, _ := search.GetBitmapCardinality(ctx, bitmap)

// Get facets directly from bitmap
facets, _ := search.HierarchicalFacetsBitmap(ctx, bitmap, 10)
```

### Performance Comparison

| Scenario | Array Method | Bitmap Method |
|----------|--------------|---------------|
| 100K matching docs | ~800KB, fast | ~100KB, fast |
| 1M matching docs | ~8MB, slow | ~500KB, fast |
| 8M matching docs | ~64MB, may crash | ~4MB, fast |

### When to Use Bitmap Methods

- **Always use** `FilterDocumentsByFacetsBitmap` for production
- **Use** `SearchWithFacetsBitmap` for faceted search
- **Use** `HierarchicalFacetsBitmap` when you have a pre-built bitmap

### Example: Large Dataset Faceted Browsing

```go
// User clicks "Electronics" category (8M products)
filters := map[string]string{"category": "Electronics"}

// Get bitmap (efficient)
bitmap, _ := search.FilterDocumentsByFacetsBitmap(ctx, filters)

// Check count
count, _ := search.GetBitmapCardinality(ctx, bitmap)
fmt.Printf("Found %d products\n", count)  // "Found 8000000 products"

// Get facet counts for the filtered set
facets, _ := search.HierarchicalFacetsBitmap(ctx, bitmap, 10)

// For pagination, use the optimized search
resp, _ := search.SearchWithFacetsBitmap(ctx, SearchWithFacetsRequest{
    Query:  "",  // Empty query = browse all matching
    Facets: filters,
    Limit:  20,
    Offset: 0,
})
// Only fetches 20 actual rows, facets computed from bitmap
```

## Performance Notes

- **Use `NativeFaceting` for bulk operations** - Direct bitmap operations are faster than SQL-based filtering
- **Merge deltas regularly** - Call `MergeDeltas()` after batch inserts to maintain query performance
- **Use bitmap operations** - For complex filter combinations, build individual bitmaps and combine them using `AndBitmaps`/`OrBitmaps`
- **Use bitmap-optimized functions for large datasets** - Prevents OOM and improves performance significantly

## Requirements

- PostgreSQL 17+
- pgfaceting_zig extension
- pg_roaringbitmap extension
- Go 1.21+

## License

MIT


##
Bonne question ! Analysons les limites de `pg_roaringbitmap` et de `pgfaceting_zig` :

## Limites de pg_roaringbitmap

[1 tool called]

D'après la documentation, voici les **limites de taille** :

## Limites de pg_roaringbitmap / pgfaceting_zig

### 1. **roaringbitmap (32-bit)** - Type par défaut utilisé par pgfaceting

| Paramètre | Limite |
|-----------|--------|
| **Plage d'IDs** | `0` à `4,294,967,295` (2³² - 1) |
| **Nombre max de documents** | **~4.3 milliards** |
| **Type d'ID requis** | `uint32` (non signé) |

> *"Logically, you could think of the roaringbitmap data type as `bit(4294967296)`"*

### 2. **roaringbitmap64 (64-bit)** - Pour des besoins plus importants

| Paramètre | Limite |
|-----------|--------|
| **Plage d'IDs** | `0` à `18,446,744,073,709,551,615` (2⁶⁴ - 1) |
| **Nombre max de documents** | **~18 quintillions** |
| **Type d'ID requis** | `uint64` (non signé) |

### 3. **Limites pratiques de pgfaceting_zig**

Le code utilise `roaringbitmap` (32-bit), donc :

```
┌─────────────────────────────────────────────────────────────┐
│  Maximum documents par facette : ~4.3 milliards             │
│  Maximum valeurs de facettes : Illimité (1 bitmap/valeur)   │
│  Maximum facettes par table : Illimité                      │
└─────────────────────────────────────────────────────────────┘
```

### 4. **Chunking dans pgfaceting**

pgfaceting utilise un système de **chunks** (configuré par `chunk_bits`, défaut = 20) :

```
chunk_bits = 20  →  2²⁰ = 1,048,576 documents par chunk
chunk_bits = 24  →  2²⁴ = 16,777,216 documents par chunk
```

Cela permet de :
- Paralléliser les requêtes
- Réduire la taille des bitmaps individuels
- Améliorer les performances de mise à jour

### 5. **Résumé pratique**

| Cas d'usage | Limite | Recommandation |
|-------------|--------|----------------|
| **Petite app** (< 1M docs) | ✅ Aucun problème | `chunk_bits = 20` |
| **Moyenne app** (1-100M docs) | ✅ OK | `chunk_bits = 20-22` |
| **Grande app** (100M-1B docs) | ✅ OK | `chunk_bits = 22-24` |
| **Très grande app** (> 4B docs) | ⚠️ Utiliser `roaringbitmap64` | Modification requise |

### 6. **Si vous dépassez 4 milliards de documents**

Vous devrez modifier pgfaceting_zig pour utiliser `roaringbitmap64` au lieu de `roaringbitmap`. Les fonctions sont similaires mais avec le suffixe `64` :
- `rb_build` → `rb64_build`
- `rb_cardinality` → `rb64_cardinality`
- etc.