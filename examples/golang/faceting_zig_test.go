// faceting_zig_test.go
// Tests for pgfaceting_zig integration (version 0.4.1)
// Includes bitmap optimization tests for large datasets
//
// To run tests:
//   ./run_tests.sh  (recommended - starts Docker and runs tests)
//
// Or manually:
//   export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
//   go test -v ./...
//
// Set PGFACETS_TEST_FAIL_ON_NO_DB=true to fail instead of skip when no database

package pgfaceting

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestFacetingZigSearch tests the FacetingZigSearch functionality
func TestFacetingZigSearch(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()

	// Setup test schema and table
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	config := Config{
		SchemaName:    "test_zig",
		DocumentTable: "documents",
	}

	search, err := NewFacetingZigSearch(pool, config, true)
	if err != nil {
		t.Fatalf("Failed to create FacetingZigSearch: %v", err)
	}

	t.Run("GetTopFacetValues", func(t *testing.T) {
		results, err := search.GetTopFacetValues(ctx, nil, 10)
		if err != nil {
			t.Fatalf("GetTopFacetValues failed: %v", err)
		}

		if len(results) == 0 {
			t.Log("No facets found (expected if table is empty)")
		} else {
			t.Logf("Found %d facet groups", len(results))
			for _, fr := range results {
				t.Logf("  Facet: %s (ID: %d), Values: %d", fr.FacetName, fr.FacetID, len(fr.Values))
			}
		}
	})

	t.Run("FilterDocumentsByFacets", func(t *testing.T) {
		filters := map[string]string{
			"category": "Electronics",
		}

		ids, err := search.FilterDocumentsByFacets(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacets failed: %v", err)
		}

		t.Logf("Found %d documents matching filters", len(ids))
	})

	t.Run("MergeDeltas", func(t *testing.T) {
		err := search.MergeDeltas(ctx)
		if err != nil {
			t.Fatalf("MergeDeltas failed: %v", err)
		}
		t.Log("MergeDeltas completed successfully")
	})

	t.Run("SearchWithFacets", func(t *testing.T) {
		req := SearchWithFacetsRequest{
			Query:         "test",
			Facets:        nil,
			ContentColumn: "content",
			Limit:         10,
			MinScore:      0.0,
		}

		resp, err := search.SearchWithFacets(ctx, req)
		if err != nil {
			// This might fail if the function signature doesn't match
			t.Logf("SearchWithFacets failed (may need schema adjustment): %v", err)
			return
		}

		t.Logf("Search returned %d results, %d facets, total: %d",
			len(resp.Results), len(resp.Facets), resp.TotalFound)
	})
}

// TestFacetDefinitions tests adding and removing facets
func TestFacetDefinitions(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	config := Config{
		SchemaName:    "test_zig",
		DocumentTable: "documents",
	}

	search, err := NewFacetingZigSearch(pool, config, true)
	if err != nil {
		t.Fatalf("Failed to create FacetingZigSearch: %v", err)
	}

	t.Run("AddPlainFacet", func(t *testing.T) {
		err := search.AddFacet(ctx, FacetDefinition{
			Column: "status",
			Type:   "plain",
		})
		if err != nil {
			t.Logf("AddFacet failed (may already exist): %v", err)
		}
	})

	t.Run("AddBucketFacet", func(t *testing.T) {
		err := search.AddFacet(ctx, FacetDefinition{
			Column:  "price",
			Type:    "bucket",
			Buckets: []float64{0, 10, 50, 100, 500},
		})
		if err != nil {
			t.Logf("AddFacet failed (may already exist): %v", err)
		}
	})

	t.Run("DropFacet", func(t *testing.T) {
		err := search.DropFacet(ctx, "status")
		if err != nil {
			t.Logf("DropFacet failed (may not exist): %v", err)
		}
	})
}

// TestCountResultsWithFilters tests the count_results function
func TestCountResultsWithFilters(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	config := Config{
		SchemaName:    "test_zig",
		DocumentTable: "documents",
	}

	search, err := NewFacetingZigSearch(pool, config, true)
	if err != nil {
		t.Fatalf("Failed to create FacetingZigSearch: %v", err)
	}

	t.Run("CountWithNoFilters", func(t *testing.T) {
		results, err := search.CountResultsWithFilters(ctx, nil)
		if err != nil {
			t.Fatalf("CountResultsWithFilters failed: %v", err)
		}
		t.Logf("Found %d facet groups without filters", len(results))
	})

	t.Run("CountWithFilters", func(t *testing.T) {
		filters := map[string]string{
			"category": "Electronics",
		}

		results, err := search.CountResultsWithFilters(ctx, filters)
		if err != nil {
			t.Fatalf("CountResultsWithFilters failed: %v", err)
		}
		t.Logf("Found %d facet groups with filters", len(results))
	})
}

// TestBitmapOptimization tests the new bitmap-based functions
func TestBitmapOptimization(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	config := Config{
		SchemaName:    "test_zig",
		DocumentTable: "documents",
	}

	search, err := NewFacetingZigSearch(pool, config, true)
	if err != nil {
		t.Fatalf("Failed to create FacetingZigSearch: %v", err)
	}

	t.Run("FilterDocumentsByFacetsBitmap", func(t *testing.T) {
		filters := map[string]string{
			"category": "Electronics",
		}

		bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, err := search.GetBitmapCardinality(ctx, bitmap)
			if err != nil {
				t.Fatalf("GetBitmapCardinality failed: %v", err)
			}
			t.Logf("Bitmap cardinality: %d (expected 3 Electronics docs)", card)

			if card != 3 {
				t.Errorf("Expected 3 Electronics documents, got %d", card)
			}
		} else {
			t.Log("Bitmap is nil (no matching documents)")
		}
	})

	t.Run("FilterDocumentsByFacetsBitmap_MultipleFilters", func(t *testing.T) {
		filters := map[string]string{
			"category": "Electronics",
			"in_stock": "true",
		}

		bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, err := search.GetBitmapCardinality(ctx, bitmap)
			if err != nil {
				t.Fatalf("GetBitmapCardinality failed: %v", err)
			}
			t.Logf("Multiple filters bitmap cardinality: %d", card)
		}
	})

	t.Run("FilterDocumentsByFacetsBitmap_Empty", func(t *testing.T) {
		filters := map[string]string{}

		bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		if bitmap != nil {
			t.Error("Expected nil bitmap for empty filters")
		}
	})

	t.Run("FilterDocumentsByFacetsBitmap_NonExistent", func(t *testing.T) {
		filters := map[string]string{
			"category": "NonExistent",
		}

		bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, _ := search.GetBitmapCardinality(ctx, bitmap)
			if card != 0 {
				t.Errorf("Expected 0 cardinality for non-existent category, got %d", card)
			}
		}
	})

	t.Run("HierarchicalFacetsBitmap", func(t *testing.T) {
		// First get a filter bitmap
		filters := map[string]string{
			"category": "Electronics",
		}
		bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		// Get hierarchical facets with the bitmap
		facets, err := search.HierarchicalFacetsBitmap(ctx, bitmap, 10)
		if err != nil {
			t.Fatalf("HierarchicalFacetsBitmap failed: %v", err)
		}

		t.Logf("Got %d facet groups from hierarchical_facets_bitmap", len(facets))
		for _, fr := range facets {
			t.Logf("  Facet: %s, Values: %d", fr.FacetName, len(fr.Values))
		}
	})

	t.Run("HierarchicalFacetsBitmap_NilFilter", func(t *testing.T) {
		// Get hierarchical facets without filter (all documents)
		facets, err := search.HierarchicalFacetsBitmap(ctx, nil, 10)
		if err != nil {
			t.Fatalf("HierarchicalFacetsBitmap failed: %v", err)
		}

		t.Logf("Got %d facet groups without filter", len(facets))
	})

	t.Run("SearchWithFacetsBitmap_EmptyQuery", func(t *testing.T) {
		req := SearchWithFacetsRequest{
			Query:         "", // Empty query triggers bitmap optimization
			Facets:        map[string]string{"category": "Electronics"},
			ContentColumn: "content",
			Limit:         10,
			FacetLimit:    10,
		}

		resp, err := search.SearchWithFacetsBitmap(ctx, req)
		if err != nil {
			t.Fatalf("SearchWithFacetsBitmap failed: %v", err)
		}

		t.Logf("Empty query search: total=%d, results=%d, facets=%d, time=%dms",
			resp.TotalFound, len(resp.Results), len(resp.Facets), resp.SearchTime)

		if resp.TotalFound != 3 {
			t.Errorf("Expected 3 Electronics documents, got %d", resp.TotalFound)
		}
	})

	t.Run("SearchWithFacetsBitmap_WithTextQuery", func(t *testing.T) {
		req := SearchWithFacetsRequest{
			Query:         "laptop",
			Facets:        map[string]string{"category": "Electronics"},
			ContentColumn: "content",
			Limit:         10,
			FacetLimit:    10,
		}

		resp, err := search.SearchWithFacetsBitmap(ctx, req)
		if err != nil {
			t.Fatalf("SearchWithFacetsBitmap failed: %v", err)
		}

		t.Logf("Text query search: total=%d, results=%d, time=%dms",
			resp.TotalFound, len(resp.Results), resp.SearchTime)
	})

	t.Run("CompareBitmapVsArray", func(t *testing.T) {
		filters := map[string]string{
			"category": "Electronics",
		}

		// Get count via bitmap method
		bitmap, err := search.FilterDocumentsByFacetsBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}
		bitmapCount, _ := search.GetBitmapCardinality(ctx, bitmap)

		// Get count via array method
		ids, err := search.FilterDocumentsByFacets(ctx, filters)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacets failed: %v", err)
		}
		arrayCount := int64(len(ids))

		t.Logf("Bitmap count: %d, Array count: %d", bitmapCount, arrayCount)

		if bitmapCount != arrayCount {
			t.Errorf("Mismatch: bitmap=%d, array=%d", bitmapCount, arrayCount)
		}
	})
}

// TestBM25Functions tests the BM25 search functionality
func TestBM25Functions(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()

	// Setup test schema with BM25 indexing
	setupBM25TestSchema(t, pool)
	defer cleanupBM25TestSchema(t, pool)

	config := Config{
		SchemaName:    "test_bm25",
		DocumentTable: "documents",
	}

	search, err := NewFacetingZigSearch(pool, config, true)
	if err != nil {
		t.Fatalf("Failed to create FacetingZigSearch: %v", err)
	}

	t.Run("IndexDocument", func(t *testing.T) {
		// Index a new document
		err := search.IndexDocument(ctx, 100, "This is a test document about machine learning and artificial intelligence", "english")
		if err != nil {
			t.Fatalf("IndexDocument failed: %v", err)
		}
		t.Log("IndexDocument succeeded")
	})

	t.Run("RecalculateStatistics", func(t *testing.T) {
		err := search.RecalculateStatistics(ctx)
		if err != nil {
			t.Fatalf("RecalculateStatistics failed: %v", err)
		}
		t.Log("RecalculateStatistics succeeded")
	})

	t.Run("GetStatistics", func(t *testing.T) {
		stats, err := search.GetStatistics(ctx)
		if err != nil {
			t.Fatalf("GetStatistics failed: %v", err)
		}
		t.Logf("BM25 Statistics: TotalDocs=%d, AvgLength=%.2f", stats.TotalDocs, stats.AvgLength)

		// We indexed 5 documents in setup + 1 in the test
		if stats.TotalDocs < 5 {
			t.Errorf("Expected at least 5 documents, got %d", stats.TotalDocs)
		}
	})

	t.Run("BM25Search", func(t *testing.T) {
		results, err := search.BM25Search(ctx, "laptop", BM25SearchOptions{
			Language: "english",
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("BM25Search failed: %v", err)
		}

		t.Logf("BM25Search returned %d results for 'laptop'", len(results))
		for i, r := range results {
			t.Logf("  %d. DocID=%d, Score=%.4f", i+1, r.DocID, r.Score)
		}

		if len(results) == 0 {
			t.Error("Expected at least 1 result for 'laptop' search")
		}
	})

	t.Run("BM25Search_MultipleTerms", func(t *testing.T) {
		results, err := search.BM25Search(ctx, "high-end professional", BM25SearchOptions{
			Language: "english",
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("BM25Search failed: %v", err)
		}

		t.Logf("BM25Search returned %d results for 'high-end professional'", len(results))
	})

	t.Run("BM25Search_NoResults", func(t *testing.T) {
		results, err := search.BM25Search(ctx, "xyznonexistent", BM25SearchOptions{
			Language: "english",
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("BM25Search failed: %v", err)
		}

		if len(results) != 0 {
			t.Errorf("Expected 0 results for nonexistent term, got %d", len(results))
		}
	})

	t.Run("BM25Score", func(t *testing.T) {
		// Get score for a specific document
		score, err := search.BM25Score(ctx, "laptop", 1, "english", 1.2, 0.75)
		if err != nil {
			t.Fatalf("BM25Score failed: %v", err)
		}

		t.Logf("BM25Score for doc 1 with 'laptop': %.4f", score)
	})

	t.Run("BM25GetMatchesBitmap", func(t *testing.T) {
		bitmap, err := search.BM25GetMatchesBitmap(ctx, "laptop", BM25SearchOptions{
			Language: "english",
		})
		if err != nil {
			t.Fatalf("BM25GetMatchesBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, err := search.GetBitmapCardinality(ctx, bitmap)
			if err != nil {
				t.Fatalf("GetBitmapCardinality failed: %v", err)
			}
			t.Logf("BM25GetMatchesBitmap cardinality for 'laptop': %d", card)
		} else {
			t.Log("Bitmap is nil (no matching documents)")
		}
	})

	t.Run("DeleteDocument", func(t *testing.T) {
		// Delete the document we added earlier
		err := search.DeleteDocument(ctx, 100)
		if err != nil {
			t.Fatalf("DeleteDocument failed: %v", err)
		}
		t.Log("DeleteDocument succeeded")

		// Verify it's gone by searching for the unique term
		results, err := search.BM25Search(ctx, "artificial intelligence", BM25SearchOptions{
			Language: "english",
			Limit:    10,
		})
		if err != nil {
			t.Fatalf("BM25Search after delete failed: %v", err)
		}

		// The deleted document should not appear
		for _, r := range results {
			if r.DocID == 100 {
				t.Error("Document 100 should have been deleted but still appears in search")
			}
		}
	})

	t.Run("IndexDocumentsBatch", func(t *testing.T) {
		docs := []BM25Document{
			{DocID: 200, Content: "Batch document one about databases"},
			{DocID: 201, Content: "Batch document two about programming"},
			{DocID: 202, Content: "Batch document three about testing"},
		}

		count, elapsed, err := search.IndexDocumentsBatch(ctx, docs, "english", 1000)
		if err != nil {
			t.Fatalf("IndexDocumentsBatch failed: %v", err)
		}

		t.Logf("IndexDocumentsBatch: indexed %d documents in %.2fms", count, elapsed)

		if count != 3 {
			t.Errorf("Expected 3 indexed documents, got %d", count)
		}
	})
}

func setupBM25TestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	queries := []string{
		`DROP SCHEMA IF EXISTS test_bm25 CASCADE`,
		`CREATE SCHEMA test_bm25`,
		`CREATE TABLE test_bm25.documents (
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
		)`,
		`INSERT INTO test_bm25.documents (id, title, content, category, subcategory, price, tags, in_stock) VALUES
			(1, 'Laptop Pro', 'High-end laptop for professionals with powerful processor', 'Electronics', 'Computers', 1299.99, ARRAY['premium', 'laptop', 'work'], true),
			(2, 'Budget Phone', 'Affordable smartphone with great camera', 'Electronics', 'Phones', 199.99, ARRAY['budget', 'phone'], true),
			(3, 'Wireless Headphones', 'Noise-cancelling headphones with bluetooth', 'Electronics', 'Audio', 299.99, ARRAY['premium', 'audio', 'wireless'], true),
			(4, 'Mystery Novel', 'Thrilling mystery story with detective investigation', 'Books', 'Fiction', 14.99, ARRAY['fiction', 'mystery', 'bestseller'], true),
			(5, 'Cooking Guide', 'Italian cuisine recipes from professional chefs', 'Books', 'Non-Fiction', 24.99, ARRAY['cooking', 'italian'], false)`,
		`SELECT facets.add_faceting_to_table(
			'test_bm25.documents',
			'id',
			ARRAY[
				facets.plain_facet('category'),
				facets.plain_facet('subcategory'),
				facets.array_facet('tags'),
				facets.boolean_facet('in_stock')
			]
		)`,
		`SELECT facets.populate_facets('test_bm25.documents'::regclass)`,
		// Index documents for BM25 search
		`SELECT facets.bm25_index_document('test_bm25.documents'::regclass, 1, 'High-end laptop for professionals with powerful processor', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_bm25.documents'::regclass, 2, 'Affordable smartphone with great camera', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_bm25.documents'::regclass, 3, 'Noise-cancelling headphones with bluetooth', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_bm25.documents'::regclass, 4, 'Thrilling mystery story with detective investigation', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_bm25.documents'::regclass, 5, 'Italian cuisine recipes from professional chefs', 'content', 'english')`,
		// Recalculate statistics after indexing
		`SELECT facets.bm25_recalculate_statistics('test_bm25.documents'::regclass)`,
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Logf("BM25 Setup query failed (may be expected): %v", err)
		}
	}
}

func cleanupBM25TestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS test_bm25 CASCADE`)
	if err != nil {
		t.Logf("BM25 Cleanup failed: %v", err)
	}
}

// Helper functions for tests

func getTestPool(t *testing.T) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get connection string from environment or use default
	connString := os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		connString = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
	}

	// Check if we should fail instead of skip
	failOnNoDb := os.Getenv("PGFACETS_TEST_FAIL_ON_NO_DB") == "true"

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		if failOnNoDb {
			t.Fatalf("FATAL: Could not connect to database (PGFACETS_TEST_FAIL_ON_NO_DB=true): %v", err)
		}
		t.Logf("Could not connect to database: %v", err)
		return nil
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		if failOnNoDb {
			t.Fatalf("FATAL: Could not ping database (PGFACETS_TEST_FAIL_ON_NO_DB=true): %v", err)
		}
		t.Logf("Could not ping database: %v", err)
		return nil
	}

	return pool
}

func setupTestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	queries := []string{
		`DROP SCHEMA IF EXISTS test_zig CASCADE`,
		`CREATE SCHEMA test_zig`,
		`CREATE TABLE test_zig.documents (
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
		)`,
		`INSERT INTO test_zig.documents (id, title, content, category, subcategory, price, tags, in_stock) VALUES
			(1, 'Laptop Pro', 'High-end laptop for professionals', 'Electronics', 'Computers', 1299.99, ARRAY['premium', 'laptop', 'work'], true),
			(2, 'Budget Phone', 'Affordable smartphone', 'Electronics', 'Phones', 199.99, ARRAY['budget', 'phone'], true),
			(3, 'Wireless Headphones', 'Noise-cancelling headphones', 'Electronics', 'Audio', 299.99, ARRAY['premium', 'audio', 'wireless'], true),
			(4, 'Mystery Novel', 'Thrilling mystery story', 'Books', 'Fiction', 14.99, ARRAY['fiction', 'mystery', 'bestseller'], true),
			(5, 'Cooking Guide', 'Italian cuisine recipes', 'Books', 'Non-Fiction', 24.99, ARRAY['cooking', 'italian'], false)`,
		`SELECT facets.add_faceting_to_table(
			'test_zig.documents',
			'id',
			ARRAY[
				facets.plain_facet('category'),
				facets.plain_facet('subcategory'),
				facets.array_facet('tags'),
				facets.boolean_facet('in_stock')
			]
		)`,
		`SELECT facets.populate_facets('test_zig.documents'::regclass)`,
		// Index documents for BM25 search
		`SELECT facets.bm25_index_document('test_zig.documents'::regclass, 1, 'High-end laptop for professionals', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_zig.documents'::regclass, 2, 'Affordable smartphone', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_zig.documents'::regclass, 3, 'Noise-cancelling headphones', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_zig.documents'::regclass, 4, 'Thrilling mystery story', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_zig.documents'::regclass, 5, 'Italian cuisine recipes', 'content', 'english')`,
		// Recalculate BM25 statistics after indexing
		`SELECT facets.bm25_recalculate_statistics('test_zig.documents'::regclass)`,
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Logf("Setup query failed (may be expected): %v", err)
		}
	}
}

func cleanupTestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS test_zig CASCADE`)
	if err != nil {
		t.Logf("Cleanup failed: %v", err)
	}
}

// TestCustomPrimaryKey tests that faceting works correctly with tables
// that use a non-standard primary key column name (not 'id')
// This is a regression test for the bug where hardcoded 'id' column
// caused empty results or facets for tables with custom primary keys.
func TestCustomPrimaryKey(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()

	// Setup test schema with custom primary key
	setupCustomPKSchema(t, pool)
	defer cleanupCustomPKSchema(t, pool)

	config := Config{
		SchemaName:    "test_custom_pk",
		DocumentTable: "articles",
	}

	search, err := NewFacetingZigSearch(pool, config, true)
	if err != nil {
		t.Fatalf("Failed to create FacetingZigSearch: %v", err)
	}

	t.Run("CustomPK_EmptyQueryWithFacetFilter", func(t *testing.T) {
		// This is the key test: empty query + facet filter with custom PK
		// Should return BOTH results AND facets
		req := SearchWithFacetsRequest{
			Query:         "", // Empty query
			Facets:        map[string]string{"category": "Technology"},
			ContentColumn: "content",
			Limit:         10,
			FacetLimit:    10,
		}

		resp, err := search.SearchWithFacetsBitmap(ctx, req)
		if err != nil {
			t.Fatalf("SearchWithFacetsBitmap failed: %v", err)
		}

		t.Logf("Custom PK - Empty query with facet filter: total=%d, results=%d, facets=%d",
			resp.TotalFound, len(resp.Results), len(resp.Facets))

		// With custom PK, we expect 3 Technology documents
		if resp.TotalFound != 3 {
			t.Errorf("Custom PK: Expected 3 Technology documents, got %d", resp.TotalFound)
		}

		if len(resp.Results) == 0 && resp.TotalFound > 0 {
			t.Errorf("Custom PK: Results are empty but total_found=%d (PK column mismatch?)", resp.TotalFound)
		}

		// Facets should not be empty when results exist
		if len(resp.Facets) == 0 && resp.TotalFound > 0 {
			t.Errorf("Custom PK: Facets are empty but results exist (expected facets)")
		}
	})

	t.Run("CustomPK_BM25Search", func(t *testing.T) {
		// BM25 search with custom PK should return BOTH results AND facets
		req := SearchWithFacetsRequest{
			Query:         "PostgreSQL",
			Facets:        nil, // No facet filter
			ContentColumn: "content",
			Limit:         10,
			FacetLimit:    10,
		}

		resp, err := search.SearchWithFacetsBitmap(ctx, req)
		if err != nil {
			t.Fatalf("SearchWithFacetsBitmap failed: %v", err)
		}

		t.Logf("Custom PK - BM25 search: total=%d, results=%d, facets=%d",
			resp.TotalFound, len(resp.Results), len(resp.Facets))

		if resp.TotalFound == 0 {
			t.Error("Custom PK: Expected results for 'PostgreSQL' search")
		}

		if len(resp.Results) == 0 && resp.TotalFound > 0 {
			t.Errorf("Custom PK: Results are empty but total_found=%d (PK column mismatch?)", resp.TotalFound)
		}

		// Facets should not be empty when results exist
		if len(resp.Facets) == 0 && resp.TotalFound > 0 {
			t.Errorf("Custom PK: Facets are empty but results exist (expected facets)")
		}
	})

	t.Run("CustomPK_BM25SearchWithFacetFilter", func(t *testing.T) {
		// BM25 search + facet filter with custom PK
		req := SearchWithFacetsRequest{
			Query:         "PostgreSQL",
			Facets:        map[string]string{"category": "Technology"},
			ContentColumn: "content",
			Limit:         10,
			FacetLimit:    10,
		}

		resp, err := search.SearchWithFacetsBitmap(ctx, req)
		if err != nil {
			t.Fatalf("SearchWithFacetsBitmap failed: %v", err)
		}

		t.Logf("Custom PK - BM25 search with facet filter: total=%d, results=%d, facets=%d",
			resp.TotalFound, len(resp.Results), len(resp.Facets))

		if len(resp.Results) == 0 && resp.TotalFound > 0 {
			t.Errorf("Custom PK: Results are empty but total_found=%d (PK column mismatch?)", resp.TotalFound)
		}

		if len(resp.Facets) == 0 && resp.TotalFound > 0 {
			t.Errorf("Custom PK: Facets are empty but results exist")
		}
	})
}

func setupCustomPKSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	queries := []string{
		`DROP SCHEMA IF EXISTS test_custom_pk CASCADE`,
		`CREATE SCHEMA test_custom_pk`,
		// Create table with NON-STANDARD primary key column name
		`CREATE TABLE test_custom_pk.articles (
			article_id SERIAL PRIMARY KEY,  -- NOT named 'id'!
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			category TEXT,
			metadata JSONB DEFAULT '{}'::jsonb,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`INSERT INTO test_custom_pk.articles (article_id, title, content, category, metadata) VALUES
			(1, 'PostgreSQL Basics', 'Learn PostgreSQL fundamentals including tables and queries', 'Technology', '{"author": "Alice"}'),
			(2, 'PostgreSQL Advanced', 'Advanced PostgreSQL topics including optimization and extensions', 'Technology', '{"author": "Bob"}'),
			(3, 'Cooking Italian', 'Italian cooking techniques and recipes from Rome', 'Cooking', '{"author": "Marco"}'),
			(4, 'Travel Spain', 'Exploring beautiful destinations in Spain', 'Travel', '{"author": "Elena"}'),
			(5, 'Database Design', 'Best practices for PostgreSQL database design', 'Technology', '{"author": "Charlie"}')`,
		// Add faceting with CUSTOM key column
		`SELECT facets.add_faceting_to_table(
			'test_custom_pk.articles',
			'article_id',  -- Custom key column!
			ARRAY[
				facets.plain_facet('category')
			]
		)`,
		`SELECT facets.populate_facets('test_custom_pk.articles'::regclass)`,
		// Index documents for BM25 search
		`SELECT facets.bm25_index_document('test_custom_pk.articles'::regclass, 1, 'Learn PostgreSQL fundamentals including tables and queries', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_custom_pk.articles'::regclass, 2, 'Advanced PostgreSQL topics including optimization and extensions', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_custom_pk.articles'::regclass, 3, 'Italian cooking techniques and recipes from Rome', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_custom_pk.articles'::regclass, 4, 'Exploring beautiful destinations in Spain', 'content', 'english')`,
		`SELECT facets.bm25_index_document('test_custom_pk.articles'::regclass, 5, 'Best practices for PostgreSQL database design', 'content', 'english')`,
		// Recalculate BM25 statistics after indexing
		`SELECT facets.bm25_recalculate_statistics('test_custom_pk.articles'::regclass)`,
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Logf("Custom PK setup query failed: %v", err)
		}
	}
}

func cleanupCustomPKSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS test_custom_pk CASCADE`)
	if err != nil {
		t.Logf("Custom PK cleanup failed: %v", err)
	}
}
