// faceting_zig_native_test.go
// Tests for pgfaceting_zig native functions (version 0.4.1)
// Includes bitmap optimization tests for large datasets
//
// To run tests:
//   ./run_tests.sh  (recommended - starts Docker and runs tests)
//
// Or manually:
//   export TEST_DATABASE_URL="postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
//   go test -v ./...

package pgfaceting

import (
	"context"
	"testing"
)

func TestNativeFaceting(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	native := NewNativeFaceting(pool, "test_zig", "documents", true)

	t.Run("GetTableOID", func(t *testing.T) {
		oid, err := native.GetTableOID(ctx)
		if err != nil {
			t.Fatalf("GetTableOID failed: %v", err)
		}
		t.Logf("Table OID: %d", oid)
		if oid == 0 {
			t.Error("Expected non-zero OID")
		}
	})

	t.Run("BuildFilterBitmap", func(t *testing.T) {
		filters := []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		}

		bitmap, err := native.BuildFilterBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("BuildFilterBitmap failed: %v", err)
		}

		if bitmap != nil {
			t.Logf("Bitmap size: %d bytes", len(bitmap))

			// Get cardinality
			card, err := native.GetBitmapCardinality(ctx, bitmap)
			if err != nil {
				t.Fatalf("GetBitmapCardinality failed: %v", err)
			}
			t.Logf("Bitmap cardinality: %d", card)
		} else {
			t.Log("Bitmap is nil (no matching documents)")
		}
	})

	t.Run("GetFacetCounts", func(t *testing.T) {
		// First build a filter bitmap
		filters := []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		}
		bitmap, _ := native.BuildFilterBitmap(ctx, filters)

		// Get facet counts with the bitmap
		results, err := native.GetFacetCounts(ctx, bitmap, nil, 10)
		if err != nil {
			t.Fatalf("GetFacetCounts failed: %v", err)
		}

		t.Logf("Found %d facet count results", len(results))
		for _, r := range results {
			t.Logf("  %s: %s = %d (facet_id: %d)", r.FacetName, r.FacetValue, r.Cardinality, r.FacetID)
		}
	})

	t.Run("GetFacetCountsNoFilter", func(t *testing.T) {
		// Get all facet counts without filter
		results, err := native.GetFacetCounts(ctx, nil, nil, 10)
		if err != nil {
			t.Fatalf("GetFacetCounts failed: %v", err)
		}

		t.Logf("Found %d facet count results (no filter)", len(results))
	})

	t.Run("SearchDocuments", func(t *testing.T) {
		filters := []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		}

		ids, err := native.SearchDocuments(ctx, filters, 100, 0)
		if err != nil {
			t.Fatalf("SearchDocuments failed: %v", err)
		}

		t.Logf("Found %d document IDs", len(ids))
		if len(ids) > 0 {
			t.Logf("First 5 IDs: %v", ids[:min(5, len(ids))])
		}
	})

	t.Run("MergeDeltasNative", func(t *testing.T) {
		err := native.MergeDeltasNative(ctx)
		if err != nil {
			t.Fatalf("MergeDeltasNative failed: %v", err)
		}
		t.Log("MergeDeltasNative completed successfully")
	})

	t.Run("BitmapOperations", func(t *testing.T) {
		// Build two bitmaps
		bitmap1, err := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		})
		if err != nil {
			t.Fatalf("BuildFilterBitmap (1) failed: %v", err)
		}

		bitmap2, err := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "in_stock", FacetValue: "true"},
		})
		if err != nil {
			t.Fatalf("BuildFilterBitmap (2) failed: %v", err)
		}

		if bitmap1 == nil || bitmap2 == nil {
			t.Log("One or both bitmaps are nil, skipping AND/OR tests")
			return
		}

		// AND operation
		andResult, err := native.AndBitmaps(ctx, bitmap1, bitmap2)
		if err != nil {
			t.Fatalf("AndBitmaps failed: %v", err)
		}
		andCard, _ := native.GetBitmapCardinality(ctx, andResult)
		t.Logf("AND result cardinality: %d", andCard)

		// OR operation
		orResult, err := native.OrBitmaps(ctx, bitmap1, bitmap2)
		if err != nil {
			t.Fatalf("OrBitmaps failed: %v", err)
		}
		orCard, _ := native.GetBitmapCardinality(ctx, orResult)
		t.Logf("OR result cardinality: %d", orCard)
	})

	t.Run("BitmapToArray", func(t *testing.T) {
		bitmap, err := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		})
		if err != nil {
			t.Fatalf("BuildFilterBitmap failed: %v", err)
		}

		if bitmap == nil {
			t.Log("Bitmap is nil, skipping BitmapToArray test")
			return
		}

		ids, err := native.BitmapToArray(ctx, bitmap, 100)
		if err != nil {
			t.Fatalf("BitmapToArray failed: %v", err)
		}

		t.Logf("Converted bitmap to %d IDs", len(ids))
		if len(ids) > 0 {
			t.Logf("IDs: %v", ids)
		}
	})
}

func TestNativeFacetingMultipleFilters(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	native := NewNativeFaceting(pool, "test_zig", "documents", true)

	t.Run("MultipleFilters", func(t *testing.T) {
		filters := []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
			{FacetName: "in_stock", FacetValue: "true"},
		}

		bitmap, err := native.BuildFilterBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("BuildFilterBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, _ := native.GetBitmapCardinality(ctx, bitmap)
			t.Logf("Multiple filters bitmap cardinality: %d", card)
		}
	})

	t.Run("SearchWithMultipleFilters", func(t *testing.T) {
		filters := []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
			{FacetName: "in_stock", FacetValue: "true"},
		}

		ids, err := native.SearchDocuments(ctx, filters, 100, 0)
		if err != nil {
			t.Fatalf("SearchDocuments failed: %v", err)
		}

		t.Logf("Found %d documents matching multiple filters", len(ids))
	})
}

// TestBitmapOptimizationNative tests the new bitmap-based functions using native methods
func TestBitmapOptimizationNative(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	setupTestSchema(t, pool)
	defer cleanupTestSchema(t, pool)

	native := NewNativeFaceting(pool, "test_zig", "documents", true)

	t.Run("FilterDocumentsByFacetsBitmap", func(t *testing.T) {
		facets := map[string]string{
			"category": "Electronics",
		}

		bitmap, err := native.FilterDocumentsByFacetsBitmap(ctx, facets)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, err := native.GetBitmapCardinality(ctx, bitmap)
			if err != nil {
				t.Fatalf("GetBitmapCardinality failed: %v", err)
			}
			t.Logf("JSONB bitmap filter cardinality: %d", card)
		}
	})

	t.Run("FilterDocumentsByFacetsBitmap_MultipleFilters", func(t *testing.T) {
		facets := map[string]string{
			"category": "Electronics",
			"in_stock": "true",
		}

		bitmap, err := native.FilterDocumentsByFacetsBitmap(ctx, facets)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}

		if bitmap != nil {
			card, _ := native.GetBitmapCardinality(ctx, bitmap)
			t.Logf("Multiple JSONB filters bitmap cardinality: %d", card)
		}
	})

	t.Run("HierarchicalFacetsBitmap", func(t *testing.T) {
		// First get a filter bitmap
		bitmap, err := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		})
		if err != nil {
			t.Fatalf("BuildFilterBitmap failed: %v", err)
		}

		// Get hierarchical facets with the bitmap
		result, err := native.HierarchicalFacetsBitmap(ctx, bitmap, 10)
		if err != nil {
			t.Fatalf("HierarchicalFacetsBitmap failed: %v", err)
		}

		t.Logf("HierarchicalFacetsBitmap result size: %d bytes", len(result))
	})

	t.Run("HierarchicalFacetsBitmap_NilFilter", func(t *testing.T) {
		result, err := native.HierarchicalFacetsBitmap(ctx, nil, 10)
		if err != nil {
			t.Fatalf("HierarchicalFacetsBitmap failed: %v", err)
		}

		t.Logf("HierarchicalFacetsBitmap (no filter) result size: %d bytes", len(result))
	})

	t.Run("BitmapContains", func(t *testing.T) {
		bitmap, err := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		})
		if err != nil {
			t.Fatalf("BuildFilterBitmap failed: %v", err)
		}

		if bitmap == nil {
			t.Skip("Bitmap is nil")
		}

		// Check if document ID 1 is in the bitmap
		contains, err := native.BitmapContains(ctx, bitmap, 1)
		if err != nil {
			t.Fatalf("BitmapContains failed: %v", err)
		}
		t.Logf("Bitmap contains ID 1: %v", contains)
	})

	t.Run("BuildBitmapFromIDs", func(t *testing.T) {
		ids := []int64{1, 2, 3, 5, 8, 13, 21}

		bitmap, err := native.BuildBitmapFromIDs(ctx, ids)
		if err != nil {
			t.Fatalf("BuildBitmapFromIDs failed: %v", err)
		}

		if bitmap == nil {
			t.Fatal("Expected non-nil bitmap")
		}

		card, err := native.GetBitmapCardinality(ctx, bitmap)
		if err != nil {
			t.Fatalf("GetBitmapCardinality failed: %v", err)
		}

		if card != int64(len(ids)) {
			t.Errorf("Expected cardinality %d, got %d", len(ids), card)
		}
		t.Logf("Built bitmap with cardinality: %d", card)
	})

	t.Run("AndNotBitmaps", func(t *testing.T) {
		bitmap1, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		})
		bitmap2, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "in_stock", FacetValue: "false"},
		})

		if bitmap1 == nil {
			t.Skip("First bitmap is nil")
		}

		result, err := native.AndNotBitmaps(ctx, bitmap1, bitmap2)
		if err != nil {
			t.Fatalf("AndNotBitmaps failed: %v", err)
		}

		if result != nil {
			card, _ := native.GetBitmapCardinality(ctx, result)
			t.Logf("ANDNOT result cardinality: %d", card)
		}
	})

	t.Run("XorBitmaps", func(t *testing.T) {
		bitmap1, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		})
		bitmap2, _ := native.BuildFilterBitmap(ctx, []FacetFilter{
			{FacetName: "category", FacetValue: "Books"},
		})

		if bitmap1 == nil || bitmap2 == nil {
			t.Skip("One or both bitmaps are nil")
		}

		result, err := native.XorBitmaps(ctx, bitmap1, bitmap2)
		if err != nil {
			t.Fatalf("XorBitmaps failed: %v", err)
		}

		if result != nil {
			card, _ := native.GetBitmapCardinality(ctx, result)
			t.Logf("XOR result cardinality: %d", card)
		}
	})

	t.Run("IsBitmapEmpty", func(t *testing.T) {
		// Test with non-existent category (should be empty)
		bitmap, _ := native.FilterDocumentsByFacetsBitmap(ctx, map[string]string{
			"category": "NonExistent",
		})

		isEmpty, err := native.IsBitmapEmpty(ctx, bitmap)
		if err != nil {
			t.Fatalf("IsBitmapEmpty failed: %v", err)
		}

		t.Logf("Non-existent category bitmap is empty: %v", isEmpty)
		if !isEmpty {
			t.Error("Expected empty bitmap for non-existent category")
		}

		// Test with existing category (should not be empty)
		bitmap2, _ := native.FilterDocumentsByFacetsBitmap(ctx, map[string]string{
			"category": "Electronics",
		})

		isEmpty2, err := native.IsBitmapEmpty(ctx, bitmap2)
		if err != nil {
			t.Fatalf("IsBitmapEmpty failed: %v", err)
		}

		t.Logf("Electronics category bitmap is empty: %v", isEmpty2)
		if isEmpty2 {
			t.Error("Expected non-empty bitmap for Electronics category")
		}
	})

	t.Run("CompareMethods", func(t *testing.T) {
		facets := map[string]string{
			"category": "Electronics",
		}

		// Using new JSONB bitmap function
		bitmap, err := native.FilterDocumentsByFacetsBitmap(ctx, facets)
		if err != nil {
			t.Fatalf("FilterDocumentsByFacetsBitmap failed: %v", err)
		}
		bitmapCard, _ := native.GetBitmapCardinality(ctx, bitmap)

		// Using native filter function
		filters := []FacetFilter{
			{FacetName: "category", FacetValue: "Electronics"},
		}
		nativeBitmap, err := native.BuildFilterBitmap(ctx, filters)
		if err != nil {
			t.Fatalf("BuildFilterBitmap failed: %v", err)
		}
		nativeCard, _ := native.GetBitmapCardinality(ctx, nativeBitmap)

		t.Logf("JSONB bitmap cardinality: %d", bitmapCard)
		t.Logf("Native bitmap cardinality: %d", nativeCard)

		if bitmapCard != nativeCard {
			t.Errorf("Cardinality mismatch: JSONB=%d, Native=%d", bitmapCard, nativeCard)
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
