// faceting_zig_native.go
// Direct access to Zig native functions for high-performance operations (version 0.4.2)
// Includes bitmap optimization functions for large datasets and BM25 helper utilities

package pgfaceting

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NativeFaceting provides direct access to pgfaceting_zig native functions
type NativeFaceting struct {
	pool       *pgxpool.Pool
	schemaName string
	tableName  string
	debug      bool
}

// NewNativeFaceting creates a new NativeFaceting instance
func NewNativeFaceting(pool *pgxpool.Pool, schemaName, tableName string, debug bool) *NativeFaceting {
	return &NativeFaceting{
		pool:       pool,
		schemaName: schemaName,
		tableName:  tableName,
		debug:      debug,
	}
}

// FacetFilter represents a facet filter (name, value pair)
type FacetFilter struct {
	FacetName  string
	FacetValue string
}

// GetTableOID returns the OID of the table
func (n *NativeFaceting) GetTableOID(ctx context.Context) (uint32, error) {
	var oid uint32
	query := fmt.Sprintf(`SELECT '%s.%s'::regclass::oid`, n.schemaName, n.tableName)
	err := n.pool.QueryRow(ctx, query).Scan(&oid)
	if err != nil {
		return 0, fmt.Errorf("failed to get table OID: %w", err)
	}
	return oid, nil
}

// BuildFilterBitmap builds a roaring bitmap from facet filters using Zig native
// Returns the bitmap as a byte array (serialized roaringbitmap)
func (n *NativeFaceting) BuildFilterBitmap(ctx context.Context, filters []FacetFilter) ([]byte, error) {
	if len(filters) == 0 {
		return nil, nil
	}

	tableOID, err := n.GetTableOID(ctx)
	if err != nil {
		return nil, err
	}

	// Build filter array
	filterParts := make([]string, len(filters))
	for i, f := range filters {
		filterParts[i] = fmt.Sprintf("ROW('%s', '%s')::facets.facet_filter", f.FacetName, f.FacetValue)
	}
	filtersSQL := strings.Join(filterParts, ", ")

	// Cast to bytea to ensure proper binary protocol handling
	// The roaringbitmap::bytea cast is WITHOUT FUNCTION, meaning binary-compatible storage
	query := fmt.Sprintf(`SELECT build_filter_bitmap_native($1, ARRAY[%s])::bytea`, filtersSQL)

	var bitmap []byte
	err = n.pool.QueryRow(ctx, query, tableOID).Scan(&bitmap)
	if err != nil {
		return nil, fmt.Errorf("build_filter_bitmap_native failed: %w", err)
	}

	return bitmap, nil
}

// GetFacetCountsResult represents a row from get_facet_counts_native
type GetFacetCountsResult struct {
	FacetName   string
	FacetValue  string
	Cardinality int64
	FacetID     int
}

// GetFacetCounts returns facet counts using Zig native function
func (n *NativeFaceting) GetFacetCounts(ctx context.Context, filterBitmap []byte, facetNames []string, topN int) ([]GetFacetCountsResult, error) {
	tableOID, err := n.GetTableOID(ctx)
	if err != nil {
		return nil, err
	}

	if topN <= 0 {
		topN = 5
	}

	var facetNamesArg interface{} = nil
	if len(facetNames) > 0 {
		facetNamesArg = facetNames
	}

	var bitmapArg interface{} = nil
	if filterBitmap != nil {
		bitmapArg = filterBitmap
	}

	query := `SELECT * FROM get_facet_counts_native($1, $2::roaringbitmap, $3, $4)`

	rows, err := n.pool.Query(ctx, query, tableOID, bitmapArg, facetNamesArg, topN)
	if err != nil {
		return nil, fmt.Errorf("get_facet_counts_native failed: %w", err)
	}
	defer rows.Close()

	var results []GetFacetCountsResult
	for rows.Next() {
		var r GetFacetCountsResult
		if err := rows.Scan(&r.FacetName, &r.FacetValue, &r.Cardinality, &r.FacetID); err != nil {
			return nil, fmt.Errorf("failed to scan facet count: %w", err)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SearchDocuments returns document IDs matching filters using Zig native
func (n *NativeFaceting) SearchDocuments(ctx context.Context, filters []FacetFilter, limit, offset int) ([]int64, error) {
	tableOID, err := n.GetTableOID(ctx)
	if err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 100
	}

	// Build filter array
	var filtersArg string = "NULL"
	if len(filters) > 0 {
		filterParts := make([]string, len(filters))
		for i, f := range filters {
			filterParts[i] = fmt.Sprintf("ROW('%s', '%s')::facets.facet_filter", f.FacetName, f.FacetValue)
		}
		filtersArg = fmt.Sprintf("ARRAY[%s]", strings.Join(filterParts, ", "))
	}

	query := fmt.Sprintf(`SELECT * FROM search_documents_native($1, %s, $2, $3)`, filtersArg)

	rows, err := n.pool.Query(ctx, query, tableOID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("search_documents_native failed: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan document ID: %w", err)
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// MergeDeltasNative merges pending deltas using Zig native function
func (n *NativeFaceting) MergeDeltasNative(ctx context.Context) error {
	tableOID, err := n.GetTableOID(ctx)
	if err != nil {
		return err
	}

	_, err = n.pool.Exec(ctx, `SELECT merge_deltas_native($1)`, tableOID)
	if err != nil {
		return fmt.Errorf("merge_deltas_native failed: %w", err)
	}

	return nil
}

// GetBitmapCardinality returns the cardinality of a roaring bitmap
func (n *NativeFaceting) GetBitmapCardinality(ctx context.Context, bitmap []byte) (int64, error) {
	if bitmap == nil {
		return 0, nil
	}

	var cardinality int64
	err := n.pool.QueryRow(ctx, `SELECT rb_cardinality($1::roaringbitmap)`, bitmap).Scan(&cardinality)
	if err != nil {
		return 0, fmt.Errorf("rb_cardinality failed: %w", err)
	}

	return cardinality, nil
}

// AndBitmaps performs AND operation on two bitmaps
func (n *NativeFaceting) AndBitmaps(ctx context.Context, bitmap1, bitmap2 []byte) ([]byte, error) {
	if bitmap1 == nil || bitmap2 == nil {
		return nil, nil
	}

	var result []byte
	// Cast result to bytea for proper binary protocol handling
	err := n.pool.QueryRow(ctx, `SELECT rb_and($1::roaringbitmap, $2::roaringbitmap)::bytea`, bitmap1, bitmap2).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("rb_and failed: %w", err)
	}

	return result, nil
}

// OrBitmaps performs OR operation on two bitmaps
func (n *NativeFaceting) OrBitmaps(ctx context.Context, bitmap1, bitmap2 []byte) ([]byte, error) {
	if bitmap1 == nil {
		return bitmap2, nil
	}
	if bitmap2 == nil {
		return bitmap1, nil
	}

	var result []byte
	// Cast result to bytea for proper binary protocol handling
	err := n.pool.QueryRow(ctx, `SELECT rb_or($1::roaringbitmap, $2::roaringbitmap)::bytea`, bitmap1, bitmap2).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("rb_or failed: %w", err)
	}

	return result, nil
}

// BitmapToArray converts a roaring bitmap to an array of integers
func (n *NativeFaceting) BitmapToArray(ctx context.Context, bitmap []byte, limit int) ([]int64, error) {
	if bitmap == nil {
		return []int64{}, nil
	}

	if limit <= 0 {
		limit = 1000
	}

	query := `SELECT unnest(rb_to_array($1::roaringbitmap)::bigint[]) LIMIT $2`
	rows, err := n.pool.Query(ctx, query, bitmap, limit)
	if err != nil {
		return nil, fmt.Errorf("rb_to_array failed: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan ID: %w", err)
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// FilterDocumentsByFacetsBitmap returns a roaring bitmap for the given JSONB facets
// This is the efficient bitmap-based version that avoids array explosions
func (n *NativeFaceting) FilterDocumentsByFacetsBitmap(ctx context.Context, facets map[string]string) ([]byte, error) {
	if len(facets) == 0 {
		return nil, nil
	}

	query := `
		SELECT facets.filter_documents_by_facets_bitmap(
			$1, -- schema_name
			$2, -- facets (JSONB)
			$3  -- table_name
		)::bytea
	`

	var bitmap []byte
	err := n.pool.QueryRow(ctx, query, n.schemaName, facets, n.tableName).Scan(&bitmap)
	if err != nil {
		return nil, fmt.Errorf("filter_documents_by_facets_bitmap failed: %w", err)
	}

	return bitmap, nil
}

// HierarchicalFacetsBitmapJSON returns hierarchical facets as JSON using a bitmap filter directly
// This avoids the expensive array-to-bitmap conversion
func (n *NativeFaceting) HierarchicalFacetsBitmap(ctx context.Context, filterBitmap []byte, limit int) ([]byte, error) {
	tableOID, err := n.GetTableOID(ctx)
	if err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 10
	}

	// Returns JSONB directly - don't cast to bytea
	query := `SELECT facets.hierarchical_facets_bitmap($1, $2, $3::roaringbitmap)`

	var result []byte
	err = n.pool.QueryRow(ctx, query, tableOID, limit, filterBitmap).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("hierarchical_facets_bitmap failed: %w", err)
	}

	return result, nil
}

// BitmapContains checks if a bitmap contains a specific document ID
func (n *NativeFaceting) BitmapContains(ctx context.Context, bitmap []byte, docID int64) (bool, error) {
	if bitmap == nil {
		return false, nil
	}

	var contains bool
	err := n.pool.QueryRow(ctx, `SELECT rb_contains($1::roaringbitmap, $2::int4)`, bitmap, docID).Scan(&contains)
	if err != nil {
		return false, fmt.Errorf("rb_contains failed: %w", err)
	}

	return contains, nil
}

// BuildBitmapFromIDs creates a roaring bitmap from an array of document IDs
func (n *NativeFaceting) BuildBitmapFromIDs(ctx context.Context, ids []int64) ([]byte, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	// Convert int64 to int32 for PostgreSQL
	int32IDs := make([]int32, len(ids))
	for i, id := range ids {
		int32IDs[i] = int32(id)
	}

	var bitmap []byte
	err := n.pool.QueryRow(ctx, `SELECT rb_build($1::int4[])::bytea`, int32IDs).Scan(&bitmap)
	if err != nil {
		return nil, fmt.Errorf("rb_build failed: %w", err)
	}

	return bitmap, nil
}

// AndNotBitmaps performs ANDNOT operation (bitmap1 AND NOT bitmap2)
func (n *NativeFaceting) AndNotBitmaps(ctx context.Context, bitmap1, bitmap2 []byte) ([]byte, error) {
	if bitmap1 == nil {
		return nil, nil
	}
	if bitmap2 == nil {
		return bitmap1, nil
	}

	var result []byte
	err := n.pool.QueryRow(ctx, `SELECT rb_andnot($1::roaringbitmap, $2::roaringbitmap)::bytea`, bitmap1, bitmap2).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("rb_andnot failed: %w", err)
	}

	return result, nil
}

// XorBitmaps performs XOR operation on two bitmaps
func (n *NativeFaceting) XorBitmaps(ctx context.Context, bitmap1, bitmap2 []byte) ([]byte, error) {
	if bitmap1 == nil {
		return bitmap2, nil
	}
	if bitmap2 == nil {
		return bitmap1, nil
	}

	var result []byte
	err := n.pool.QueryRow(ctx, `SELECT rb_xor($1::roaringbitmap, $2::roaringbitmap)::bytea`, bitmap1, bitmap2).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("rb_xor failed: %w", err)
	}

	return result, nil
}

// IsBitmapEmpty checks if a bitmap is empty
func (n *NativeFaceting) IsBitmapEmpty(ctx context.Context, bitmap []byte) (bool, error) {
	if bitmap == nil {
		return true, nil
	}

	var isEmpty bool
	err := n.pool.QueryRow(ctx, `SELECT rb_is_empty($1::roaringbitmap)`, bitmap).Scan(&isEmpty)
	if err != nil {
		return false, fmt.Errorf("rb_is_empty failed: %w", err)
	}

	return isEmpty, nil
}
