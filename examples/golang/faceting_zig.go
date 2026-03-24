// faceting_zig.go
// Go client for pgfaceting_zig extension (version 0.4.2 compatible)

package pgfaceting

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds the configuration for faceting operations
type Config struct {
	SchemaName     string
	DocumentTable  string
	EmbeddingTable string
}

// FacetingZigSearch implements search using pgfaceting_zig extension
// This uses the 0.4.2 API signature with JSONB facets, bitmap optimization, BM25 search, and helper functions
type FacetingZigSearch struct {
	pool               *pgxpool.Pool
	config             Config
	metadataFacetNames map[string]bool
	debug              bool
}

// NewFacetingZigSearch creates a new instance using pgfaceting_zig
func NewFacetingZigSearch(pool *pgxpool.Pool, config Config, debug bool) (*FacetingZigSearch, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	search := &FacetingZigSearch{
		pool:   pool,
		config: config,
		debug:  debug,
	}

	if err := search.initMetadataFacets(); err != nil && debug {
		log.Printf("Warning: Failed to initialize metadata facets: %v\n", err)
	}

	return search, nil
}

// initMetadataFacets initializes the list of known metadata facets from the database
func (s *FacetingZigSearch) initMetadataFacets() error {
	ctx := context.Background()
	query := `
		SELECT DISTINCT SUBSTRING(facet_name FROM 4) AS facet_name_without_md 
		FROM facets.facet_definition 
		WHERE facet_name LIKE 'md_%'
	`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query metadata facets: %w", err)
	}
	defer rows.Close()

	s.metadataFacetNames = make(map[string]bool)

	for rows.Next() {
		var facetName string
		if err := rows.Scan(&facetName); err != nil {
			return fmt.Errorf("failed to scan facet name: %w", err)
		}
		s.metadataFacetNames[facetName] = true
	}

	return rows.Err()
}

// SearchWithFacetsRequest represents a search request using 0.4.1 API
type SearchWithFacetsRequest struct {
	Query         string            // Text query for BM25 search
	Facets        map[string]string // Facet filters as key-value pairs (JSONB format)
	VectorColumn  string            // Optional: column name for vector search
	ContentColumn string            // Column for text content (default: 'content')
	Limit         int
	Offset        int
	MinScore      float64
	VectorWeight  float64
	FacetLimit    int
	Language      string            // Optional: BM25 language (default: use table's bm25_language or 'english')
}

// SearchWithFacetsResponse represents the response from search_documents_with_facets
type SearchWithFacetsResponse struct {
	Results    []SearchResult `json:"results"`
	Facets     []FacetResult  `json:"facets"`
	TotalFound int64          `json:"total_found"`
	SearchTime int            `json:"search_time"`
}

// SearchResult represents a single search result
type SearchResult struct {
	ID            int64                  `json:"id"`
	Content       string                 `json:"content"`
	BM25Score     float64                `json:"bm25_score"`
	VectorScore   float64                `json:"vector_score"`
	CombinedScore float64                `json:"combined_score"`
	CreatedAt     JSONTime               `json:"created_at"`
	UpdatedAt     JSONTime               `json:"updated_at"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// JSONTime is a time.Time that can parse PostgreSQL timestamp formats
type JSONTime struct {
	time.Time
}

// UnmarshalJSON parses PostgreSQL timestamps (with or without timezone)
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	// Remove quotes
	s := string(data)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}

	if s == "null" || s == "" {
		t.Time = time.Time{}
		return nil
	}

	// Try various PostgreSQL timestamp formats
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}

	var err error
	for _, format := range formats {
		t.Time, err = time.Parse(format, s)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("cannot parse timestamp: %s", s)
}

// FacetResult represents facet counts from the search
type FacetResult struct {
	FacetName string       `json:"facet_name"`
	FacetID   int          `json:"facet_id"`
	Values    []FacetValue `json:"values"`
}

// FacetValue represents a single facet value with count
type FacetValue struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// SearchWithFacets performs a search using pgfaceting_zig 0.4.1 API
// This is the main search function using the JSONB-based facet interface with bitmap optimization and BM25 search
func (s *FacetingZigSearch) SearchWithFacets(ctx context.Context, req SearchWithFacetsRequest) (*SearchWithFacetsResponse, error) {
	startTime := time.Now()

	// Set defaults
	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.ContentColumn == "" {
		req.ContentColumn = "content"
	}
	if req.FacetLimit <= 0 {
		req.FacetLimit = 5
	}

	// Convert facets map to JSONB
	var facetsJSON interface{} = nil
	if len(req.Facets) > 0 {
		facetsJSON = req.Facets
	}

	// Build the query using 0.4.2 signature (includes p_language)
	query := `
		SELECT * FROM facets.search_documents_with_facets(
			$1,  -- p_schema_name
			$2,  -- p_table_name
			$3,  -- p_query
			$4,  -- p_facets (JSONB)
			$5,  -- p_vector_column
			$6,  -- p_content_column
			$7,  -- p_metadata_column
			$8,  -- p_created_at_column
			$9,  -- p_updated_at_column
			$10, -- p_limit
			$11, -- p_offset
			$12, -- p_min_score
			$13, -- p_vector_weight
			$14, -- p_facet_limit
			$15  -- p_language
		)
	`

	// Use language from request or default to nil (will use table's bm25_language)
	var language interface{} = nil
	if req.Language != "" {
		language = req.Language
	}

	args := []interface{}{
		s.config.SchemaName,
		s.config.DocumentTable,
		req.Query,
		facetsJSON,
		nilIfEmpty(req.VectorColumn),
		req.ContentColumn,
		"metadata",
		"created_at",
		"updated_at",
		req.Limit,
		req.Offset,
		req.MinScore,
		req.VectorWeight,
		req.FacetLimit,
		language,
	}

	if s.debug {
		log.Printf("Executing search_documents_with_facets: schema=%s, table=%s, query=%s",
			s.config.SchemaName, s.config.DocumentTable, req.Query)
	}

	// Execute query
	var resultsJSON, facetsResultJSON json.RawMessage
	var totalFound int64
	var searchTimeMs int

	err := s.pool.QueryRow(ctx, query, args...).Scan(&resultsJSON, &facetsResultJSON, &totalFound, &searchTimeMs)
	if err != nil {
		return nil, fmt.Errorf("search_documents_with_facets failed: %w", err)
	}

	// Parse results
	var results []SearchResult
	if err := json.Unmarshal(resultsJSON, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal results: %w", err)
	}

	var facets []FacetResult
	if err := json.Unmarshal(facetsResultJSON, &facets); err != nil {
		if s.debug {
			log.Printf("Failed to parse facets as array: %v", err)
		}
		facets = []FacetResult{}
	}

	elapsed := time.Since(startTime)
	if s.debug {
		log.Printf("Search completed: total=%d, results=%d, facets=%d, db_time=%dms, total_time=%dms",
			totalFound, len(results), len(facets), searchTimeMs, elapsed.Milliseconds())
	}

	return &SearchWithFacetsResponse{
		Results:    results,
		Facets:     facets,
		TotalFound: totalFound,
		SearchTime: searchTimeMs,
	}, nil
}

// FilterDocumentsByFacets returns document IDs matching the given facet filters
// Uses the Zig-native search_documents_native function
// WARNING: For large result sets (>100K documents), use FilterDocumentsByFacetsBitmap instead
func (s *FacetingZigSearch) FilterDocumentsByFacets(ctx context.Context, facets map[string]string) ([]int64, error) {
	if len(facets) == 0 {
		return []int64{}, nil
	}

	query := `
		SELECT * FROM facets.filter_documents_by_facets(
			$1, -- schema_name
			$2, -- facets (JSONB)
			$3  -- table_name
		)
	`

	rows, err := s.pool.Query(ctx, query, s.config.SchemaName, facets, s.config.DocumentTable)
	if err != nil {
		return nil, fmt.Errorf("filter_documents_by_facets failed: %w", err)
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

// FilterDocumentsByFacetsBitmap returns a roaring bitmap of matching document IDs
// This is MUCH more efficient for large result sets (millions of documents)
// The bitmap is returned as a serialized byte array
func (s *FacetingZigSearch) FilterDocumentsByFacetsBitmap(ctx context.Context, facets map[string]string) ([]byte, error) {
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
	err := s.pool.QueryRow(ctx, query, s.config.SchemaName, facets, s.config.DocumentTable).Scan(&bitmap)
	if err != nil {
		return nil, fmt.Errorf("filter_documents_by_facets_bitmap failed: %w", err)
	}

	return bitmap, nil
}

// GetBitmapCardinality returns the number of documents in a bitmap
func (s *FacetingZigSearch) GetBitmapCardinality(ctx context.Context, bitmap []byte) (int64, error) {
	if bitmap == nil {
		return 0, nil
	}

	var cardinality int64
	err := s.pool.QueryRow(ctx, `SELECT rb_cardinality($1::roaringbitmap)`, bitmap).Scan(&cardinality)
	if err != nil {
		return 0, fmt.Errorf("rb_cardinality failed: %w", err)
	}

	return cardinality, nil
}

// HierarchicalFacetsBitmap returns hierarchical facet counts using a bitmap filter
// This avoids the expensive array-to-bitmap conversion for large result sets
func (s *FacetingZigSearch) HierarchicalFacetsBitmap(ctx context.Context, filterBitmap []byte, limit int) ([]FacetResult, error) {
	if limit <= 0 {
		limit = 10
	}

	// Get table OID
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	var tableOID uint32
	err := s.pool.QueryRow(ctx, `SELECT $1::regclass::oid`, tableName).Scan(&tableOID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table OID: %w", err)
	}

	query := `SELECT facets.hierarchical_facets_bitmap($1, $2, $3::roaringbitmap)`

	var facetsJSON json.RawMessage
	err = s.pool.QueryRow(ctx, query, tableOID, limit, filterBitmap).Scan(&facetsJSON)
	if err != nil {
		return nil, fmt.Errorf("hierarchical_facets_bitmap failed: %w", err)
	}

	var facets []FacetResult
	if err := json.Unmarshal(facetsJSON, &facets); err != nil {
		if s.debug {
			log.Printf("Failed to parse facets: %v", err)
		}
		return []FacetResult{}, nil
	}

	return facets, nil
}

// SearchWithFacetsBitmap performs search using bitmap optimization for large result sets
// This is optimized for scenarios where facet filters match many documents
func (s *FacetingZigSearch) SearchWithFacetsBitmap(ctx context.Context, req SearchWithFacetsRequest) (*SearchWithFacetsResponse, error) {
	startTime := time.Now()

	// Set defaults
	if req.Limit <= 0 {
		req.Limit = 10
	}
	if req.ContentColumn == "" {
		req.ContentColumn = "content"
	}
	if req.FacetLimit <= 0 {
		req.FacetLimit = 5
	}

	// Convert facets map to JSONB
	var facetsJSON interface{} = nil
	if len(req.Facets) > 0 {
		facetsJSON = req.Facets
	}

	// The search_documents_with_facets function is now bitmap-optimized
	query := `
		SELECT * FROM facets.search_documents_with_facets(
			$1,  -- p_schema_name
			$2,  -- p_table_name
			$3,  -- p_query
			$4,  -- p_facets (JSONB)
			$5,  -- p_vector_column
			$6,  -- p_content_column
			$7,  -- p_metadata_column
			$8,  -- p_created_at_column
			$9,  -- p_updated_at_column
			$10, -- p_limit
			$11, -- p_offset
			$12, -- p_min_score
			$13, -- p_vector_weight
			$14, -- p_facet_limit
			$15  -- p_language
		)
	`

	// Use language from request or default to nil (will use table's bm25_language)
	var language interface{} = nil
	if req.Language != "" {
		language = req.Language
	}

	args := []interface{}{
		s.config.SchemaName,
		s.config.DocumentTable,
		req.Query,
		facetsJSON,
		nilIfEmpty(req.VectorColumn),
		req.ContentColumn,
		"metadata",
		"created_at",
		"updated_at",
		req.Limit,
		req.Offset,
		req.MinScore,
		req.VectorWeight,
		req.FacetLimit,
		language,
	}

	if s.debug {
		log.Printf("Executing search_documents_with_facets (bitmap-optimized): schema=%s, table=%s, query=%s",
			s.config.SchemaName, s.config.DocumentTable, req.Query)
	}

	// Execute query
	var resultsJSON, facetsResultJSON json.RawMessage
	var totalFound int64
	var searchTimeMs int

	err := s.pool.QueryRow(ctx, query, args...).Scan(&resultsJSON, &facetsResultJSON, &totalFound, &searchTimeMs)
	if err != nil {
		return nil, fmt.Errorf("search_documents_with_facets failed: %w", err)
	}

	// Parse results
	var results []SearchResult
	if err := json.Unmarshal(resultsJSON, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal results: %w", err)
	}

	var facets []FacetResult
	if err := json.Unmarshal(facetsResultJSON, &facets); err != nil {
		if s.debug {
			log.Printf("Failed to parse facets as array: %v", err)
		}
		facets = []FacetResult{}
	}

	elapsed := time.Since(startTime)
	if s.debug {
		log.Printf("Search completed (bitmap): total=%d, results=%d, facets=%d, db_time=%dms, total_time=%dms",
			totalFound, len(results), len(facets), searchTimeMs, elapsed.Milliseconds())
	}

	return &SearchWithFacetsResponse{
		Results:    results,
		Facets:     facets,
		TotalFound: totalFound,
		SearchTime: searchTimeMs,
	}, nil
}

// GetTopFacetValues returns the top N values for specified facets
func (s *FacetingZigSearch) GetTopFacetValues(ctx context.Context, facetNames []string, limit int) ([]FacetResult, error) {
	if limit <= 0 {
		limit = 10
	}

	var facetNamesArg interface{} = nil
	if len(facetNames) > 0 {
		facetNamesArg = facetNames
	}

	query := `
		SELECT facet_name, facet_value, cardinality, facet_id
		FROM facets.top_values($1::regclass, $2, $3)
	`

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	rows, err := s.pool.Query(ctx, query, tableName, limit, facetNamesArg)
	if err != nil {
		return nil, fmt.Errorf("top_values failed: %w", err)
	}
	defer rows.Close()

	// Group results by facet_name
	facetMap := make(map[string]*FacetResult)
	for rows.Next() {
		var facetName, facetValue string
		var cardinality int64
		var facetID int

		if err := rows.Scan(&facetName, &facetValue, &cardinality, &facetID); err != nil {
			return nil, fmt.Errorf("failed to scan facet value: %w", err)
		}

		if _, exists := facetMap[facetName]; !exists {
			facetMap[facetName] = &FacetResult{
				FacetName: facetName,
				FacetID:   facetID,
				Values:    []FacetValue{},
			}
		}

		facetMap[facetName].Values = append(facetMap[facetName].Values, FacetValue{
			Value: facetValue,
			Count: cardinality,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Convert map to slice
	results := make([]FacetResult, 0, len(facetMap))
	for _, fr := range facetMap {
		results = append(results, *fr)
	}

	return results, nil
}

// MergeDeltas applies pending delta updates to facet data
// Uses the Zig-native merge_deltas_native function for high performance
func (s *FacetingZigSearch) MergeDeltas(ctx context.Context) error {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)

	_, err := s.pool.Exec(ctx, `SELECT facets.merge_deltas($1::regclass)`, tableName)
	if err != nil {
		return fmt.Errorf("merge_deltas failed: %w", err)
	}

	if s.debug {
		log.Printf("Deltas merged successfully for %s", tableName)
	}

	return nil
}

// RunMaintenance runs the global faceting maintenance procedure
func (s *FacetingZigSearch) RunMaintenance(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CALL facets.run_maintenance()`)
	if err != nil {
		return fmt.Errorf("run_maintenance failed: %w", err)
	}

	if s.debug {
		log.Printf("Maintenance completed")
	}

	return nil
}

// FacetDefinition represents a facet to be added
type FacetDefinition struct {
	Column     string
	Type       string // plain, array, bucket, datetrunc, boolean
	CustomName string
	Buckets    []float64 // For bucket facets
	Precision  string    // For datetrunc facets (day, month, year)
}

// AddFacet adds a new facet definition to the table
func (s *FacetingZigSearch) AddFacet(ctx context.Context, facetDef FacetDefinition) error {
	var facetCall string

	switch facetDef.Type {
	case "plain":
		if facetDef.CustomName != "" {
			facetCall = fmt.Sprintf("facets.plain_facet('%s', '%s')", facetDef.Column, facetDef.CustomName)
		} else {
			facetCall = fmt.Sprintf("facets.plain_facet('%s')", facetDef.Column)
		}
	case "array":
		facetCall = fmt.Sprintf("facets.array_facet('%s')", facetDef.Column)
	case "bucket":
		var bucketStrs []string
		for _, b := range facetDef.Buckets {
			bucketStrs = append(bucketStrs, fmt.Sprintf("%g", b))
		}
		bucketsStr := strings.Join(bucketStrs, ",")
		facetCall = fmt.Sprintf("facets.bucket_facet('%s', ARRAY[%s])", facetDef.Column, bucketsStr)
	case "datetrunc":
		facetCall = fmt.Sprintf("facets.datetrunc_facet('%s', '%s')", facetDef.Column, facetDef.Precision)
	case "boolean":
		facetCall = fmt.Sprintf("facets.boolean_facet('%s')", facetDef.Column)
	case "rating":
		if facetDef.CustomName != "" {
			facetCall = fmt.Sprintf("facets.rating_facet('%s', '%s')", facetDef.Column, facetDef.CustomName)
		} else {
			facetCall = fmt.Sprintf("facets.rating_facet('%s')", facetDef.Column)
		}
	default:
		return fmt.Errorf("unsupported facet type: %s", facetDef.Type)
	}

	query := fmt.Sprintf(`
		SELECT facets.add_facets(
			'%s.%s',
			ARRAY[%s]
		)
	`, s.config.SchemaName, s.config.DocumentTable, facetCall)

	_, err := s.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("add_facets failed: %w", err)
	}

	return nil
}

// DropFacet removes a facet from the table
func (s *FacetingZigSearch) DropFacet(ctx context.Context, facetName string) error {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)

	_, err := s.pool.Exec(ctx, `SELECT facets.drop_facets($1::regclass, ARRAY[$2])`, tableName, facetName)
	if err != nil {
		return fmt.Errorf("drop_facets failed: %w", err)
	}

	return nil
}

// GetConfig returns the configuration
func (s *FacetingZigSearch) GetConfig() Config {
	return s.config
}

// CountResultsWithFilters returns facet counts filtered by the given facet filters
func (s *FacetingZigSearch) CountResultsWithFilters(ctx context.Context, filters map[string]string) ([]FacetResult, error) {
	if len(filters) == 0 {
		return s.GetTopFacetValues(ctx, nil, 100)
	}

	// Build filter array
	filterPairs := make([]string, 0, len(filters))
	for k, v := range filters {
		filterPairs = append(filterPairs, fmt.Sprintf("ROW('%s', '%s')::facets.facet_filter", k, v))
	}
	filtersSQL := strings.Join(filterPairs, ", ")

	query := fmt.Sprintf(`
		SELECT facet_name, facet_value, cardinality, facet_id
		FROM facets.count_results(
			'%s.%s'::regclass::oid,
			ARRAY[%s]
		)
	`, s.config.SchemaName, s.config.DocumentTable, filtersSQL)

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("count_results failed: %w", err)
	}
	defer rows.Close()

	// Group results by facet_name
	facetMap := make(map[string]*FacetResult)
	for rows.Next() {
		var facetName, facetValue string
		var cardinality int64
		var facetID int

		if err := rows.Scan(&facetName, &facetValue, &cardinality, &facetID); err != nil {
			return nil, fmt.Errorf("failed to scan count result: %w", err)
		}

		if _, exists := facetMap[facetName]; !exists {
			facetMap[facetName] = &FacetResult{
				FacetName: facetName,
				FacetID:   facetID,
				Values:    []FacetValue{},
			}
		}

		facetMap[facetName].Values = append(facetMap[facetName].Values, FacetValue{
			Value: facetValue,
			Count: cardinality,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Convert map to slice
	results := make([]FacetResult, 0, len(facetMap))
	for _, fr := range facetMap {
		results = append(results, *fr)
	}

	return results, nil
}

// ============================================================================
// BM25 Search Functions
// ============================================================================

// BM25SearchOptions holds options for BM25 search
type BM25SearchOptions struct {
	Language       string  // Text search config (default: "english")
	PrefixMatch    bool    // Enable prefix matching (default: false)
	FuzzyMatch     bool    // Enable fuzzy matching (default: false)
	FuzzyThreshold float64 // Fuzzy match threshold (default: 0.3)
	K1             float64 // BM25 k1 parameter (default: 1.2)
	B              float64 // BM25 b parameter (default: 0.75)
	Limit          int     // Maximum number of results (default: 10)
}

// BM25SearchResult represents a single BM25 search result
type BM25SearchResult struct {
	DocID int64   `json:"doc_id"`
	Score float64 `json:"score"`
}

// BM25Statistics represents BM25 collection statistics
type BM25Statistics struct {
	TotalDocs int64   `json:"total_docs"`
	AvgLength float64 `json:"avg_length"`
}

// IndexDocument indexes a document for BM25 search
// This adds or updates the document in the BM25 index
func (s *FacetingZigSearch) IndexDocument(ctx context.Context, docID int64, content string, language string) error {
	if language == "" {
		language = "english"
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT facets.bm25_index_document($1::regclass, $2, $3, 'content', $4)`

	_, err := s.pool.Exec(ctx, query, tableName, docID, content, language)
	if err != nil {
		return fmt.Errorf("bm25_index_document failed: %w", err)
	}

	if s.debug {
		log.Printf("Indexed document %d for BM25 search", docID)
	}

	return nil
}

// DeleteDocument removes a document from the BM25 index
func (s *FacetingZigSearch) DeleteDocument(ctx context.Context, docID int64) error {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT facets.bm25_delete_document($1::regclass, $2)`

	_, err := s.pool.Exec(ctx, query, tableName, docID)
	if err != nil {
		return fmt.Errorf("bm25_delete_document failed: %w", err)
	}

	if s.debug {
		log.Printf("Deleted document %d from BM25 index", docID)
	}

	return nil
}

// BM25Search performs a BM25 search and returns ranked results
func (s *FacetingZigSearch) BM25Search(ctx context.Context, query string, options BM25SearchOptions) ([]BM25SearchResult, error) {
	if options.Language == "" {
		options.Language = "english"
	}
	if options.Limit <= 0 {
		options.Limit = 10
	}
	if options.K1 == 0 {
		options.K1 = 1.2
	}
	if options.B == 0 {
		options.B = 0.75
	}
	if options.FuzzyThreshold == 0 {
		options.FuzzyThreshold = 0.3
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	sqlQuery := `
		SELECT doc_id, score 
		FROM facets.bm25_search(
			$1::regclass,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9
		)
		ORDER BY score DESC
	`

	rows, err := s.pool.Query(ctx, sqlQuery,
		tableName,
		query,
		options.Language,
		options.PrefixMatch,
		options.FuzzyMatch,
		options.FuzzyThreshold,
		options.K1,
		options.B,
		options.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("bm25_search failed: %w", err)
	}
	defer rows.Close()

	var results []BM25SearchResult
	for rows.Next() {
		var result BM25SearchResult
		if err := rows.Scan(&result.DocID, &result.Score); err != nil {
			return nil, fmt.Errorf("failed to scan BM25 search result: %w", err)
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating BM25 search results: %w", err)
	}

	if s.debug {
		log.Printf("BM25 search returned %d results for query: %s", len(results), query)
	}

	return results, nil
}

// BM25Score calculates the BM25 score for a specific document and query
func (s *FacetingZigSearch) BM25Score(ctx context.Context, query string, docID int64, language string, k1 float64, b float64) (float64, error) {
	if language == "" {
		language = "english"
	}
	if k1 == 0 {
		k1 = 1.2
	}
	if b == 0 {
		b = 0.75
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	sqlQuery := `SELECT facets.bm25_score($1::regclass, $2, $3, $4, $5, $6)`

	var score float64
	err := s.pool.QueryRow(ctx, sqlQuery, tableName, query, docID, language, k1, b).Scan(&score)
	if err != nil {
		return 0, fmt.Errorf("bm25_score failed: %w", err)
	}

	return score, nil
}

// RecalculateStatistics recalculates BM25 collection statistics for the table
// This should be called after batch indexing operations for accurate scoring
func (s *FacetingZigSearch) RecalculateStatistics(ctx context.Context) error {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT facets.bm25_recalculate_statistics($1::regclass)`

	_, err := s.pool.Exec(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("bm25_recalculate_statistics failed: %w", err)
	}

	if s.debug {
		log.Printf("Recalculated BM25 statistics for %s", tableName)
	}

	return nil
}

// GetStatistics retrieves BM25 collection statistics for the table
func (s *FacetingZigSearch) GetStatistics(ctx context.Context) (*BM25Statistics, error) {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT * FROM facets.bm25_get_statistics($1::regclass)`

	var stats BM25Statistics
	err := s.pool.QueryRow(ctx, query, tableName).Scan(&stats.TotalDocs, &stats.AvgLength)
	if err != nil {
		return nil, fmt.Errorf("bm25_get_statistics failed: %w", err)
	}

	return &stats, nil
}

// BM25Document represents a document for batch indexing
type BM25Document struct {
	DocID   int64  `json:"doc_id"`
	Content string `json:"content"`
}

// BM25GetMatchesBitmap returns a roaring bitmap of documents matching the query
// This is useful for combining BM25 results with facet filters at the bitmap level
func (s *FacetingZigSearch) BM25GetMatchesBitmap(ctx context.Context, query string, options BM25SearchOptions) ([]byte, error) {
	if options.Language == "" {
		options.Language = "english"
	}
	if options.FuzzyThreshold == 0 {
		options.FuzzyThreshold = 0.3
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	sqlQuery := `
		SELECT facets.bm25_get_matches_bitmap_native(
			$1::regclass::oid,
			$2,
			$3,
			$4,
			$5,
			$6
		)::bytea
	`

	var bitmap []byte
	err := s.pool.QueryRow(ctx, sqlQuery,
		tableName,
		query,
		options.Language,
		options.PrefixMatch,
		options.FuzzyMatch,
		options.FuzzyThreshold,
	).Scan(&bitmap)
	if err != nil {
		return nil, fmt.Errorf("bm25_get_matches_bitmap_native failed: %w", err)
	}

	if s.debug {
		log.Printf("BM25GetMatchesBitmap returned bitmap for query: %s", query)
	}

	return bitmap, nil
}

// IndexDocumentsBatch indexes multiple documents in a single transaction
// This is more efficient than calling IndexDocument for each document individually
// Returns the number of indexed documents and elapsed time in milliseconds
func (s *FacetingZigSearch) IndexDocumentsBatch(ctx context.Context, documents []BM25Document, language string, batchSize int) (int, float64, error) {
	if language == "" {
		language = "english"
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Convert documents to JSONB array
	docsJSON, err := json.Marshal(documents)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal documents: %w", err)
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `
		SELECT indexed_count, elapsed_ms 
		FROM facets.bm25_index_documents_batch(
			$1::regclass,
			$2::jsonb,
			'content',
			$3,
			$4
		)
	`

	var indexedCount int
	var elapsedMs float64
	err = s.pool.QueryRow(ctx, query, tableName, string(docsJSON), language, batchSize).Scan(&indexedCount, &elapsedMs)
	if err != nil {
		return 0, 0, fmt.Errorf("bm25_index_documents_batch failed: %w", err)
	}

	if s.debug {
		log.Printf("Indexed %d documents in %.2fms", indexedCount, elapsedMs)
	}

	return indexedCount, elapsedMs, nil
}

// IndexDocumentsParallel indexes documents using parallel workers
// This is the most efficient method for large datasets
// Requires dblink extension to be installed
// Returns results per worker
func (s *FacetingZigSearch) IndexDocumentsParallel(ctx context.Context, sourceQuery string, language string, numWorkers int, connectionString string) ([]ParallelIndexResult, error) {
	if language == "" {
		language = "english"
	}
	if numWorkers <= 0 {
		numWorkers = 4
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `
		SELECT worker_id, docs_indexed, elapsed_ms, status
		FROM facets.bm25_index_documents_parallel(
			$1::regclass,
			$2,
			'content',
			$3,
			$4,
			$5
		)
	`

	var connStr interface{} = nil
	if connectionString != "" {
		connStr = connectionString
	}

	rows, err := s.pool.Query(ctx, query, tableName, sourceQuery, language, numWorkers, connStr)
	if err != nil {
		return nil, fmt.Errorf("bm25_index_documents_parallel failed: %w", err)
	}
	defer rows.Close()

	var results []ParallelIndexResult
	for rows.Next() {
		var r ParallelIndexResult
		if err := rows.Scan(&r.WorkerID, &r.DocsIndexed, &r.ElapsedMs, &r.Status); err != nil {
			return nil, fmt.Errorf("failed to scan parallel index result: %w", err)
		}
		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating parallel index results: %w", err)
	}

	if s.debug {
		totalDocs := 0
		for _, r := range results {
			totalDocs += r.DocsIndexed
		}
		log.Printf("Parallel indexing completed: %d total documents across %d workers", totalDocs, len(results))
	}

	return results, nil
}

// ParallelIndexResult represents the result from a parallel indexing worker
type ParallelIndexResult struct {
	WorkerID    int     `json:"worker_id"`
	DocsIndexed int     `json:"docs_indexed"`
	ElapsedMs   float64 `json:"elapsed_ms"`
	Status      string  `json:"status"`
}

// ============================================================================
// NEW IN 0.4.2: BM25 Helper Functions
// ============================================================================

// BM25CreateSyncTrigger creates a trigger to keep BM25 index in sync with table changes
// This trigger handles INSERT, UPDATE, and DELETE operations automatically
func (s *FacetingZigSearch) BM25CreateSyncTrigger(ctx context.Context, idColumn, contentColumn, language string) error {
	if idColumn == "" {
		idColumn = "id"
	}
	if contentColumn == "" {
		contentColumn = "content"
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT facets.bm25_create_sync_trigger($1::regclass, $2, $3, $4)`

	var langArg interface{} = nil
	if language != "" {
		langArg = language
	}

	_, err := s.pool.Exec(ctx, query, tableName, idColumn, contentColumn, langArg)
	if err != nil {
		return fmt.Errorf("bm25_create_sync_trigger failed: %w", err)
	}

	if s.debug {
		log.Printf("Created BM25 sync trigger on %s", tableName)
	}

	return nil
}

// BM25DropSyncTrigger drops the BM25 sync trigger from the table
func (s *FacetingZigSearch) BM25DropSyncTrigger(ctx context.Context) error {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT facets.bm25_drop_sync_trigger($1::regclass)`

	_, err := s.pool.Exec(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("bm25_drop_sync_trigger failed: %w", err)
	}

	if s.debug {
		log.Printf("Dropped BM25 sync trigger from %s", tableName)
	}

	return nil
}

// BM25RebuildOptions configures the BM25 rebuild operation
type BM25RebuildOptions struct {
	IDColumn         string // Column name for document ID (default: "id")
	ContentColumn    string // Column name for content (default: "content")
	Language         string // Text search language (default: "english")
	NumWorkers       int    // Number of parallel workers (0 = auto)
	ConnectionString string // Connection string for parallel mode
	ProgressStepSize int    // Progress reporting frequency (default: 50000)
}

// BM25RebuildIndex rebuilds the BM25 index for the table
// This clears existing index data and re-indexes all documents
func (s *FacetingZigSearch) BM25RebuildIndex(ctx context.Context, options BM25RebuildOptions) error {
	if options.IDColumn == "" {
		options.IDColumn = "id"
	}
	if options.ContentColumn == "" {
		options.ContentColumn = "content"
	}
	if options.Language == "" {
		options.Language = "english"
	}
	if options.ProgressStepSize <= 0 {
		options.ProgressStepSize = 50000
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT facets.bm25_rebuild_index($1::regclass, $2, $3, $4, $5, $6, $7)`

	var connStr interface{} = nil
	if options.ConnectionString != "" {
		connStr = options.ConnectionString
	}

	_, err := s.pool.Exec(ctx, query,
		tableName,
		options.IDColumn,
		options.ContentColumn,
		options.Language,
		options.NumWorkers,
		connStr,
		options.ProgressStepSize,
	)
	if err != nil {
		return fmt.Errorf("bm25_rebuild_index failed: %w", err)
	}

	if s.debug {
		log.Printf("BM25 index rebuilt for %s", tableName)
	}

	return nil
}

// BM25StatusResult represents BM25 index status for a table
type BM25StatusResult struct {
	TableName        string    `json:"table_name"`
	DocumentsIndexed int64     `json:"documents_indexed"`
	UniqueTerms      int64     `json:"unique_terms"`
	TotalDocuments   int64     `json:"total_documents"`
	AvgDocLength     float64   `json:"avg_doc_length"`
	LastUpdated      time.Time `json:"last_updated"`
}

// BM25Status returns the status of all BM25 indexes
func (s *FacetingZigSearch) BM25Status(ctx context.Context) ([]BM25StatusResult, error) {
	query := `SELECT table_name, documents_indexed, unique_terms, total_documents, avg_doc_length, last_updated FROM facets.bm25_status()`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("bm25_status failed: %w", err)
	}
	defer rows.Close()

	var results []BM25StatusResult
	for rows.Next() {
		var r BM25StatusResult
		var lastUpdated *time.Time
		if err := rows.Scan(&r.TableName, &r.DocumentsIndexed, &r.UniqueTerms, &r.TotalDocuments, &r.AvgDocLength, &lastUpdated); err != nil {
			return nil, fmt.Errorf("failed to scan BM25 status: %w", err)
		}
		if lastUpdated != nil {
			r.LastUpdated = *lastUpdated
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// BM25ProgressResult represents indexing progress for a table
type BM25ProgressResult struct {
	TableName        string   `json:"table_name"`
	DocumentsIndexed int64    `json:"documents_indexed"`
	SourceDocuments  *int64   `json:"source_documents"`
	ProgressPct      *float64 `json:"progress_pct"`
	UniqueTerms      int64    `json:"unique_terms"`
}

// BM25Progress returns the indexing progress for the configured table
func (s *FacetingZigSearch) BM25Progress(ctx context.Context) (*BM25ProgressResult, error) {
	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)
	query := `SELECT table_name, documents_indexed, source_documents, progress_pct, unique_terms FROM facets.bm25_progress($1::regclass)`

	var r BM25ProgressResult
	err := s.pool.QueryRow(ctx, query, tableName).Scan(&r.TableName, &r.DocumentsIndexed, &r.SourceDocuments, &r.ProgressPct, &r.UniqueTerms)
	if err != nil {
		return nil, fmt.Errorf("bm25_progress failed: %w", err)
	}

	return &r, nil
}

// BM25ActiveProcessResult represents an active BM25 process
type BM25ActiveProcessResult struct {
	PID           *int    `json:"pid"`
	State         string  `json:"state"`
	Duration      *string `json:"duration"` // interval as string
	WaitEvent     string  `json:"wait_event"`
	OperationType string  `json:"operation_type"`
	QueryPreview  string  `json:"query_preview"`
}

// BM25ActiveProcesses returns currently running BM25-related processes
func (s *FacetingZigSearch) BM25ActiveProcesses(ctx context.Context) ([]BM25ActiveProcessResult, error) {
	query := `SELECT pid, state, duration::text, wait_event, operation_type, query_preview FROM facets.bm25_active_processes()`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("bm25_active_processes failed: %w", err)
	}
	defer rows.Close()

	var results []BM25ActiveProcessResult
	for rows.Next() {
		var r BM25ActiveProcessResult
		if err := rows.Scan(&r.PID, &r.State, &r.Duration, &r.WaitEvent, &r.OperationType, &r.QueryPreview); err != nil {
			return nil, fmt.Errorf("failed to scan active process: %w", err)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// BM25CleanupResult represents the result of a cleanup operation
type BM25CleanupResult struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// BM25CleanupDblinks disconnects all dblink connections
func (s *FacetingZigSearch) BM25CleanupDblinks(ctx context.Context) ([]BM25CleanupResult, error) {
	query := `SELECT connection_name, status FROM facets.bm25_cleanup_dblinks()`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("bm25_cleanup_dblinks failed: %w", err)
	}
	defer rows.Close()

	var results []BM25CleanupResult
	for rows.Next() {
		var r BM25CleanupResult
		var name *string
		if err := rows.Scan(&name, &r.Status); err != nil {
			return nil, fmt.Errorf("failed to scan cleanup result: %w", err)
		}
		if name != nil {
			r.Name = *name
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// BM25CleanupStaging drops orphaned staging tables
func (s *FacetingZigSearch) BM25CleanupStaging(ctx context.Context) ([]BM25CleanupResult, error) {
	query := `SELECT table_name, status FROM facets.bm25_cleanup_staging()`

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("bm25_cleanup_staging failed: %w", err)
	}
	defer rows.Close()

	var results []BM25CleanupResult
	for rows.Next() {
		var r BM25CleanupResult
		var name *string
		if err := rows.Scan(&name, &r.Status); err != nil {
			return nil, fmt.Errorf("failed to scan cleanup result: %w", err)
		}
		if name != nil {
			r.Name = *name
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// BM25KillStuckResult represents a killed stuck process
type BM25KillStuckResult struct {
	PID      *int    `json:"pid"`
	Duration *string `json:"duration"`
	Status   string  `json:"status"`
}

// BM25KillStuck terminates stuck BM25 processes older than the specified duration
func (s *FacetingZigSearch) BM25KillStuck(ctx context.Context, minDuration string) ([]BM25KillStuckResult, error) {
	if minDuration == "" {
		minDuration = "5 minutes"
	}

	query := `SELECT pid, duration::text, status FROM facets.bm25_kill_stuck($1::interval)`

	rows, err := s.pool.Query(ctx, query, minDuration)
	if err != nil {
		return nil, fmt.Errorf("bm25_kill_stuck failed: %w", err)
	}
	defer rows.Close()

	var results []BM25KillStuckResult
	for rows.Next() {
		var r BM25KillStuckResult
		if err := rows.Scan(&r.PID, &r.Duration, &r.Status); err != nil {
			return nil, fmt.Errorf("failed to scan kill result: %w", err)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// BM25FullCleanupResult represents the result of a full cleanup operation
type BM25FullCleanupResult struct {
	Operation string `json:"operation"`
	Details   string `json:"details"`
}

// BM25FullCleanup performs complete cleanup: dblinks, staging tables, and stuck processes
func (s *FacetingZigSearch) BM25FullCleanup(ctx context.Context, killThreshold string) ([]BM25FullCleanupResult, error) {
	if killThreshold == "" {
		killThreshold = "5 minutes"
	}

	query := `SELECT operation, details FROM facets.bm25_full_cleanup($1::interval)`

	rows, err := s.pool.Query(ctx, query, killThreshold)
	if err != nil {
		return nil, fmt.Errorf("bm25_full_cleanup failed: %w", err)
	}
	defer rows.Close()

	var results []BM25FullCleanupResult
	for rows.Next() {
		var r BM25FullCleanupResult
		if err := rows.Scan(&r.Operation, &r.Details); err != nil {
			return nil, fmt.Errorf("failed to scan cleanup result: %w", err)
		}
		results = append(results, r)
	}

	return results, rows.Err()
}

// SetupTableWithBM25Options configures the one-stop table setup
type SetupTableWithBM25Options struct {
	IDColumn        string   // Column name for document ID (default: "id")
	ContentColumn   string   // Column name for content (default: "content")
	FacetDefinitions []string // SQL facet definitions (e.g., "facets.plain_facet('category')")
	Language        string   // Text search language (default: "english")
	CreateTrigger   bool     // Whether to create sync trigger (default: true)
	ChunkBits       *int     // Chunk bits for faceting (nil = auto)
	PopulateFacets  bool     // Whether to populate facets (default: true)
	BuildBM25Index  bool     // Whether to build BM25 index (default: true)
	BM25Workers     int      // Number of workers for BM25 indexing (0 = auto)
}

// SetupTableWithBM25 performs one-stop setup for facets + BM25 indexing
func (s *FacetingZigSearch) SetupTableWithBM25(ctx context.Context, options SetupTableWithBM25Options) error {
	if options.IDColumn == "" {
		options.IDColumn = "id"
	}
	if options.ContentColumn == "" {
		options.ContentColumn = "content"
	}
	if options.Language == "" {
		options.Language = "english"
	}

	tableName := fmt.Sprintf("%s.%s", s.config.SchemaName, s.config.DocumentTable)

	// Build facets array SQL
	var facetsSQL string
	if len(options.FacetDefinitions) > 0 {
		facetsSQL = fmt.Sprintf("ARRAY[%s]", strings.Join(options.FacetDefinitions, ", "))
	} else {
		facetsSQL = "NULL"
	}

	// Build chunk_bits parameter
	var chunkBitsSQL string
	if options.ChunkBits != nil {
		chunkBitsSQL = fmt.Sprintf("%d", *options.ChunkBits)
	} else {
		chunkBitsSQL = "NULL"
	}

	query := fmt.Sprintf(`
		SELECT facets.setup_table_with_bm25(
			$1::regclass,
			$2,  -- id_column
			$3,  -- content_column
			%s,  -- facets array
			$4,  -- language
			$5,  -- create_trigger
			%s,  -- chunk_bits
			$6,  -- populate_facets
			$7,  -- build_bm25_index
			$8   -- bm25_workers
		)
	`, facetsSQL, chunkBitsSQL)

	_, err := s.pool.Exec(ctx, query,
		tableName,
		options.IDColumn,
		options.ContentColumn,
		options.Language,
		options.CreateTrigger,
		options.PopulateFacets,
		options.BuildBM25Index,
		options.BM25Workers,
	)
	if err != nil {
		return fmt.Errorf("setup_table_with_bm25 failed: %w", err)
	}

	if s.debug {
		log.Printf("Setup complete for %s with BM25", tableName)
	}

	return nil
}

// Helper function to return nil for empty strings
func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
