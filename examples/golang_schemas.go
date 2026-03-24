// golang_schemas.go - Go structs for pg_facets JSON responses
// Use these structs to unmarshal JSON responses from pg_facets functions

package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// FacetCount represents individual facet count from facets.get_facet_counts()
type FacetCount struct {
	FacetName   string `json:"facet_name"`
	FacetValue  string `json:"facet_value"`
	Cardinality int64  `json:"cardinality"`
	FacetID     int    `json:"facet_id"`
}

// HierarchicalFacet represents a facet with children in hierarchical responses
type HierarchicalFacet struct {
	Count    int64                    `json:"count"`
	Children map[string]HierarchicalFacet `json:"children,omitempty"`
}

// FacetsResponse represents the complete facets response from hierarchical_facets functions
type FacetsResponse struct {
	RegularFacets     map[string][]FacetCount        `json:"regular_facets"`
	HierarchicalFacets map[string]map[string]HierarchicalFacet `json:"hierarchical_facets,omitempty"`
}

// SearchResult represents a single search result document
type SearchResult struct {
	DocumentID int                    `json:"document_id"`
	Score      float64                `json:"score"`
	Content    string                 `json:"content,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt  time.Time              `json:"created_at,omitempty"`
	UpdatedAt  time.Time              `json:"updated_at,omitempty"`
}

// BM25Result represents BM25 search result
type BM25Result struct {
	DocID int64   `json:"doc_id"`
	Score float64 `json:"score"`
}

// SearchWithFacetsResponse represents complete search response
type SearchWithFacetsResponse struct {
	Results    []SearchResult `json:"results"`
	Facets     FacetsResponse `json:"facets"`
	TotalFound int64          `json:"total_found"`
	SearchTime int            `json:"search_time"`
}

// BM25SearchResponse represents BM25 search response (array of results)
type BM25SearchResponse []BM25Result

// Example usage functions
func parseFacetsResponse(jsonStr string) (*FacetsResponse, error) {
	var resp FacetsResponse
	err := json.Unmarshal([]byte(jsonStr), &resp)
	return &resp, err
}

func parseSearchResponse(jsonStr string) (*SearchWithFacetsResponse, error) {
	var resp SearchWithFacetsResponse
	err := json.Unmarshal([]byte(jsonStr), &resp)
	return &resp, err
}

func parseBM25Response(jsonStr string) (BM25SearchResponse, error) {
	var resp BM25SearchResponse
	err := json.Unmarshal([]byte(jsonStr), &resp)
	return resp, err
}

// Example: Process facets for frontend
func processFacetsForFrontend(facets *FacetsResponse) map[string][]map[string]interface{} {
	result := make(map[string][]map[string]interface{})

	for facetName, counts := range facets.RegularFacets {
		var facetOptions []map[string]interface{}
		for _, count := range counts {
			facetOptions = append(facetOptions, map[string]interface{}{
				"value":       count.FacetValue,
				"label":       count.FacetValue,
				"count":       count.Cardinality,
				"selected":    false, // Frontend state
			})
		}
		result[facetName] = facetOptions
	}

	return result
}

// Example: Process search results
func processSearchResults(searchResp *SearchWithFacetsResponse) {
	fmt.Printf("Found %d results in %dms\n", searchResp.TotalFound, searchResp.SearchTime)

	for _, result := range searchResp.Results {
		fmt.Printf("Doc %d: Score %.3f - %s\n",
			result.DocumentID, result.Score, result.Content)
	}

	// Process facets for UI
	facetOptions := processFacetsForFrontend(&searchResp.Facets)
	for facetName, options := range facetOptions {
		fmt.Printf("Facet %s has %d options\n", facetName, len(options))
	}
}

func main() {
	// Example JSON responses (from your SQL queries)

	// 1. Facets-only response
	facetsJSON := `{
		"regular_facets": {
			"category": [
				{"facet_name": "category", "facet_value": "electronics", "cardinality": 1250, "facet_id": 1},
				{"facet_name": "category", "facet_value": "books", "cardinality": 890, "facet_id": 1}
			]
		}
	}`

	// 2. Search with facets response
	searchJSON := `{
		"results": [
			{
				"document_id": 12345,
				"score": 0.85,
				"content": "High-performance laptop...",
				"metadata": {"title": "Gaming Laptop Pro", "category": "electronics"},
				"created_at": "2024-01-15T10:30:00Z",
				"updated_at": "2024-01-15T10:30:00Z"
			}
		],
		"facets": {
			"regular_facets": {
				"category": [
					{"facet_name": "category", "facet_value": "electronics", "cardinality": 1250, "facet_id": 1}
				]
			}
		},
		"total_found": 2500,
		"search_time": 45
	}`

	// 3. BM25 search response
	bm25JSON := `[
		{"doc_id": 12345, "score": 1.234},
		{"doc_id": 67890, "score": 0.987}
	]`

	// Parse and process
	facets, _ := parseFacetsResponse(facetsJSON)
	search, _ := parseSearchResponse(searchJSON)
	bm25, _ := parseBM25Response(bm25JSON)

	fmt.Println("=== Facets Response ===")
	fmt.Printf("Categories: %d options\n", len(facets.RegularFacets["category"]))

	fmt.Println("\n=== Search Response ===")
	processSearchResults(search)

	fmt.Println("\n=== BM25 Response ===")
	fmt.Printf("BM25 results: %d documents\n", len(bm25))
	for _, result := range bm25 {
		fmt.Printf("Doc %d: Score %.3f\n", result.DocID, result.Score)
	}
}

/*
Expected output:

=== Facets Response ===
Categories: 2 options

=== Search Response ===
Found 2500 results in 45ms
Doc 12345: Score 0.850 - High-performance laptop...
Facet category has 1 options

=== BM25 Response ===
BM25 results: 2 documents
Doc 12345: Score 1.234
Doc 67890: Score 0.987
*/
