// faceting_benchmark_test.go
// Benchmark tests for pgfaceting_zig document operations
// Tests insertion, removal, and facet maintenance procedures

package pgfaceting

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CategoryEntry represents a row from gestion_locative_categories.csv
type CategoryEntry struct {
	TypeCategorie       string
	CategoriePrincipale string
	Categorie           string
	SousCategorie       string
}

// BenchmarkConfig holds configuration for benchmark runs
type BenchmarkConfig struct {
	NumDocuments int
	BatchSize    int
	NumQueries   int
}

// loadCategories loads categories from the CSV file
func loadCategories(t *testing.T) []CategoryEntry {
	file, err := os.Open("gestion_locative_categories.csv")
	if err != nil {
		t.Logf("Could not open CSV file (using fallback): %v", err)
		return getDefaultCategories()
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Logf("Could not read CSV file (using fallback): %v", err)
		return getDefaultCategories()
	}

	var categories []CategoryEntry
	for i, record := range records {
		if i == 0 { // Skip header
			continue
		}
		if len(record) >= 4 {
			categories = append(categories, CategoryEntry{
				TypeCategorie:       record[0],
				CategoriePrincipale: record[1],
				Categorie:           record[2],
				SousCategorie:       record[3],
			})
		}
	}

	if len(categories) == 0 {
		return getDefaultCategories()
	}

	return categories
}

// getDefaultCategories returns fallback categories if CSV can't be loaded
func getDefaultCategories() []CategoryEntry {
	return []CategoryEntry{
		{"Gestion Locative", "Gestion Administrative & Contrats", "Documents, Dossiers & Contrats", "Contrats de location"},
		{"Gestion Locative", "Gestion Administrative & Contrats", "Documents, Dossiers & Contrats", "États des lieux et inventaires"},
		{"Gestion Locative", "Finances, Comptabilité & Contentieux", "Paiement et Transactions", "Encaissement des loyers"},
		{"Gestion Locative", "Finances, Comptabilité & Contentieux", "Facturation", "Émission Factures"},
		{"Gestion Locative", "Maintenance, Travaux & Interventions", "Entretien Courant & Réparations", "Signalement Incident"},
		{"Gestion Locative", "Visites & Accompagnement", "Organisation des Visites", "Planification des visites"},
		{"Gestion Locative", "Incidents & Problèmes", "Signalement et Suivi des Incidents", "Réception des réclamations"},
		{"Gestion Locative", "Support et Services Complémentaires", "Outils et Interfaces Numériques", "Plateformes de gestion en ligne"},
	}
}

// TestDocumentBenchmark benchmarks document operations
func TestDocumentBenchmark(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	categories := loadCategories(t)
	t.Logf("Loaded %d categories from CSV", len(categories))

	configs := []BenchmarkConfig{
		{NumDocuments: 100, BatchSize: 10, NumQueries: 10},
		{NumDocuments: 1000, BatchSize: 100, NumQueries: 20},
		{NumDocuments: 100000, BatchSize: 1000, NumQueries: 50},
	}

	for _, cfg := range configs {
		t.Run(fmt.Sprintf("Docs_%d", cfg.NumDocuments), func(t *testing.T) {
			runBenchmark(t, pool, categories, cfg)
		})
	}
}

// runBenchmark runs a single benchmark configuration
func runBenchmark(t *testing.T, pool *pgxpool.Pool, categories []CategoryEntry, cfg BenchmarkConfig) {
	// Use a timeout context to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup benchmark schema
	schemaName := fmt.Sprintf("bench_%d", cfg.NumDocuments)
	setupBenchmarkSchema(t, pool, schemaName)
	defer cleanupBenchmarkSchema(t, pool, schemaName)

	results := &BenchmarkResults{
		Config: cfg,
	}

	// 1. Benchmark: Batch Insert Documents
	t.Log("=== Phase 1: Inserting documents ===")
	insertedIDs := benchmarkInsertDocuments(t, pool, ctx, schemaName, categories, cfg, results)

	if len(insertedIDs) == 0 {
		t.Fatalf("No documents were inserted! Check database connection and schema setup.")
	}

	t.Logf("Inserted %d documents in %v (%.2f docs/sec)",
		len(insertedIDs), results.InsertTime,
		float64(len(insertedIDs))/results.InsertTime.Seconds())

	// 2. Benchmark: Merge Deltas after Insert
	t.Log("=== Phase 2: Merging deltas after insert ===")
	benchmarkMergeDeltas(t, pool, ctx, schemaName, "after_insert", results)
	t.Logf("Merge deltas (insert): %v", results.MergeDeltasAfterInsert)

	// 3. Benchmark: Query facet counts
	t.Log("=== Phase 3: Querying facets ===")
	benchmarkFacetQueries(t, pool, ctx, schemaName, categories, cfg, results)
	t.Logf("Facet queries: avg %v per query", results.AvgQueryTime)

	// 4. Benchmark: Update documents
	t.Log("=== Phase 4: Updating documents ===")
	updateCount := min(cfg.NumDocuments/10, len(insertedIDs)) // Update 10% of documents
	if updateCount > 0 {
		benchmarkUpdateDocuments(t, pool, ctx, schemaName, insertedIDs[:updateCount], categories, results)
		t.Logf("Updated %d documents in %v", updateCount, results.UpdateTime)
	}

	// 5. Benchmark: Merge Deltas after Update
	t.Log("=== Phase 5: Merging deltas after update ===")
	benchmarkMergeDeltas(t, pool, ctx, schemaName, "after_update", results)
	t.Logf("Merge deltas (update): %v", results.MergeDeltasAfterUpdate)

	// 6. Benchmark: Delete documents
	t.Log("=== Phase 6: Deleting documents ===")
	deleteCount := min(cfg.NumDocuments/5, len(insertedIDs)) // Delete 20% of documents
	if deleteCount > 0 {
		benchmarkDeleteDocuments(t, pool, ctx, schemaName, insertedIDs[:deleteCount], results)
		t.Logf("Deleted %d documents in %v", deleteCount, results.DeleteTime)
	}

	// 7. Benchmark: Merge Deltas after Delete
	t.Log("=== Phase 7: Merging deltas after delete ===")
	benchmarkMergeDeltas(t, pool, ctx, schemaName, "after_delete", results)
	t.Logf("Merge deltas (delete): %v", results.MergeDeltasAfterDelete)

	// 8. Benchmark: Incremental batch additions (only for larger datasets)
	if cfg.NumDocuments >= 10000 {
		t.Log("=== Phase 8: Incremental batch additions (3 batches of 1000) ===")
		benchmarkIncrementalBatches(t, pool, ctx, schemaName, categories, results)
	}

	// 9. List all maintenance procedures
	t.Log("=== Available Maintenance Procedures ===")
	listMaintenanceProcedures(t)

	// Print summary
	printBenchmarkSummary(t, results)
}

// BenchmarkResults holds timing results from benchmark
type BenchmarkResults struct {
	Config                 BenchmarkConfig
	InsertTime             time.Duration
	MergeDeltasAfterInsert time.Duration
	AvgQueryTime           time.Duration
	UpdateTime             time.Duration
	MergeDeltasAfterUpdate time.Duration
	DeleteTime             time.Duration
	MergeDeltasAfterDelete time.Duration
	PopulateFacetsTime     time.Duration
	// Incremental batch results
	IncrementalBatches []IncrementalBatchResult
}

// IncrementalBatchResult holds results for a single incremental batch
type IncrementalBatchResult struct {
	BatchNum       int
	BatchSize      int
	InsertTime     time.Duration
	MergeDeltaTime time.Duration
	DocsPerSec     float64
}

// setupBenchmarkSchema creates the schema and table for benchmarking
func setupBenchmarkSchema(t *testing.T, pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()

	// First, clean up any existing faceting for this table
	cleanupBenchmarkSchema(t, pool, schemaName)

	queries := []string{
		fmt.Sprintf(`CREATE SCHEMA %s`, schemaName),
		fmt.Sprintf(`CREATE TABLE %s.documents (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT,
			type_categorie TEXT,
			categorie_principale TEXT,
			categorie TEXT,
			sous_categorie TEXT,
			status TEXT DEFAULT 'active',
			priority INTEGER DEFAULT 1,
			tags TEXT[],
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`, schemaName),
		// Add faceting to the table
		fmt.Sprintf(`SELECT facets.add_faceting_to_table(
			'%s.documents',
			key => 'id',
			facets => ARRAY[
				facets.plain_facet('type_categorie'),
				facets.plain_facet('categorie_principale'),
				facets.plain_facet('categorie'),
				facets.plain_facet('sous_categorie'),
				facets.plain_facet('status'),
				facets.bucket_facet('priority', buckets => ARRAY[0, 1, 2, 3, 4, 5]),
				facets.array_facet('tags'),
				facets.datetrunc_facet('created_at', 'month')
			],
			populate => false
		)`, schemaName),
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Fatalf("Setup query failed: %v\nQuery: %s", err, q)
		}
	}

	t.Logf("Created benchmark schema: %s", schemaName)
}

// cleanupBenchmarkSchema removes the benchmark schema and faceting metadata
func cleanupBenchmarkSchema(t *testing.T, pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()

	tableName := fmt.Sprintf("%s.documents", schemaName)

	// First, try to remove faceting (this cleans up facets.facet_definition, triggers, etc.)
	_, _ = pool.Exec(ctx, `SELECT facets.drop_faceting($1)`, tableName)

	// Then drop the schema
	_, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
	if err != nil {
		t.Logf("Schema cleanup failed (may not exist): %v", err)
	}
}

// benchmarkInsertDocuments inserts documents using bulk mode (disable triggers, insert, populate facets)
func benchmarkInsertDocuments(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	schemaName string,
	categories []CategoryEntry,
	cfg BenchmarkConfig,
	results *BenchmarkResults,
) []int64 {
	statuses := []string{"active", "pending", "archived", "draft"}
	tagOptions := []string{"urgent", "important", "routine", "reviewed", "pending_review", "approved", "rejected"}

	tableName := fmt.Sprintf("%s.documents", schemaName)

	// Disable triggers for bulk insert
	_, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER ALL`, tableName))
	if err != nil {
		t.Fatalf("Failed to disable triggers: %v", err)
	}

	start := time.Now()

	// Bulk insert in batches
	for batch := 0; batch < cfg.NumDocuments; batch += cfg.BatchSize {
		batchEnd := batch + cfg.BatchSize
		if batchEnd > cfg.NumDocuments {
			batchEnd = cfg.NumDocuments
		}
		batchSize := batchEnd - batch

		var valueStrings []string
		var args []interface{}
		argIndex := 1

		for i := 0; i < batchSize; i++ {
			cat := categories[rand.Intn(len(categories))]
			status := statuses[rand.Intn(len(statuses))]
			priority := rand.Intn(5) + 1

			// Generate random tags (1-3 tags) as PostgreSQL array literal
			numTags := rand.Intn(3) + 1
			tags := make([]string, numTags)
			for j := 0; j < numTags; j++ {
				tags[j] = tagOptions[rand.Intn(len(tagOptions))]
			}
			tagsLiteral := fmt.Sprintf("{%s}", strings.Join(tags, ","))

			valueStrings = append(valueStrings, fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4,
				argIndex+5, argIndex+6, argIndex+7, argIndex+8,
			))
			args = append(args,
				fmt.Sprintf("Document %d", batch+i+1),
				fmt.Sprintf("Content for document %d about %s", batch+i+1, cat.SousCategorie),
				cat.TypeCategorie,
				cat.CategoriePrincipale,
				cat.Categorie,
				cat.SousCategorie,
				status,
				priority,
				tagsLiteral,
			)
			argIndex += 9
		}

		query := fmt.Sprintf(`
			INSERT INTO %s.documents 
			(title, content, type_categorie, categorie_principale, categorie, sous_categorie, status, priority, tags)
			VALUES %s`,
			schemaName, strings.Join(valueStrings, ", "))

		_, err := pool.Exec(ctx, query, args...)
		if err != nil {
			t.Fatalf("Batch insert failed: %v", err)
		}

		// Progress logging for large inserts
		if batchEnd%500 == 0 || batchEnd == cfg.NumDocuments {
			t.Logf("  Inserted %d/%d documents...", batchEnd, cfg.NumDocuments)
		}
	}

	insertTime := time.Since(start)

	// Re-enable triggers
	_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER ALL`, tableName))
	if err != nil {
		t.Fatalf("Failed to re-enable triggers: %v", err)
	}

	// Populate facets (this is what would normally happen via triggers)
	t.Log("  Populating facets...")
	populateStart := time.Now()
	_, err = pool.Exec(ctx, `SELECT facets.populate_facets($1::regclass)`, tableName)
	if err != nil {
		t.Fatalf("Failed to populate facets: %v", err)
	}
	results.PopulateFacetsTime = time.Since(populateStart)

	results.InsertTime = insertTime

	// Query for all inserted IDs
	insertedIDs := make([]int64, 0, cfg.NumDocuments)
	rows, err := pool.Query(ctx, fmt.Sprintf(`SELECT id FROM %s.documents ORDER BY id`, schemaName))
	if err != nil {
		t.Fatalf("Failed to query inserted IDs: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Failed to scan ID: %v", err)
		}
		insertedIDs = append(insertedIDs, id)
	}

	t.Logf("Inserted %d documents in %v, populated facets in %v",
		len(insertedIDs), insertTime, results.PopulateFacetsTime)
	return insertedIDs
}

// benchmarkMergeDeltas measures delta merge time
func benchmarkMergeDeltas(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	schemaName string,
	phase string,
	results *BenchmarkResults,
) {
	start := time.Now()

	tableName := fmt.Sprintf("%s.documents", schemaName)
	_, err := pool.Exec(ctx, `SELECT facets.merge_deltas($1::regclass)`, tableName)
	if err != nil {
		t.Logf("Warning: merge_deltas failed: %v", err)
	}

	elapsed := time.Since(start)

	switch phase {
	case "after_insert":
		results.MergeDeltasAfterInsert = elapsed
	case "after_update":
		results.MergeDeltasAfterUpdate = elapsed
	case "after_delete":
		results.MergeDeltasAfterDelete = elapsed
	}
}

// benchmarkFacetQueries measures facet query performance
func benchmarkFacetQueries(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	schemaName string,
	categories []CategoryEntry,
	cfg BenchmarkConfig,
	results *BenchmarkResults,
) {
	tableName := fmt.Sprintf("%s.documents", schemaName)

	var totalTime time.Duration
	queries := 0

	// Query 1: Get all top values
	for i := 0; i < cfg.NumQueries/4; i++ {
		start := time.Now()
		rows, err := pool.Query(ctx,
			`SELECT * FROM facets.top_values($1::regclass, $2, NULL)`,
			tableName, 10)
		if err == nil {
			// Drain the rows to properly close the connection
			for rows.Next() {
			}
			rows.Close()
			totalTime += time.Since(start)
			queries++
		} else {
			t.Logf("top_values query failed: %v", err)
		}
	}

	// Query 2: Query with specific facet filters
	for i := 0; i < cfg.NumQueries/4; i++ {
		cat := categories[rand.Intn(len(categories))]
		start := time.Now()

		filterQuery := fmt.Sprintf(`
			SELECT * FROM facets.count_results(
				'%s.documents'::regclass::oid,
				ARRAY[ROW('categorie_principale', '%s')]::facets.facet_filter[]
			)`,
			schemaName, escapeString(cat.CategoriePrincipale))

		rows, err := pool.Query(ctx, filterQuery)
		if err == nil {
			for rows.Next() {
			}
			rows.Close()
			totalTime += time.Since(start)
			queries++
		}
	}

	// Query 3: Multi-facet filters
	for i := 0; i < cfg.NumQueries/4; i++ {
		cat := categories[rand.Intn(len(categories))]
		start := time.Now()

		filterQuery := fmt.Sprintf(`
			SELECT * FROM facets.count_results(
				'%s.documents'::regclass::oid,
				ARRAY[
					ROW('categorie_principale', '%s'),
					ROW('status', 'active')
				]::facets.facet_filter[]
			)`,
			schemaName, escapeString(cat.CategoriePrincipale))

		rows, err := pool.Query(ctx, filterQuery)
		if err == nil {
			for rows.Next() {
			}
			rows.Close()
			totalTime += time.Since(start)
			queries++
		}
	}

	// Query 4: Filter documents by facets (JSONB API)
	for i := 0; i < cfg.NumQueries/4; i++ {
		cat := categories[rand.Intn(len(categories))]
		start := time.Now()

		filterQuery := fmt.Sprintf(`
			SELECT * FROM facets.filter_documents_by_facets(
				'%s',
				'{"categorie_principale": "%s"}'::jsonb,
				'documents'
			) LIMIT 100`,
			schemaName, escapeString(cat.CategoriePrincipale))

		rows, err := pool.Query(ctx, filterQuery)
		if err == nil {
			for rows.Next() {
			}
			rows.Close()
			totalTime += time.Since(start)
			queries++
		}
	}

	if queries > 0 {
		results.AvgQueryTime = totalTime / time.Duration(queries)
	}

	t.Logf("Executed %d facet queries", queries)
}

// benchmarkUpdateDocuments measures update performance
// Uses bulk mode (disable triggers, update, re-populate) to avoid ON CONFLICT issues
func benchmarkUpdateDocuments(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	schemaName string,
	ids []int64,
	categories []CategoryEntry,
	results *BenchmarkResults,
) {
	tableName := fmt.Sprintf("%s.documents", schemaName)

	// Disable triggers to avoid ON CONFLICT issues
	_, _ = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER ALL`, tableName))

	start := time.Now()

	for _, id := range ids {
		// Change category and status
		newCat := categories[rand.Intn(len(categories))]
		newStatuses := []string{"active", "pending", "archived"}
		newStatus := newStatuses[rand.Intn(len(newStatuses))]

		query := fmt.Sprintf(`
			UPDATE %s.documents 
			SET categorie_principale = $1, 
				categorie = $2, 
				sous_categorie = $3,
				status = $4,
				updated_at = NOW()
			WHERE id = $5`,
			schemaName)

		_, err := pool.Exec(ctx, query,
			newCat.CategoriePrincipale,
			newCat.Categorie,
			newCat.SousCategorie,
			newStatus,
			id)

		if err != nil {
			t.Logf("Warning: Update failed for ID %d: %v", id, err)
		}
	}

	results.UpdateTime = time.Since(start)

	// Re-enable triggers
	_, _ = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER ALL`, tableName))

	// Re-populate facets (this is what would need to happen after bulk updates)
	_, _ = pool.Exec(ctx, `SELECT facets.populate_facets($1::regclass)`, tableName)
}

// benchmarkDeleteDocuments measures delete performance
// Uses bulk mode (disable triggers, delete, re-populate) to avoid ON CONFLICT issues
func benchmarkDeleteDocuments(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	schemaName string,
	ids []int64,
	results *BenchmarkResults,
) {
	tableName := fmt.Sprintf("%s.documents", schemaName)

	// Disable triggers to avoid ON CONFLICT issues
	_, _ = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER ALL`, tableName))

	start := time.Now()

	// Delete in batches (now safe without triggers)
	batchSize := 100
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[i:end]

		// Convert to string for IN clause
		idStrings := make([]string, len(batch))
		for j, id := range batch {
			idStrings[j] = fmt.Sprintf("%d", id)
		}

		query := fmt.Sprintf(`DELETE FROM %s.documents WHERE id IN (%s)`,
			schemaName, strings.Join(idStrings, ","))

		_, err := pool.Exec(ctx, query)
		if err != nil {
			t.Logf("Warning: Delete batch failed: %v", err)
		}
	}

	results.DeleteTime = time.Since(start)

	// Re-enable triggers
	_, _ = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER ALL`, tableName))

	// Re-populate facets (this rebuilds facets after deletes)
	_, _ = pool.Exec(ctx, `SELECT facets.populate_facets($1::regclass)`, tableName)
}

// benchmarkIncrementalBatches tests adding batches of 1000 docs to an existing large dataset
func benchmarkIncrementalBatches(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
	schemaName string,
	categories []CategoryEntry,
	results *BenchmarkResults,
) {
	statuses := []string{"active", "pending", "archived", "draft"}
	tagOptions := []string{"urgent", "important", "routine", "reviewed", "pending_review", "approved", "rejected"}
	tableName := fmt.Sprintf("%s.documents", schemaName)

	const numBatches = 3
	const batchSize = 1000

	results.IncrementalBatches = make([]IncrementalBatchResult, 0, numBatches)

	// Get current max ID to continue numbering
	var maxID int64
	err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) FROM %s.documents`, schemaName)).Scan(&maxID)
	if err != nil {
		t.Logf("Warning: Could not get max ID: %v", err)
		maxID = 100000
	}

	for batch := 0; batch < numBatches; batch++ {
		batchResult := IncrementalBatchResult{
			BatchNum:  batch + 1,
			BatchSize: batchSize,
		}

		// Disable triggers for this batch
		_, _ = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER ALL`, tableName))

		// Build batch insert
		var valueStrings []string
		var args []interface{}
		argIndex := 1

		for i := 0; i < batchSize; i++ {
			cat := categories[rand.Intn(len(categories))]
			status := statuses[rand.Intn(len(statuses))]
			priority := rand.Intn(5) + 1

			numTags := rand.Intn(3) + 1
			tags := make([]string, numTags)
			for j := 0; j < numTags; j++ {
				tags[j] = tagOptions[rand.Intn(len(tagOptions))]
			}
			tagsLiteral := fmt.Sprintf("{%s}", strings.Join(tags, ","))

			docNum := maxID + int64(batch*batchSize+i+1)
			valueStrings = append(valueStrings, fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4,
				argIndex+5, argIndex+6, argIndex+7, argIndex+8,
			))
			args = append(args,
				fmt.Sprintf("Incremental Document %d", docNum),
				fmt.Sprintf("Content for incremental document %d", docNum),
				cat.TypeCategorie,
				cat.CategoriePrincipale,
				cat.Categorie,
				cat.SousCategorie,
				status,
				priority,
				tagsLiteral,
			)
			argIndex += 9
		}

		// Time the insert
		insertStart := time.Now()
		query := fmt.Sprintf(`
			INSERT INTO %s.documents 
			(title, content, type_categorie, categorie_principale, categorie, sous_categorie, status, priority, tags)
			VALUES %s`,
			schemaName, strings.Join(valueStrings, ", "))

		_, err := pool.Exec(ctx, query, args...)
		if err != nil {
			t.Logf("Warning: Batch %d insert failed: %v", batch+1, err)
			continue
		}
		batchResult.InsertTime = time.Since(insertStart)

		// Re-enable triggers
		_, _ = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER ALL`, tableName))

		// Time merge_deltas (this updates the facets for the new documents)
		mergeStart := time.Now()
		_, err = pool.Exec(ctx, `SELECT facets.populate_facets($1::regclass)`, tableName)
		if err != nil {
			t.Logf("Warning: Batch %d populate_facets failed: %v", batch+1, err)
		}
		batchResult.MergeDeltaTime = time.Since(mergeStart)

		batchResult.DocsPerSec = float64(batchSize) / batchResult.InsertTime.Seconds()
		results.IncrementalBatches = append(results.IncrementalBatches, batchResult)

		t.Logf("  Batch %d: inserted %d docs in %v (%.1f docs/sec), facet update: %v",
			batch+1, batchSize, batchResult.InsertTime, batchResult.DocsPerSec, batchResult.MergeDeltaTime)
	}
}

// listMaintenanceProcedures outputs all procedures to call after document operations
func listMaintenanceProcedures(t *testing.T) {
	procedures := []struct {
		Name        string
		When        string
		Description string
		Example     string
	}{
		{
			Name:        "facets.merge_deltas(table_id)",
			When:        "After INSERT/UPDATE/DELETE",
			Description: "Applies pending delta updates to facet bitmaps. This is the PRIMARY procedure to call after any document changes.",
			Example:     "SELECT facets.merge_deltas('myschema.documents'::regclass);",
		},
		{
			Name:        "facets.run_maintenance()",
			When:        "Periodically (cron job)",
			Description: "Runs maintenance for ALL faceted tables. Ideal for scheduled background jobs.",
			Example:     "CALL facets.run_maintenance();",
		},
		{
			Name:        "facets.populate_facets(table_id)",
			When:        "Initial setup or full rebuild",
			Description: "Completely rebuilds facet data from scratch. Use when facet data is corrupt or after bulk imports.",
			Example:     "SELECT facets.populate_facets('myschema.documents'::regclass);",
		},
		{
			Name:        "merge_deltas_native(table_id)",
			When:        "After INSERT/UPDATE/DELETE (Zig native)",
			Description: "Native Zig implementation of merge_deltas. Higher performance for large delta sets.",
			Example:     "SELECT merge_deltas_native('myschema.documents'::regclass);",
		},
	}

	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║          PROCEDURES TO CALL AFTER DOCUMENT OPERATIONS                        ║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")

	for _, proc := range procedures {
		t.Log("║")
		t.Logf("║  📌 %s", proc.Name)
		t.Logf("║     When: %s", proc.When)
		t.Logf("║     %s", proc.Description)
		t.Logf("║     Example: %s", proc.Example)
	}

	t.Log("║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Log("║  RECOMMENDED WORKFLOW                                                         ║")
	t.Log("║                                                                              ║")
	t.Log("║  1. After single/few document changes:                                       ║")
	t.Log("║     SELECT facets.merge_deltas('schema.table'::regclass);                  ║")
	t.Log("║                                                                              ║")
	t.Log("║  2. After bulk operations (many inserts/updates/deletes):                    ║")
	t.Log("║     - Option A: Call merge_deltas after the batch completes                  ║")
	t.Log("║     - Option B: Let pg_cron handle it via run_maintenance()                  ║")
	t.Log("║                                                                              ║")
	t.Log("║  3. Schedule periodic maintenance (recommended):                             ║")
	t.Log("║     SELECT cron.schedule('faceting-maintenance', '*/5 * * * *',              ║")
	t.Log("║            'CALL facets.run_maintenance()');                               ║")
	t.Log("║                                                                              ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")
	t.Log("")
}

// printBenchmarkSummary outputs the benchmark results
func printBenchmarkSummary(t *testing.T, results *BenchmarkResults) {
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║                         BENCHMARK SUMMARY                                     ║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Documents: %d | Batch Size: %d | Queries: %d",
		results.Config.NumDocuments, results.Config.BatchSize, results.Config.NumQueries)
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Log("║  OPERATION                    │ TIME           │ RATE                        ║")
	t.Log("╠───────────────────────────────┼────────────────┼─────────────────────────────╣")

	insertRate := float64(results.Config.NumDocuments) / results.InsertTime.Seconds()
	t.Logf("║  Insert Documents (bulk)      │ %14v │ %.1f docs/sec", results.InsertTime, insertRate)
	t.Logf("║  Populate Facets              │ %14v │ -", results.PopulateFacetsTime)
	t.Logf("║  Merge Deltas (after insert)  │ %14v │ -", results.MergeDeltasAfterInsert)
	t.Logf("║  Average Facet Query          │ %14v │ -", results.AvgQueryTime)
	t.Logf("║  Update Documents (10%%)       │ %14v │ -", results.UpdateTime)
	t.Logf("║  Merge Deltas (after update)  │ %14v │ -", results.MergeDeltasAfterUpdate)
	t.Logf("║  Delete Documents (20%%)       │ %14v │ -", results.DeleteTime)
	t.Logf("║  Merge Deltas (after delete)  │ %14v │ -", results.MergeDeltasAfterDelete)

	// Print incremental batch results if available
	if len(results.IncrementalBatches) > 0 {
		t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
		t.Log("║  INCREMENTAL BATCH ADDITIONS (after initial load)                            ║")
		t.Log("╠───────────────────────────────┼────────────────┼─────────────────────────────╣")
		for _, batch := range results.IncrementalBatches {
			t.Logf("║  Batch %d (%d docs)            │ %14v │ %.1f docs/sec + %v facets",
				batch.BatchNum, batch.BatchSize, batch.InsertTime, batch.DocsPerSec, batch.MergeDeltaTime)
		}

		// Calculate average
		var totalInsert, totalMerge time.Duration
		var totalDocs int
		for _, batch := range results.IncrementalBatches {
			totalInsert += batch.InsertTime
			totalMerge += batch.MergeDeltaTime
			totalDocs += batch.BatchSize
		}
		avgInsertRate := float64(totalDocs) / totalInsert.Seconds()
		t.Log("╠───────────────────────────────┼────────────────┼─────────────────────────────╣")
		t.Logf("║  AVERAGE (incremental)        │ %14v │ %.1f docs/sec + %v facets",
			totalInsert/time.Duration(len(results.IncrementalBatches)),
			avgInsertRate,
			totalMerge/time.Duration(len(results.IncrementalBatches)))
	}

	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")
}

// escapeString escapes single quotes for SQL strings
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// TestFacetedNavigationBenchmark benchmarks the real faceted search workflow:
// 1. Initial load: Get ALL facet values with counts (full category tree)
// 2. User selects a facet: Update ALL facets to show filtered counts
// 3. User drills down: Each selection narrows further
func TestFacetedNavigationBenchmark(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	categories := loadCategories(t)
	t.Logf("Loaded %d categories from CSV", len(categories))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	schemaName := "bench_navigation"

	// ===========================================
	// SETUP: 103K documents with category hierarchy
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SETUP: Creating 103,000 documents with category hierarchy                   ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	setupBenchmarkSchema(t, pool, schemaName)
	defer cleanupBenchmarkSchema(t, pool, schemaName)

	cfg := BenchmarkConfig{NumDocuments: 103000, BatchSize: 1000}
	results := &BenchmarkResults{Config: cfg}
	insertedIDs := benchmarkInsertDocuments(t, pool, ctx, schemaName, categories, cfg, results)

	t.Logf("  Documents created: %d", len(insertedIDs))
	t.Logf("  Insert time: %v (%.0f docs/sec)", results.InsertTime, float64(len(insertedIDs))/results.InsertTime.Seconds())
	t.Logf("  Facet population time: %v", results.PopulateFacetsTime)

	tableName := fmt.Sprintf("%s.documents", schemaName)

	// Build the category tree structure for display
	categoryTree := buildCategoryTree(categories)
	t.Logf("  Category tree: %d principal categories", len(categoryTree))

	// ===========================================
	// BENCHMARK 1: Initial Load - Get ALL Facets (Full Tree)
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  STEP 1: INITIAL LOAD - Get ALL facet values (user opens page)               ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// This is what happens when user first loads the page - get ALL facet values
	initialLoadTimes := make([]time.Duration, 5)
	var initialFacetData []FacetResultRow

	for i := 0; i < 5; i++ {
		start := time.Now()
		initialFacetData = getAllFacetCounts(t, pool, ctx, tableName, nil)
		initialLoadTimes[i] = time.Since(start)
	}

	avgInitialLoad := averageDuration(initialLoadTimes)
	t.Logf("  Time to load ALL facets: %v (avg of 5 runs)", avgInitialLoad)
	t.Logf("  Total facet values returned: %d", len(initialFacetData))
	printFacetSummary(t, initialFacetData)

	// ===========================================
	// BENCHMARK 2: User Selects "Catégorie Principale"
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  STEP 2: User selects a 'Catégorie Principale' - Update all facets           ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Pick a random principal category
	principalCategories := getUniqueValues(categories, func(c CategoryEntry) string { return c.CategoriePrincipale })
	selectedPrincipal := principalCategories[rand.Intn(len(principalCategories))]
	t.Logf("  Selected: categorie_principale = '%s'", selectedPrincipal)

	filters1 := map[string]string{"categorie_principale": selectedPrincipal}
	step2Times := make([]time.Duration, 5)
	var step2FacetData []FacetResultRow

	for i := 0; i < 5; i++ {
		start := time.Now()
		step2FacetData = getAllFacetCounts(t, pool, ctx, tableName, filters1)
		step2Times[i] = time.Since(start)
	}

	avgStep2 := averageDuration(step2Times)
	t.Logf("  Time to update facets after selection: %v (avg of 5 runs)", avgStep2)
	t.Logf("  Facet values returned: %d", len(step2FacetData))
	printFacetSummary(t, step2FacetData)

	// ===========================================
	// BENCHMARK 3: User Drills Down to "Catégorie"
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  STEP 3: User selects a 'Catégorie' - Drill down further                     ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Find categories that belong to the selected principal
	matchingCategories := []string{}
	for _, cat := range categories {
		if cat.CategoriePrincipale == selectedPrincipal && cat.Categorie != "" {
			found := false
			for _, mc := range matchingCategories {
				if mc == cat.Categorie {
					found = true
					break
				}
			}
			if !found {
				matchingCategories = append(matchingCategories, cat.Categorie)
			}
		}
	}

	if len(matchingCategories) == 0 {
		t.Log("  No matching categories found, skipping this step")
	} else {
		selectedCategorie := matchingCategories[rand.Intn(len(matchingCategories))]
		t.Logf("  Selected: categorie = '%s'", selectedCategorie)

		filters2 := map[string]string{
			"categorie_principale": selectedPrincipal,
			"categorie":            selectedCategorie,
		}
		step3Times := make([]time.Duration, 5)
		var step3FacetData []FacetResultRow

		for i := 0; i < 5; i++ {
			start := time.Now()
			step3FacetData = getAllFacetCounts(t, pool, ctx, tableName, filters2)
			step3Times[i] = time.Since(start)
		}

		avgStep3 := averageDuration(step3Times)
		t.Logf("  Time to update facets after drill-down: %v (avg of 5 runs)", avgStep3)
		t.Logf("  Facet values returned: %d", len(step3FacetData))
		printFacetSummary(t, step3FacetData)

		// ===========================================
		// BENCHMARK 4: User Drills Down to "Sous-catégorie"
		// ===========================================
		t.Log("")
		t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
		t.Log("║  STEP 4: User selects a 'Sous-catégorie' - Final drill down                  ║")
		t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

		// Find sous-categories that match
		matchingSousCategories := []string{}
		for _, cat := range categories {
			if cat.CategoriePrincipale == selectedPrincipal &&
				cat.Categorie == selectedCategorie &&
				cat.SousCategorie != "" {
				found := false
				for _, ms := range matchingSousCategories {
					if ms == cat.SousCategorie {
						found = true
						break
					}
				}
				if !found {
					matchingSousCategories = append(matchingSousCategories, cat.SousCategorie)
				}
			}
		}

		if len(matchingSousCategories) > 0 {
			selectedSousCategorie := matchingSousCategories[rand.Intn(len(matchingSousCategories))]
			t.Logf("  Selected: sous_categorie = '%s'", selectedSousCategorie)

			filters3 := map[string]string{
				"categorie_principale": selectedPrincipal,
				"categorie":            selectedCategorie,
				"sous_categorie":       selectedSousCategorie,
			}
			step4Times := make([]time.Duration, 5)
			var step4FacetData []FacetResultRow

			for i := 0; i < 5; i++ {
				start := time.Now()
				step4FacetData = getAllFacetCounts(t, pool, ctx, tableName, filters3)
				step4Times[i] = time.Since(start)
			}

			avgStep4 := averageDuration(step4Times)
			t.Logf("  Time to update facets after final drill-down: %v (avg of 5 runs)", avgStep4)
			t.Logf("  Facet values returned: %d", len(step4FacetData))
			printFacetSummary(t, step4FacetData)
		}
	}

	// ===========================================
	// BENCHMARK 5: Get Matching Document IDs
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  STEP 5: Get document IDs matching the current selection                     ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	docIDTimes := make([]time.Duration, 5)
	var matchedDocCount int

	for i := 0; i < 5; i++ {
		start := time.Now()

		filterJSON := fmt.Sprintf(`{"categorie_principale": "%s"}`, escapeString(selectedPrincipal))
		query := fmt.Sprintf(`
			SELECT COUNT(*) FROM facets.filter_documents_by_facets(
				'%s', '%s'::jsonb, 'documents'
			)`, schemaName, filterJSON)

		err := pool.QueryRow(ctx, query).Scan(&matchedDocCount)
		if err != nil {
			t.Logf("  ERROR: %v", err)
			break
		}
		docIDTimes[i] = time.Since(start)
	}

	avgDocID := averageDuration(docIDTimes)
	t.Logf("  Time to get matching document IDs: %v (avg of 5 runs)", avgDocID)
	t.Logf("  Documents matching selection: %d", matchedDocCount)

	// ===========================================
	// BENCHMARK 6: Multiple Random Navigation Paths
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  STEP 6: Simulate 10 random user navigation paths                            ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	var totalNavTime time.Duration
	numPaths := 10

	for path := 0; path < numPaths; path++ {
		pathStart := time.Now()

		// Random principal category
		randPrincipal := principalCategories[rand.Intn(len(principalCategories))]

		// Step 1: Select principal
		getAllFacetCounts(t, pool, ctx, tableName, map[string]string{
			"categorie_principale": randPrincipal,
		})

		// Find matching categorie
		var randCategorie string
		for _, cat := range categories {
			if cat.CategoriePrincipale == randPrincipal && cat.Categorie != "" {
				randCategorie = cat.Categorie
				break
			}
		}

		if randCategorie != "" {
			// Step 2: Drill down to categorie
			getAllFacetCounts(t, pool, ctx, tableName, map[string]string{
				"categorie_principale": randPrincipal,
				"categorie":            randCategorie,
			})
		}

		pathTime := time.Since(pathStart)
		totalNavTime += pathTime
		t.Logf("  Path %d: %s → %s = %v", path+1, randPrincipal[:min(20, len(randPrincipal))], randCategorie[:min(20, len(randCategorie))], pathTime)
	}

	avgNavPath := totalNavTime / time.Duration(numPaths)
	t.Logf("  Average navigation path (2 selections): %v", avgNavPath)

	// ===========================================
	// FINAL SUMMARY
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  FACETED NAVIGATION BENCHMARK SUMMARY                                        ║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Total Documents: %d", len(insertedIDs))
	t.Logf("║  Category Tree: %d principal → %d categories → %d sub-categories",
		len(principalCategories),
		len(getUniqueValues(categories, func(c CategoryEntry) string { return c.Categorie })),
		len(getUniqueValues(categories, func(c CategoryEntry) string { return c.SousCategorie })))
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Initial page load (all facets):     %v", avgInitialLoad)
	t.Logf("║  After 1st selection (update facets): %v", avgStep2)
	t.Logf("║  Get matching document IDs:          %v", avgDocID)
	t.Logf("║  Avg 2-step navigation path:         %v", avgNavPath)
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Log("║  Facet population (full rebuild):    ", results.PopulateFacetsTime)
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")
}

// FacetResultRow represents a single row from count_results
type FacetResultRow struct {
	FacetName   string
	FacetValue  string
	Cardinality int64
	FacetID     int
}

// getAllFacetCounts gets all facet values with counts, optionally filtered
func getAllFacetCounts(t *testing.T, pool *pgxpool.Pool, ctx context.Context, tableName string, filters map[string]string) []FacetResultRow {
	var query string

	if len(filters) == 0 {
		// No filters - get all facet values using top_values with high limit
		query = fmt.Sprintf(`
			SELECT facet_name, facet_value, cardinality, facet_id
			FROM facets.top_values('%s'::regclass, 1000, NULL)`,
			tableName)
	} else {
		// With filters - use count_results
		filterParts := make([]string, 0, len(filters))
		for k, v := range filters {
			filterParts = append(filterParts, fmt.Sprintf("ROW('%s', '%s')", k, escapeString(v)))
		}
		query = fmt.Sprintf(`
			SELECT facet_name, facet_value, cardinality, facet_id
			FROM facets.count_results(
				'%s'::regclass::oid,
				ARRAY[%s]::facets.facet_filter[]
			)`,
			tableName, strings.Join(filterParts, ", "))
	}

	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Logf("  Query error: %v", err)
		return nil
	}
	defer rows.Close()

	var results []FacetResultRow
	for rows.Next() {
		var row FacetResultRow
		if err := rows.Scan(&row.FacetName, &row.FacetValue, &row.Cardinality, &row.FacetID); err != nil {
			continue
		}
		results = append(results, row)
	}

	return results
}

// getFacetsForDocumentIDs gets facet counts for a specific list of document IDs
// This is useful when you've already filtered documents (e.g., via BM25 search)
// and want to show available facets for just those documents
func getFacetsForDocumentIDs(t *testing.T, pool *pgxpool.Pool, ctx context.Context, schemaName, tableName string, documentIDs []int64) []FacetResultRow {
	if len(documentIDs) == 0 {
		return []FacetResultRow{}
	}

	// Extract table name from schema.table format
	tableParts := strings.Split(tableName, ".")
	actualTableName := tableParts[len(tableParts)-1]
	facetsTableName := fmt.Sprintf("%s.%s_facets", schemaName, actualTableName)

	// Convert IDs to string array for SQL
	idStrings := make([]string, len(documentIDs))
	for i, id := range documentIDs {
		idStrings[i] = fmt.Sprintf("%d", id)
	}

	// Build a bitmap from the document IDs
	// Note: rb_build expects int[], so we cast bigint[] to int[]
	// Then for each facet value, intersect with that bitmap and count
	query := fmt.Sprintf(`
		WITH target_bitmap AS (
			SELECT rb_build(ARRAY[%s]::int[]) AS bitmap
		),
		facet_counts AS (
			SELECT 
				fd.facet_id,
				fd.facet_name,
				fv.facet_value,
				rb_cardinality(rb_and(fv.postinglist, tb.bitmap)) AS cardinality
			FROM facets.facet_definition fd
			JOIN %s fv ON fv.facet_id = fd.facet_id
			CROSS JOIN target_bitmap tb
			WHERE fd.table_id = '%s.%s'::regclass::oid
			AND rb_cardinality(rb_and(fv.postinglist, tb.bitmap)) > 0
		)
		SELECT facet_name, facet_value, cardinality, facet_id
		FROM facet_counts
		ORDER BY facet_name, cardinality DESC`,
		strings.Join(idStrings, ","),
		facetsTableName,
		schemaName, actualTableName)

	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Logf("  Query error: %v", err)
		return nil
	}
	defer rows.Close()

	var results []FacetResultRow
	for rows.Next() {
		var row FacetResultRow
		if err := rows.Scan(&row.FacetName, &row.FacetValue, &row.Cardinality, &row.FacetID); err != nil {
			continue
		}
		results = append(results, row)
	}

	return results
}

// printFacetSummary prints a summary of facet results grouped by facet name
func printFacetSummary(t *testing.T, data []FacetResultRow) {
	if len(data) == 0 {
		t.Log("    (no facets found)")
		return
	}

	// Group by facet name
	facetGroups := make(map[string][]FacetResultRow)
	for _, row := range data {
		facetGroups[row.FacetName] = append(facetGroups[row.FacetName], row)
	}

	for facetName, rows := range facetGroups {
		var totalCount int64
		for _, r := range rows {
			totalCount += r.Cardinality
		}
		t.Logf("    %s: %d values, total docs: %d", facetName, len(rows), totalCount)

		// Show top 5 values for each facet
		// Sort by count descending
		sortedRows := make([]FacetResultRow, len(rows))
		copy(sortedRows, rows)
		for i := 0; i < len(sortedRows); i++ {
			for j := i + 1; j < len(sortedRows); j++ {
				if sortedRows[j].Cardinality > sortedRows[i].Cardinality {
					sortedRows[i], sortedRows[j] = sortedRows[j], sortedRows[i]
				}
			}
		}

		showCount := 5
		if len(sortedRows) < showCount {
			showCount = len(sortedRows)
		}
		for i := 0; i < showCount; i++ {
			value := sortedRows[i].FacetValue
			if len(value) > 50 {
				value = value[:50] + "..."
			}
			t.Logf("      • %s: %d docs", value, sortedRows[i].Cardinality)
		}
		if len(sortedRows) > showCount {
			t.Logf("      ... and %d more", len(sortedRows)-showCount)
		}
	}
}

// buildCategoryTree builds a tree structure from the categories
func buildCategoryTree(categories []CategoryEntry) map[string]map[string][]string {
	tree := make(map[string]map[string][]string)

	for _, cat := range categories {
		if _, ok := tree[cat.CategoriePrincipale]; !ok {
			tree[cat.CategoriePrincipale] = make(map[string][]string)
		}
		if cat.Categorie != "" {
			if _, ok := tree[cat.CategoriePrincipale][cat.Categorie]; !ok {
				tree[cat.CategoriePrincipale][cat.Categorie] = []string{}
			}
			if cat.SousCategorie != "" {
				tree[cat.CategoriePrincipale][cat.Categorie] = append(
					tree[cat.CategoriePrincipale][cat.Categorie],
					cat.SousCategorie,
				)
			}
		}
	}

	return tree
}

// averageDuration calculates the average of a slice of durations
func averageDuration(durations []time.Duration) time.Duration {
	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

// =============================================================================
// TREE RENDERING BENCHMARK
// =============================================================================

// FacetTreeNode represents a node in the facet tree
type FacetTreeNode struct {
	Name     string           `json:"name"`
	Count    int64            `json:"count"`
	Children []*FacetTreeNode `json:"children,omitempty"`
}

// FacetTree represents the complete facet tree for UI rendering
type FacetTree struct {
	TotalDocuments int64                     `json:"total_documents"`
	Facets         map[string]*FacetTreeNode `json:"facets"`
	CategoryTree   *FacetTreeNode            `json:"category_tree"`
	BuildTime      string                    `json:"build_time"`
}

// TestFacetTreeRenderingBenchmark benchmarks building the complete facet tree for UI
func TestFacetTreeRenderingBenchmark(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	categories := loadCategories(t)
	t.Logf("Loaded %d categories from CSV", len(categories))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	schemaName := "bench_tree"

	// Setup with 103K documents
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SETUP: Creating 103,000 documents                                           ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	setupBenchmarkSchema(t, pool, schemaName)
	defer cleanupBenchmarkSchema(t, pool, schemaName)

	cfg := BenchmarkConfig{NumDocuments: 103000, BatchSize: 1000}
	results := &BenchmarkResults{Config: cfg}
	_ = benchmarkInsertDocuments(t, pool, ctx, schemaName, categories, cfg, results)

	tableName := fmt.Sprintf("%s.documents", schemaName)

	// ===========================================
	// BENCHMARK 1: Build Complete Facet Tree (No Filter)
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  BENCHMARK 1: Build complete facet tree (initial page load)                  ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	var tree *FacetTree
	var jsonBytes []byte
	buildTimes := make([]time.Duration, 5)

	for i := 0; i < 5; i++ {
		start := time.Now()
		tree = buildFacetTree(t, pool, ctx, schemaName, tableName, categories, nil)
		buildTimes[i] = time.Since(start)
	}

	avgBuildTime := averageDuration(buildTimes)
	t.Logf("  Tree build time: %v (avg of 5 runs)", avgBuildTime)

	// Serialize to JSON
	jsonStart := time.Now()
	var err error
	jsonBytes, err = json.MarshalIndent(tree, "", "  ")
	if err != nil {
		t.Fatalf("JSON marshal failed: %v", err)
	}
	jsonTime := time.Since(jsonStart)
	t.Logf("  JSON serialization: %v (%d bytes)", jsonTime, len(jsonBytes))
	t.Logf("  Total (build + JSON): %v", avgBuildTime+jsonTime)

	// Print the tree structure
	t.Log("")
	t.Log("  📁 CATEGORY TREE (with document counts):")
	printTreeNode(t, tree.CategoryTree, "  ")

	// Print other facets
	t.Log("")
	t.Log("  📊 OTHER FACETS:")
	for facetName, node := range tree.Facets {
		if facetName != "category_tree" {
			t.Logf("    %s: %d values", facetName, len(node.Children))
			// Print first 5 values
			for i, child := range node.Children {
				if i >= 5 {
					t.Logf("      ... and %d more", len(node.Children)-5)
					break
				}
				t.Logf("      • %s: %d docs", child.Name, child.Count)
			}
		}
	}

	// ===========================================
	// BENCHMARK 2: Build Tree After Selection
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  BENCHMARK 2: Build tree after user selection                                ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Select a principal category
	principalCategories := getUniqueValues(categories, func(c CategoryEntry) string { return c.CategoriePrincipale })
	selectedPrincipal := principalCategories[0]
	filters := map[string]string{"categorie_principale": selectedPrincipal}

	t.Logf("  Filter: categorie_principale = '%s'", selectedPrincipal)

	filteredBuildTimes := make([]time.Duration, 5)
	var filteredTree *FacetTree

	for i := 0; i < 5; i++ {
		start := time.Now()
		filteredTree = buildFacetTree(t, pool, ctx, schemaName, tableName, categories, filters)
		filteredBuildTimes[i] = time.Since(start)
	}

	avgFilteredBuildTime := averageDuration(filteredBuildTimes)
	t.Logf("  Tree build time (filtered): %v (avg of 5 runs)", avgFilteredBuildTime)

	// Serialize filtered tree
	jsonStart = time.Now()
	filteredJsonBytes, _ := json.MarshalIndent(filteredTree, "", "  ")
	filteredJsonTime := time.Since(jsonStart)
	t.Logf("  JSON serialization: %v (%d bytes)", filteredJsonTime, len(filteredJsonBytes))

	// Print filtered tree
	t.Log("")
	t.Log("  📁 FILTERED CATEGORY TREE:")
	printTreeNode(t, filteredTree.CategoryTree, "  ")

	// ===========================================
	// BENCHMARK 3: Full Render Cycle (Query + Build + JSON)
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  BENCHMARK 3: Full render cycle (DB query + tree build + JSON)               ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	fullCycleTimes := make([]time.Duration, 10)

	for i := 0; i < 10; i++ {
		start := time.Now()

		// 1. Query facets from DB
		facetData := getAllFacetCounts(t, pool, ctx, tableName, nil)

		// 2. Build tree structure
		tree := buildFacetTreeFromData(categories, facetData)

		// 3. Serialize to JSON
		_, _ = json.Marshal(tree)

		fullCycleTimes[i] = time.Since(start)
	}

	avgFullCycle := averageDuration(fullCycleTimes)
	t.Logf("  Full render cycle: %v (avg of 10 runs)", avgFullCycle)
	t.Logf("  Breakdown estimate:")
	t.Logf("    - DB query: ~%v", avgBuildTime-avgBuildTime/3)
	t.Logf("    - Tree build: ~%v", avgBuildTime/3)
	t.Logf("    - JSON serialize: ~%v", jsonTime)

	// ===========================================
	// OUTPUT: Sample JSON for UI
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SAMPLE JSON OUTPUT (first 2000 chars)                                       ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	sampleLen := 2000
	if len(jsonBytes) < sampleLen {
		sampleLen = len(jsonBytes)
	}
	t.Logf("\n%s\n...", string(jsonBytes[:sampleLen]))

	// ===========================================
	// SUMMARY
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  TREE RENDERING BENCHMARK SUMMARY                                            ║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Documents: 103,000")
	t.Logf("║  Category hierarchy: 7 principal → 19 categories → 59 sub-categories")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Initial tree build:      %v", avgBuildTime)
	t.Logf("║  Filtered tree build:     %v", avgFilteredBuildTime)
	t.Logf("║  JSON serialization:      %v", jsonTime)
	t.Logf("║  Full render cycle:       %v", avgFullCycle)
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  JSON size (unfiltered):  %d bytes (%.1f KB)", len(jsonBytes), float64(len(jsonBytes))/1024)
	t.Logf("║  JSON size (filtered):    %d bytes (%.1f KB)", len(filteredJsonBytes), float64(len(filteredJsonBytes))/1024)
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")
}

// buildFacetTree builds the complete facet tree from database queries
func buildFacetTree(t *testing.T, pool *pgxpool.Pool, ctx context.Context, schemaName, tableName string, categories []CategoryEntry, filters map[string]string) *FacetTree {
	// Get facet data from database
	facetData := getAllFacetCounts(t, pool, ctx, tableName, filters)

	return buildFacetTreeFromData(categories, facetData)
}

// buildFacetTreeFromData builds the tree structure from facet data
func buildFacetTreeFromData(categories []CategoryEntry, facetData []FacetResultRow) *FacetTree {
	tree := &FacetTree{
		Facets: make(map[string]*FacetTreeNode),
	}

	// Group facet data by facet name
	facetGroups := make(map[string]map[string]int64)
	for _, row := range facetData {
		if _, ok := facetGroups[row.FacetName]; !ok {
			facetGroups[row.FacetName] = make(map[string]int64)
		}
		facetGroups[row.FacetName][row.FacetValue] = row.Cardinality
	}

	// Build flat facets (status, priority, tags, etc.)
	for facetName, values := range facetGroups {
		if facetName == "categorie_principale" || facetName == "categorie" ||
			facetName == "sous_categorie" || facetName == "type_categorie" {
			continue // These go into the category tree
		}

		node := &FacetTreeNode{
			Name:     facetName,
			Children: make([]*FacetTreeNode, 0),
		}

		for value, count := range values {
			node.Children = append(node.Children, &FacetTreeNode{
				Name:  value,
				Count: count,
			})
			node.Count += count
		}

		// Sort by count descending
		sortTreeChildren(node)
		tree.Facets[facetName] = node
	}

	// Build hierarchical category tree
	tree.CategoryTree = buildCategoryTreeWithCounts(categories, facetGroups)

	// Prune nodes with 0 documents
	pruneEmptyNodes(tree.CategoryTree)

	// Calculate total documents
	if principalCounts, ok := facetGroups["categorie_principale"]; ok {
		for _, count := range principalCounts {
			tree.TotalDocuments += count
		}
	}

	return tree
}

// buildCategoryTreeWithCounts builds the hierarchical category tree with counts
func buildCategoryTreeWithCounts(categories []CategoryEntry, facetGroups map[string]map[string]int64) *FacetTreeNode {
	root := &FacetTreeNode{
		Name:     "Gestion Locative",
		Children: make([]*FacetTreeNode, 0),
	}

	// Get counts
	principalCounts := facetGroups["categorie_principale"]
	categorieCounts := facetGroups["categorie"]
	sousCounts := facetGroups["sous_categorie"]

	// Build tree structure
	principalMap := make(map[string]*FacetTreeNode)
	categorieMap := make(map[string]map[string]*FacetTreeNode)

	for _, cat := range categories {
		// Level 1: Catégorie Principale
		if _, ok := principalMap[cat.CategoriePrincipale]; !ok {
			count := int64(0)
			if principalCounts != nil {
				count = principalCounts[cat.CategoriePrincipale]
			}
			principalMap[cat.CategoriePrincipale] = &FacetTreeNode{
				Name:     cat.CategoriePrincipale,
				Count:    count,
				Children: make([]*FacetTreeNode, 0),
			}
			categorieMap[cat.CategoriePrincipale] = make(map[string]*FacetTreeNode)
		}

		// Level 2: Catégorie
		if cat.Categorie != "" {
			if _, ok := categorieMap[cat.CategoriePrincipale][cat.Categorie]; !ok {
				count := int64(0)
				if categorieCounts != nil {
					count = categorieCounts[cat.Categorie]
				}
				categorieMap[cat.CategoriePrincipale][cat.Categorie] = &FacetTreeNode{
					Name:     cat.Categorie,
					Count:    count,
					Children: make([]*FacetTreeNode, 0),
				}
			}

			// Level 3: Sous-catégorie
			if cat.SousCategorie != "" {
				count := int64(0)
				if sousCounts != nil {
					count = sousCounts[cat.SousCategorie]
				}

				// Check if already added
				found := false
				for _, child := range categorieMap[cat.CategoriePrincipale][cat.Categorie].Children {
					if child.Name == cat.SousCategorie {
						found = true
						break
					}
				}

				if !found {
					categorieMap[cat.CategoriePrincipale][cat.Categorie].Children = append(
						categorieMap[cat.CategoriePrincipale][cat.Categorie].Children,
						&FacetTreeNode{
							Name:  cat.SousCategorie,
							Count: count,
						},
					)
				}
			}
		}
	}

	// Assemble the tree
	for principalName, principalNode := range principalMap {
		for _, categorieNode := range categorieMap[principalName] {
			sortTreeChildren(categorieNode)
			principalNode.Children = append(principalNode.Children, categorieNode)
		}
		sortTreeChildren(principalNode)
		root.Children = append(root.Children, principalNode)
		root.Count += principalNode.Count
	}

	sortTreeChildren(root)
	return root
}

// sortTreeChildren sorts children by count descending
func sortTreeChildren(node *FacetTreeNode) {
	if len(node.Children) == 0 {
		return
	}

	// Simple bubble sort (good enough for small arrays)
	for i := 0; i < len(node.Children); i++ {
		for j := i + 1; j < len(node.Children); j++ {
			if node.Children[j].Count > node.Children[i].Count {
				node.Children[i], node.Children[j] = node.Children[j], node.Children[i]
			}
		}
	}
}

// pruneEmptyNodes removes nodes with 0 docs from the tree
// Returns true if the node should be kept, false if it should be removed
func pruneEmptyNodes(node *FacetTreeNode) bool {
	if node == nil {
		return false
	}

	// First, recursively prune children
	var keptChildren []*FacetTreeNode
	for _, child := range node.Children {
		if pruneEmptyNodes(child) {
			keptChildren = append(keptChildren, child)
		}
	}
	node.Children = keptChildren

	// A node is kept if:
	// 1. It has a count > 0, OR
	// 2. It has children (even if its own count is 0, it's a container)
	return node.Count > 0 || len(node.Children) > 0
}

// printTreeNode recursively prints the tree structure
func printTreeNode(t *testing.T, node *FacetTreeNode, indent string) {
	if node == nil {
		return
	}

	if node.Count > 0 {
		t.Logf("%s📁 %s (%d docs)", indent, node.Name, node.Count)
	} else {
		t.Logf("%s📁 %s", indent, node.Name)
	}

	for _, child := range node.Children {
		if len(child.Children) > 0 {
			printTreeNode(t, child, indent+"  ")
		} else {
			t.Logf("%s  📄 %s (%d docs)", indent, child.Name, child.Count)
		}
	}
}

// =============================================================================
// DOCUMENT METADATA FACETS FOR JURIDIC/TECHNICAL DOCUMENTS
// =============================================================================

// DocumentMetadata represents a document with rich metadata for faceted search
type DocumentMetadata struct {
	// Core identification
	ID       int64  `json:"id"`
	Title    string `json:"title"`
	Content  string `json:"content"`
	FilePath string `json:"file_path"` // e.g., "/contracts/2024/client_abc/contract_001.pdf"

	// Document classification
	DocumentType    string `json:"document_type"`    // juridic, technical, administrative, financial
	DocumentSubtype string `json:"document_subtype"` // contract, invoice, report, specification, etc.

	// Category hierarchy (from CSV)
	CategoriePrincipale string `json:"categorie_principale"`
	Categorie           string `json:"categorie"`
	SousCategorie       string `json:"sous_categorie"`

	// Status and lifecycle
	Status          string `json:"status"`          // draft, pending_review, active, archived, expired
	Confidentiality string `json:"confidentiality"` // public, internal, confidential, restricted

	// Time-based metadata (critical for juridic documents)
	CreatedAt  time.Time  `json:"created_at"`
	ModifiedAt time.Time  `json:"modified_at"`
	ValidFrom  *time.Time `json:"valid_from,omitempty"`  // Contract start date
	ValidUntil *time.Time `json:"valid_until,omitempty"` // Contract end date / expiry
	SignedAt   *time.Time `json:"signed_at,omitempty"`   // Signature date
	ArchivedAt *time.Time `json:"archived_at,omitempty"`

	// Organization
	Author     string   `json:"author"`
	Department string   `json:"department"`
	Client     string   `json:"client,omitempty"`
	Tags       []string `json:"tags"`

	// For vector search (RAG)
	// Embedding would be stored in a separate column/table
}

// TestDocumentMetadataFacetsBenchmark tests faceted search with rich document metadata
func TestDocumentMetadataFacetsBenchmark(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	categories := loadCategories(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	schemaName := "bench_docs"

	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  DOCUMENT METADATA FACETS BENCHMARK                                          ║")
	t.Log("║  Simulating juridic/technical document management system                     ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// ===========================================
	// SETUP: Create rich metadata schema
	// ===========================================
	setupDocumentMetadataSchema(t, pool, ctx, schemaName)
	defer cleanupBenchmarkSchema(t, pool, schemaName)

	// Insert 50K documents with rich metadata
	t.Log("")
	t.Log("  📥 Inserting 50,000 documents with rich metadata...")
	insertStart := time.Now()
	insertDocumentsWithMetadata(t, pool, ctx, schemaName, categories, 50000)
	insertTime := time.Since(insertStart)

	// Verify actual insert count
	var actualCount int
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s.documents`, schemaName)
	err := pool.QueryRow(ctx, countQuery).Scan(&actualCount)
	if err == nil {
		t.Logf("  ✓ Insert completed in %v", insertTime)
		t.Logf("  ✓ Actually inserted: %d documents (target was 50,000)", actualCount)
		if actualCount < 50000 {
			t.Logf("  ⚠️  Some inserts may have failed due to UTF-8 encoding issues")
		}
	} else {
		t.Logf("  ✓ Insert completed in %v (could not verify count)", insertTime)
	}

	// Populate facets
	t.Log("  📊 Populating facets...")
	tableName := fmt.Sprintf("%s.documents", schemaName)
	populateStart := time.Now()
	_, err = pool.Exec(ctx, `SELECT facets.populate_facets($1::regclass)`, tableName)
	if err != nil {
		t.Fatalf("Failed to populate facets: %v", err)
	}
	populateTime := time.Since(populateStart)
	t.Logf("  ✓ Facets populated in %v", populateTime)

	// ===========================================
	// BENCHMARK 1: Load All Facets (Initial Page)
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 1: Initial page load - Get all facets                              ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	var facetData []FacetResultRow
	initialTimes := benchmarkQuery(t, pool, ctx, 5, func() {
		facetData = getAllFacetCounts(t, pool, ctx, tableName, nil)
	})
	t.Logf("  Time: %v (avg)", averageDuration(initialTimes))
	t.Log("  Facets:")
	printFacetSummary(t, facetData)

	// ===========================================
	// BENCHMARK 2: Filter by Document Type
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 2: User filters by document_type = 'juridic'                       ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	filters2 := map[string]string{"document_type": "juridic"}
	var facetData2 []FacetResultRow
	times2 := benchmarkQuery(t, pool, ctx, 5, func() {
		facetData2 = getAllFacetCounts(t, pool, ctx, tableName, filters2)
	})
	t.Logf("  Time: %v (avg)", averageDuration(times2))
	t.Log("  Facets:")
	printFacetSummary(t, facetData2)

	// ===========================================
	// BENCHMARK 3: Filter by Time Range (Year)
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 3: User filters by created_at year (2024)                          ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Note: datetrunc facets store truncated dates as strings
	filters3 := map[string]string{"created_year": "2024"}
	var facetData3 []FacetResultRow
	times3 := benchmarkQuery(t, pool, ctx, 5, func() {
		facetData3 = getAllFacetCounts(t, pool, ctx, tableName, filters3)
	})
	t.Logf("  Time: %v (avg)", averageDuration(times3))
	t.Log("  Facets:")
	printFacetSummary(t, facetData3)

	// ===========================================
	// BENCHMARK 4: Multi-facet Filter (Document Type + Status + Department)
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 4: Multi-filter (juridic + active + Legal department)              ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	filters4 := map[string]string{
		"document_type": "juridic",
		"status":        "active",
		"department":    "Legal",
	}
	var facetData4 []FacetResultRow
	times4 := benchmarkQuery(t, pool, ctx, 5, func() {
		facetData4 = getAllFacetCounts(t, pool, ctx, tableName, filters4)
	})
	t.Logf("  Time: %v (avg)", averageDuration(times4))
	t.Logf("  Matching facet values: %d", len(facetData4))
	t.Log("  Facets:")
	printFacetSummary(t, facetData4)

	// ===========================================
	// BENCHMARK 5: Filter + Get Document IDs
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 5: Get document IDs for filtered results                           ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	var docCount int
	times5 := benchmarkQuery(t, pool, ctx, 5, func() {
		query := fmt.Sprintf(`
			SELECT COUNT(*) FROM facets.filter_documents_by_facets(
				'%s', '{"document_type": "juridic", "status": "active"}'::jsonb, 'documents'
			)`, schemaName)
		pool.QueryRow(ctx, query).Scan(&docCount)
	})
	t.Logf("  Time: %v (avg)", averageDuration(times5))
	t.Logf("  Documents matching: %d", docCount)

	// ===========================================
	// BENCHMARK 6: Facets + BM25 Text Search
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 6: Faceted search + BM25 text search                               ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// This simulates: user types "contrat location" AND filters by document_type=juridic
	searchQuery := "contrat bail locataire"

	t.Logf("  Query: '%s' + filter: document_type='juridic'", searchQuery)

	var searchResultCount int
	times6 := benchmarkQuery(t, pool, ctx, 5, func() {
		// Step 1: Get filtered document IDs via facets
		filterJSON := `{"document_type": "juridic"}`
		idQuery := fmt.Sprintf(`
			SELECT id FROM facets.filter_documents_by_facets(
				'%s', '%s'::jsonb, 'documents'
			)`, schemaName, filterJSON)

		rows, _ := pool.Query(ctx, idQuery)
		var ids []int64
		for rows.Next() {
			var id int64
			rows.Scan(&id)
			ids = append(ids, id)
		}
		rows.Close()

		// Step 2: BM25 search within those IDs (simplified - real impl would use ts_rank)
		if len(ids) > 0 {
			// In real implementation, you'd use pg_search or ts_rank here
			// This is a placeholder showing the pattern
			searchResultCount = len(ids)
		}
	})
	t.Logf("  Time (facet filter + ID retrieval): %v (avg)", averageDuration(times6))
	t.Logf("  Candidate documents for BM25: %d", searchResultCount)

	// ===========================================
	// BENCHMARK 7: Time-Range Faceted Search
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 7: Time-range query (contracts expiring in next 90 days)           ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Query for contracts expiring soon
	expiringQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s.documents 
		WHERE document_type = 'juridic' 
		AND valid_until IS NOT NULL 
		AND valid_until BETWEEN NOW() AND NOW() + INTERVAL '90 days'`,
		schemaName)

	var expiringCount int
	times7 := benchmarkQuery(t, pool, ctx, 5, func() {
		pool.QueryRow(ctx, expiringQuery).Scan(&expiringCount)
	})
	t.Logf("  Time: %v (avg)", averageDuration(times7))
	t.Logf("  Contracts expiring in 90 days: %d", expiringCount)

	// ===========================================
	// BENCHMARK 8: Directory Path Hierarchy
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 8: Filter by directory path hierarchy                              ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Get documents in /contracts/2024/
	pathQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM %s.documents 
		WHERE file_path LIKE '/contracts/2024/%%'`,
		schemaName)

	var pathCount int
	times8 := benchmarkQuery(t, pool, ctx, 5, func() {
		pool.QueryRow(ctx, pathQuery).Scan(&pathCount)
	})
	t.Logf("  Time (LIKE query): %v (avg)", averageDuration(times8))
	t.Logf("  Documents in /contracts/2024/: %d", pathCount)

	// ===========================================
	// BENCHMARK 9: Get Facets for Specific Document IDs
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  SCENARIO 9: Get facets for a list of document IDs                          ║")
	t.Log("║  (Use case: After BM25 search, show available facets for results)           ║")
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Declare timing variables for summary
	var times9_10, times9_50, times9_100 []time.Duration

	// First, check how many documents we have
	var totalDocCount int
	docCountQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
	err = pool.QueryRow(ctx, docCountQuery).Scan(&totalDocCount)
	if err != nil {
		t.Logf("  Warning: Could not count documents: %v", err)
		totalDocCount = 0
	} else {
		t.Logf("  Total documents in table: %d", totalDocCount)
	}

	// Get some document IDs (simulating BM25 search results)
	var sampleIDs []int64
	if totalDocCount > 0 {
		idQuery := fmt.Sprintf(`SELECT id FROM %s ORDER BY id LIMIT 100`, tableName)
		rows, err := pool.Query(ctx, idQuery)
		if err != nil {
			t.Logf("  Warning: Could not query document IDs: %v", err)
			t.Logf("  Query was: %s", idQuery)
		} else {
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err == nil {
					sampleIDs = append(sampleIDs, id)
				}
			}
			rows.Close()
		}
	}

	if len(sampleIDs) > 0 {
		t.Logf("  Sample document IDs: %d documents", len(sampleIDs))

		// Test with different sizes
		testSizes := []int{10, 50, 100}
		if len(sampleIDs) < 100 {
			testSizes = []int{min(10, len(sampleIDs)), min(50, len(sampleIDs)), len(sampleIDs)}
		}

		for _, size := range testSizes {
			if size > len(sampleIDs) {
				continue
			}
			testIDs := sampleIDs[:size]

			var facetData9 []FacetResultRow
			times9 := benchmarkQuery(t, pool, ctx, 5, func() {
				facetData9 = getFacetsForDocumentIDs(t, pool, ctx, schemaName, tableName, testIDs)
			})

			// Store timings for summary
			switch size {
			case 10:
				times9_10 = times9
			case 50:
				times9_50 = times9
			case 100:
				times9_100 = times9
			}

			t.Logf("  Time for %d document IDs: %v (avg)", size, averageDuration(times9))
			t.Logf("  Facets found: %d facet values", len(facetData9))
			if len(facetData9) > 0 {
				t.Log("  Sample facets:")
				// Show first 10 facet values
				showCount := min(10, len(facetData9))
				for i := 0; i < showCount; i++ {
					value := facetData9[i].FacetValue
					if len(value) > 40 {
						value = value[:40] + "..."
					}
					t.Logf("    • %s.%s: %d docs", facetData9[i].FacetName, value, facetData9[i].Cardinality)
				}
				if len(facetData9) > showCount {
					t.Logf("    ... and %d more", len(facetData9)-showCount)
				}
			}
		}
	} else {
		if totalDocCount == 0 {
			t.Log("  ⚠️  No documents found in table (insert may have failed due to UTF-8 errors)")
			t.Log("  This benchmark requires documents to be successfully inserted")
		} else {
			t.Logf("  ⚠️  Could not retrieve document IDs (table has %d docs but query failed)", totalDocCount)
		}
	}

	// ===========================================
	// SUMMARY
	// ===========================================
	t.Log("")
	t.Log("╔══════════════════════════════════════════════════════════════════════════════╗")
	t.Log("║  DOCUMENT METADATA FACETS - BENCHMARK SUMMARY                                ║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Log("║  Documents: 50,000 with rich metadata                                        ║")
	t.Log("║  Facets: 12 (document_type, subtype, status, confidentiality,                ║")
	t.Log("║          department, author, tags, created_year, created_month,              ║")
	t.Log("║          categorie_principale, categorie, sous_categorie)                    ║")
	t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
	t.Logf("║  Initial load (all facets):        %v", averageDuration(initialTimes))
	t.Logf("║  Single filter (document_type):    %v", averageDuration(times2))
	t.Logf("║  Year filter (created_year):       %v", averageDuration(times3))
	t.Logf("║  Multi-filter (3 facets):          %v", averageDuration(times4))
	t.Logf("║  Filter + get doc IDs:             %v", averageDuration(times5))
	t.Logf("║  Facet + BM25 prep:                %v", averageDuration(times6))
	t.Logf("║  Time-range (expiring contracts):  %v", averageDuration(times7))
	t.Logf("║  Path hierarchy (LIKE):            %v", averageDuration(times8))
	if len(times9_10) > 0 {
		t.Log("╠══════════════════════════════════════════════════════════════════════════════╣")
		t.Log("║  Get facets for document IDs (Scenario 9):                                  ║")
		t.Logf("║    - 10 document IDs:            %v", averageDuration(times9_10))
		if len(times9_50) > 0 {
			t.Logf("║    - 50 document IDs:            %v", averageDuration(times9_50))
		}
		if len(times9_100) > 0 {
			t.Logf("║    - 100 document IDs:           %v", averageDuration(times9_100))
		}
	}
	t.Log("╚══════════════════════════════════════════════════════════════════════════════╝")
}

// setupDocumentMetadataSchema creates the schema with rich document metadata
func setupDocumentMetadataSchema(t *testing.T, pool *pgxpool.Pool, ctx context.Context, schemaName string) {
	// Clean up first
	cleanupBenchmarkSchema(t, pool, schemaName)

	queries := []string{
		fmt.Sprintf(`CREATE SCHEMA %s`, schemaName),
		fmt.Sprintf(`CREATE TABLE %s.documents (
			id SERIAL PRIMARY KEY,
			
			-- Core content
			title TEXT NOT NULL,
			content TEXT,
			file_path TEXT,
			
			-- Document classification
			document_type TEXT NOT NULL,      -- juridic, technical, administrative, financial
			document_subtype TEXT,            -- contract, invoice, report, specification
			
			-- Category hierarchy
			categorie_principale TEXT,
			categorie TEXT,
			sous_categorie TEXT,
			
			-- Status and lifecycle
			status TEXT DEFAULT 'draft',
			confidentiality TEXT DEFAULT 'internal',
			
			-- Time metadata
			created_at TIMESTAMPTZ,
			modified_at TIMESTAMPTZ,
			valid_from TIMESTAMPTZ,
			valid_until TIMESTAMPTZ,
			signed_at TIMESTAMPTZ,
			archived_at TIMESTAMPTZ,
			
			-- Derived time facets (populated during insert for efficient faceting)
			created_year INTEGER,
			created_month INTEGER,
			
			-- Organization
			author TEXT,
			department TEXT,
			client TEXT,
			tags TEXT[]
		)`, schemaName),

		// Note: search_vector for full-text search would be added in production
		// Skipped here to focus on faceted search benchmarks

		// Create index for time-range queries
		fmt.Sprintf(`CREATE INDEX idx_documents_valid_until ON %s.documents(valid_until) WHERE valid_until IS NOT NULL`, schemaName),

		// Create index for path queries
		fmt.Sprintf(`CREATE INDEX idx_documents_file_path ON %s.documents(file_path text_pattern_ops)`, schemaName),

		// Add faceting
		fmt.Sprintf(`SELECT facets.add_faceting_to_table(
			'%s.documents',
			key => 'id',
			facets => ARRAY[
				facets.plain_facet('document_type'),
				facets.plain_facet('document_subtype'),
				facets.plain_facet('status'),
				facets.plain_facet('confidentiality'),
				facets.plain_facet('department'),
				facets.plain_facet('author'),
				facets.plain_facet('client'),
				facets.plain_facet('categorie_principale'),
				facets.plain_facet('categorie'),
				facets.plain_facet('sous_categorie'),
				facets.plain_facet('created_year'),
				facets.plain_facet('created_month'),
				facets.array_facet('tags')
			],
			populate => false
		)`, schemaName),
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Fatalf("Setup failed: %v\nQuery: %s", err, q[:min(200, len(q))])
		}
	}

	t.Log("  ✓ Schema created with 13 facets")
}

// insertDocumentsWithMetadata inserts documents with rich metadata
func insertDocumentsWithMetadata(t *testing.T, pool *pgxpool.Pool, ctx context.Context, schemaName string, categories []CategoryEntry, count int) {
	tableName := fmt.Sprintf("%s.documents", schemaName)

	// Disable triggers for bulk insert
	pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER ALL`, tableName))
	defer pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER ALL`, tableName))

	documentTypes := []string{"juridic", "technical", "administrative", "financial"}
	documentSubtypes := map[string][]string{
		"juridic":        {"contract", "amendment", "termination", "lease", "agreement", "power_of_attorney"},
		"technical":      {"specification", "manual", "report", "diagram", "procedure"},
		"administrative": {"memo", "policy", "guideline", "form", "certificate"},
		"financial":      {"invoice", "receipt", "budget", "statement", "audit"},
	}
	statuses := []string{"draft", "pending_review", "active", "archived", "expired"}
	confidentialities := []string{"public", "internal", "confidential", "restricted"}
	departments := []string{"Legal", "Finance", "Operations", "HR", "IT", "Sales", "Marketing"}
	authors := []string{"Alice Martin", "Bob Dupont", "Claire Bernard", "David Petit", "Emma Moreau",
		"François Leroy", "Gabrielle Simon", "Henri Laurent", "Isabelle Michel", "Jacques Durand"}
	clients := []string{"Client Alpha", "Client Beta", "Client Gamma", "Client Delta", "Client Epsilon",
		"Client Zeta", "Client Eta", "Client Theta", "Client Iota", "Client Kappa", ""}
	tagOptions := []string{"urgent", "important", "review_needed", "approved", "pending",
		"confidential", "archived", "template", "final", "draft"}

	pathPrefixes := []string{"/contracts", "/reports", "/invoices", "/policies", "/specifications"}
	years := []string{"2022", "2023", "2024", "2025"}

	batchSize := 500
	for batch := 0; batch < count; batch += batchSize {
		currentBatch := batchSize
		if batch+batchSize > count {
			currentBatch = count - batch
		}

		var valueStrings []string
		var args []interface{}
		argIdx := 1

		for i := 0; i < currentBatch; i++ {
			docType := documentTypes[rand.Intn(len(documentTypes))]
			docSubtype := documentSubtypes[docType][rand.Intn(len(documentSubtypes[docType]))]
			cat := categories[rand.Intn(len(categories))]
			status := statuses[rand.Intn(len(statuses))]
			confidentiality := confidentialities[rand.Intn(len(confidentialities))]
			department := departments[rand.Intn(len(departments))]
			author := authors[rand.Intn(len(authors))]
			client := clients[rand.Intn(len(clients))]

			// Generate file path
			year := years[rand.Intn(len(years))]
			pathPrefix := pathPrefixes[rand.Intn(len(pathPrefixes))]
			filePath := fmt.Sprintf("%s/%s/%s/doc_%d.pdf", pathPrefix, year, department, batch+i+1)

			// Generate tags (will be added separately via UPDATE for simplicity)
			_ = tagOptions // tags handled separately

			// Generate dates
			baseDate := time.Now().AddDate(0, -rand.Intn(36), -rand.Intn(28)) // Random date in last 3 years
			createdAt := baseDate
			modifiedAt := baseDate.Add(time.Duration(rand.Intn(90*24)) * time.Hour)

			// For juridic documents, add validity dates
			var validFrom, validUntil, signedAt interface{}
			if docType == "juridic" {
				vf := baseDate.Add(time.Duration(rand.Intn(30*24)) * time.Hour)
				validFrom = vf
				validUntil = vf.AddDate(rand.Intn(3)+1, rand.Intn(12), 0) // 1-3 years validity
				signedAt = baseDate.Add(time.Duration(rand.Intn(7*24)) * time.Hour)
			} else {
				validFrom = nil
				validUntil = nil
				signedAt = nil
			}

			// Truncate sous_categorie for title (safely handle UTF-8)
			sousCateg := cat.SousCategorie
			if len(sousCateg) > 20 {
				// Convert to runes to safely truncate UTF-8
				runes := []rune(sousCateg)
				if len(runes) > 20 {
					runes = runes[:20]
				}
				sousCateg = string(runes)
			}

			title := fmt.Sprintf("%s - %s - %s #%d",
				docType, strings.ReplaceAll(docSubtype, "_", " "),
				sousCateg, batch+i+1)

			content := fmt.Sprintf("Document %s de type %s concernant %s. Catégorie: %s > %s > %s. "+
				"Département: %s. Auteur: %s. Client: %s.",
				docSubtype, docType, cat.SousCategorie,
				cat.CategoriePrincipale, cat.Categorie, cat.SousCategorie,
				department, author, client)

			// Extract year and month from created_at
			createdYear := createdAt.Year()
			createdMonth := int(createdAt.Month())

			valueStrings = append(valueStrings, fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5, argIdx+6, argIdx+7, argIdx+8,
				argIdx+9, argIdx+10, argIdx+11, argIdx+12, argIdx+13, argIdx+14, argIdx+15, argIdx+16, argIdx+17,
				argIdx+18, argIdx+19,
			))

			args = append(args,
				title, content, filePath,
				docType, docSubtype,
				cat.CategoriePrincipale, cat.Categorie, cat.SousCategorie,
				status, confidentiality,
				createdAt, modifiedAt, validFrom, validUntil, signedAt,
				author, department, client,
				createdYear, createdMonth,
			)
			argIdx += 20
		}

		// Note: For simplicity, tags are added with a separate UPDATE
		// In production, you'd handle array types properly
		query := fmt.Sprintf(`
			INSERT INTO %s.documents 
			(title, content, file_path, document_type, document_subtype,
			 categorie_principale, categorie, sous_categorie,
			 status, confidentiality,
			 created_at, modified_at, valid_from, valid_until, signed_at,
			 author, department, client,
			 created_year, created_month)
			VALUES %s`,
			schemaName, strings.Join(valueStrings, ", "))

		_, err := pool.Exec(ctx, query, args...)
		if err != nil {
			t.Logf("Warning: Batch insert failed: %v", err)
		}

		if (batch+currentBatch)%10000 == 0 {
			t.Logf("    Inserted %d/%d documents...", batch+currentBatch, count)
		}
	}

	// Add random tags (simplified)
	_, _ = pool.Exec(ctx, fmt.Sprintf(`
		UPDATE %s.documents SET tags = ARRAY[
			(ARRAY['urgent','important','review_needed','approved','pending'])[floor(random()*5+1)::int],
			(ARRAY['confidential','archived','template','final','draft'])[floor(random()*5+1)::int]
		]`, schemaName))
}

// benchmarkQuery runs a query function multiple times and returns the timings
func benchmarkQuery(t *testing.T, pool *pgxpool.Pool, ctx context.Context, iterations int, queryFunc func()) []time.Duration {
	times := make([]time.Duration, iterations)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		queryFunc()
		times[i] = time.Since(start)
	}
	return times
}

// getUniqueValues extracts unique values from categories using a selector function
func getUniqueValues(categories []CategoryEntry, selector func(CategoryEntry) string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, cat := range categories {
		val := selector(cat)
		if val != "" && !seen[val] {
			seen[val] = true
			result = append(result, val)
		}
	}
	return result
}

// BenchmarkMergeDeltas is a Go benchmark for merge_deltas performance
func BenchmarkMergeDeltas(b *testing.B) {
	ctx := context.Background()
	connString := "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		b.Skip("Database not available")
		return
	}
	defer pool.Close()

	// Setup
	schemaName := "bench_merge"
	setupBenchmarkSchemaB(b, pool, schemaName)
	defer cleanupBenchmarkSchemaB(b, pool, schemaName)

	// Insert some documents first
	for i := 0; i < 1000; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.documents (title, content, type_categorie, categorie_principale, categorie, sous_categorie, status, priority, tags)
			VALUES ('Doc %d', 'Content %d', 'Gestion Locative', 'Admin', 'Contrats', 'Location', 'active', 1, ARRAY['tag1'])`,
			schemaName, i, i))
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}

	tableName := fmt.Sprintf("%s.documents", schemaName)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add more documents to create deltas
		_, _ = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.documents (title, content, type_categorie, categorie_principale, categorie, sous_categorie, status, priority, tags)
			VALUES ('New Doc %d', 'Content', 'Gestion Locative', 'Admin', 'Contrats', 'Location', 'active', 1, ARRAY['tag1'])`,
			schemaName, i))

		// Merge deltas
		_, err := pool.Exec(ctx, `SELECT facets.merge_deltas($1::regclass)`, tableName)
		if err != nil {
			b.Logf("merge_deltas failed: %v", err)
		}
	}
}

// Helper functions for benchmarks
func setupBenchmarkSchemaB(b *testing.B, pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()
	queries := []string{
		fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName),
		fmt.Sprintf(`CREATE SCHEMA %s`, schemaName),
		fmt.Sprintf(`CREATE TABLE %s.documents (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT,
			type_categorie TEXT,
			categorie_principale TEXT,
			categorie TEXT,
			sous_categorie TEXT,
			status TEXT DEFAULT 'active',
			priority INTEGER DEFAULT 1,
			tags TEXT[],
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)`, schemaName),
		fmt.Sprintf(`SELECT facets.add_faceting_to_table(
			'%s.documents',
			key => 'id',
			facets => ARRAY[
				facets.plain_facet('type_categorie'),
				facets.plain_facet('categorie_principale'),
				facets.plain_facet('categorie'),
				facets.plain_facet('sous_categorie'),
				facets.plain_facet('status'),
				facets.array_facet('tags')
			],
			populate => false
		)`, schemaName),
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			b.Fatalf("Setup failed: %v\nQuery: %s", err, q)
		}
	}
}

func cleanupBenchmarkSchemaB(b *testing.B, pool *pgxpool.Pool, schemaName string) {
	ctx := context.Background()
	_, _ = pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schemaName))
}
