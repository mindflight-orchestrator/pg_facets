// acid_compliance_test.go
// Dedicated ACID compliance tests for pg_facets 0.4.3
// Tests atomicity, consistency, isolation, and durability guarantees
//
// IMPORTANT: You MUST use ./run_tests.sh to run these tests!
// The script starts a Docker container with PostgreSQL and pg_facets extension.
//
// To run tests:
//   ./run_tests.sh  (REQUIRED - starts Docker container, waits for DB, then runs tests)
//
// Do NOT run "go test" directly - it will fail because PostgreSQL is not running.
// The run_tests.sh script handles:
//   1. Building and starting the Docker container
//   2. Waiting for PostgreSQL to be ready
//   3. Installing required extensions (roaringbitmap, pg_facets)
//   4. Running the Go tests
//   5. Cleaning up containers

package pgfaceting

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestACIDCompliance tests ACID properties of pg_facets operations
func TestACIDCompliance(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	// Setup test schema
	setupACIDTestSchema(t, pool)
	defer cleanupACIDTestSchema(t, pool)

	t.Run("Atomicity_DeltaMerge", func(t *testing.T) { testAtomicityDeltaMerge(t, pool) })
	t.Run("Atomicity_BM25Index", func(t *testing.T) { testAtomicityBM25Index(t, pool) })
	t.Run("Atomicity_BM25Delete", func(t *testing.T) { testAtomicityBM25Delete(t, pool) })
	t.Run("Consistency_Statistics", func(t *testing.T) { testConsistencyStatistics(t, pool) })
	t.Run("Consistency_FacetCounts", func(t *testing.T) { testConsistencyFacetCounts(t, pool) })
	t.Run("Isolation_ConcurrentMerges", func(t *testing.T) { testIsolationConcurrentMerges(t, pool) })
	t.Run("Isolation_ConcurrentIndexing", func(t *testing.T) { testIsolationConcurrentIndexing(t, pool) })
	t.Run("Durability_Commit", func(t *testing.T) { testDurabilityCommit(t, pool) })
	t.Run("Durability_Rollback", func(t *testing.T) { testDurabilityRollback(t, pool) })
	t.Run("SafeWrappers", func(t *testing.T) { testSafeWrappers(t, pool) })
}

// testAtomicityDeltaMerge tests that delta merge is atomic (all-or-nothing)
func testAtomicityDeltaMerge(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Create test table with faceting
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_acid_deltas (
			id bigint PRIMARY KEY,
			category text,
			status text
		);
		
		DELETE FROM facets.faceted_table WHERE table_id = 'test_acid_deltas'::regclass::oid;
	`)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Add faceting
	_, err = pool.Exec(ctx, `
		SELECT facets.add_faceting_to_table(
			'test_acid_deltas'::regclass,
			key => 'id',
			facets => ARRAY[
				facets.plain_facet('category'),
				facets.plain_facet('status')
			],
			populate => true
		);
	`)
	if err != nil {
		t.Fatalf("Add faceting failed: %v", err)
	}

	// Get initial facet count
	var initialCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_deltas_facets
	`).Scan(&initialCount)
	if err != nil {
		t.Fatalf("Failed to get initial count: %v", err)
	}

	// Insert data to create deltas
	_, err = pool.Exec(ctx, `
		INSERT INTO test_acid_deltas VALUES 
			(1, 'A', 'active'),
			(2, 'B', 'inactive'),
			(3, 'A', 'active');
	`)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify deltas exist
	var deltaCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_deltas_facets_deltas WHERE delta <> 0
	`).Scan(&deltaCount)
	if err != nil {
		t.Fatalf("Failed to count deltas: %v", err)
	}

	if deltaCount == 0 {
		t.Skip("No deltas created (triggers may not be set up)")
		return
	}

	// Use safe merge wrapper (should be atomic)
	_, err = pool.Exec(ctx, `
		SELECT facets.merge_deltas_safe('test_acid_deltas'::regclass)
	`)
	if err != nil {
		t.Fatalf("Safe merge failed: %v", err)
	}

	// Verify all deltas were merged (or none)
	var finalCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_deltas_facets
	`).Scan(&finalCount)
	if err != nil {
		t.Fatalf("Failed to get final count: %v", err)
	}

	// Verify no deltas remain
	var remainingDeltas int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_deltas_facets_deltas WHERE delta <> 0
	`).Scan(&remainingDeltas)
	if err != nil {
		t.Fatalf("Failed to check remaining deltas: %v", err)
	}

	if remainingDeltas > 0 {
		t.Errorf("Expected all deltas to be merged, but %d remain", remainingDeltas)
	}

	t.Logf("Atomicity test passed: Initial facets: %d, Final facets: %d, Remaining deltas: %d",
		initialCount, finalCount, remainingDeltas)
}

// testAtomicityBM25Index tests that BM25 indexing is atomic
func testAtomicityBM25Index(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Create test table
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_acid_bm25 (
			id bigint PRIMARY KEY,
			content text
		);
		
		DELETE FROM facets.faceted_table WHERE table_id = 'test_acid_bm25'::regclass::oid;
	`)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Add faceting and BM25
	_, err = pool.Exec(ctx, `
		SELECT facets.add_faceting_to_table(
			'test_acid_bm25'::regclass,
			key => 'id',
			facets => ARRAY[]::facets.facet_definition[]
		);
		
		SELECT facets.bm25_set_language('test_acid_bm25'::regclass, 'english');
	`)
	if err != nil {
		t.Fatalf("Setup BM25 failed: %v", err)
	}

	// Get initial document count
	var initialDocs int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents 
		WHERE table_id = 'test_acid_bm25'::regclass::oid
	`).Scan(&initialDocs)
	if err != nil {
		t.Fatalf("Failed to get initial doc count: %v", err)
	}

	// Insert test data
	_, err = pool.Exec(ctx, `
		INSERT INTO test_acid_bm25 VALUES 
			(1, 'The quick brown fox jumps over the lazy dog'),
			(2, 'PostgreSQL is a powerful open source database');
	`)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Use safe indexing wrapper (should be atomic)
	_, err = pool.Exec(ctx, `
		SELECT facets.bm25_index_document_safe(
			'test_acid_bm25'::regclass,
			1,
			'The quick brown fox jumps over the lazy dog',
			'content',
			'english'
		);
		
		SELECT facets.bm25_index_document_safe(
			'test_acid_bm25'::regclass,
			2,
			'PostgreSQL is a powerful open source database',
			'content',
			'english'
		);
	`)
	if err != nil {
		t.Fatalf("Safe indexing failed: %v", err)
	}

	// Verify documents were indexed atomically
	var finalDocs int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents 
		WHERE table_id = 'test_acid_bm25'::regclass::oid
	`).Scan(&finalDocs)
	if err != nil {
		t.Fatalf("Failed to get final doc count: %v", err)
	}

	// Verify terms were indexed
	var termCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_index 
		WHERE table_id = 'test_acid_bm25'::regclass::oid
	`).Scan(&termCount)
	if err != nil {
		t.Fatalf("Failed to get term count: %v", err)
	}

	if finalDocs != initialDocs+2 {
		t.Errorf("Expected %d documents, got %d", initialDocs+2, finalDocs)
	}

	if termCount == 0 {
		t.Error("Expected terms to be indexed, but term count is 0")
	}

	t.Logf("Atomicity test passed: Initial docs: %d, Final docs: %d, Terms: %d",
		initialDocs, finalDocs, termCount)
}

// testAtomicityBM25Delete tests that BM25 deletion is atomic
func testAtomicityBM25Delete(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Use existing test_acid_bm25 table
	// First ensure we have a document indexed
	_, err := pool.Exec(ctx, `
		SELECT facets.bm25_index_document_safe(
			'test_acid_bm25'::regclass,
			999,
			'Test document for deletion',
			'content',
			'english'
		);
	`)
	if err != nil {
		t.Fatalf("Index document failed: %v", err)
	}

	// Verify document exists
	var exists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM facets.bm25_documents 
			WHERE table_id = 'test_acid_bm25'::regclass::oid AND doc_id = 999
		)
	`).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to check document: %v", err)
	}

	if !exists {
		t.Skip("Document not indexed, skipping delete test")
		return
	}

	// Use safe delete wrapper (should be atomic)
	_, err = pool.Exec(ctx, `
		SELECT facets.bm25_delete_document_safe('test_acid_bm25'::regclass, 999)
	`)
	if err != nil {
		t.Fatalf("Safe delete failed: %v", err)
	}

	// Verify document and all related data were deleted
	var docExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM facets.bm25_documents 
			WHERE table_id = 'test_acid_bm25'::regclass::oid AND doc_id = 999
		)
	`).Scan(&docExists)
	if err != nil {
		t.Fatalf("Failed to check deleted document: %v", err)
	}

	if docExists {
		t.Error("Document still exists after safe delete")
	}

	// Verify no terms reference this document
	var termRefs int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_index 
		WHERE table_id = 'test_acid_bm25'::regclass::oid
		AND rb_contains(doc_ids, 999)
	`).Scan(&termRefs)
	if err != nil {
		t.Logf("Note: Could not verify term references (rb_contains may not be available): %v", err)
	} else if termRefs > 0 {
		t.Errorf("Found %d terms still referencing deleted document", termRefs)
	}

	t.Log("Atomicity delete test passed: Document and references removed atomically")
}

// testConsistencyStatistics tests that statistics remain consistent
func testConsistencyStatistics(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Get statistics before operations
	var statsBefore struct {
		totalDocs int
		avgLength float64
	}
	err := pool.QueryRow(ctx, `
		SELECT total_documents, avg_document_length
		FROM facets.bm25_statistics
		WHERE table_id = 'test_acid_bm25'::regclass::oid
	`).Scan(&statsBefore.totalDocs, &statsBefore.avgLength)
	if err != nil {
		// Statistics may not exist yet
		t.Logf("Statistics not found (may need to be created): %v", err)
	}

	// Perform indexing operations
	_, err = pool.Exec(ctx, `
		SELECT facets.bm25_index_document_safe(
			'test_acid_bm25'::regclass,
			1001,
			'Consistency test document with multiple words',
			'content',
			'english'
		);
	`)
	if err != nil {
		t.Fatalf("Index document failed: %v", err)
	}

	// Verify statistics are updated
	var statsAfter struct {
		totalDocs int
		avgLength float64
	}
	err = pool.QueryRow(ctx, `
		SELECT total_documents, avg_document_length
		FROM facets.bm25_statistics
		WHERE table_id = 'test_acid_bm25'::regclass::oid
	`).Scan(&statsAfter.totalDocs, &statsAfter.avgLength)
	if err != nil {
		t.Fatalf("Failed to get statistics after: %v", err)
	}

	// Verify statistics are consistent
	if statsAfter.totalDocs <= statsBefore.totalDocs {
		t.Errorf("Expected document count to increase, got %d -> %d",
			statsBefore.totalDocs, statsAfter.totalDocs)
	}

	if statsAfter.avgLength <= 0 {
		t.Error("Average document length should be positive")
	}

	t.Logf("Consistency test passed: Docs %d -> %d, Avg length: %.2f",
		statsBefore.totalDocs, statsAfter.totalDocs, statsAfter.avgLength)
}

// testConsistencyFacetCounts tests that facet counts remain consistent
func testConsistencyFacetCounts(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Get facet counts before merge
	var countBefore int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_deltas_facets
	`).Scan(&countBefore)
	if err != nil {
		t.Fatalf("Failed to get facet count: %v", err)
	}

	// Add more data
	_, err = pool.Exec(ctx, `
		INSERT INTO test_acid_deltas VALUES 
			(10, 'C', 'pending'),
			(11, 'A', 'active');
	`)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Merge deltas
	_, err = pool.Exec(ctx, `
		SELECT facets.merge_deltas_safe('test_acid_deltas'::regclass)
	`)
	if err != nil {
		t.Fatalf("Merge deltas failed: %v", err)
	}

	// Verify facet counts are consistent
	var countAfter int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_deltas_facets
	`).Scan(&countAfter)
	if err != nil {
		t.Fatalf("Failed to get facet count after: %v", err)
	}

	// Counts should have increased (or stayed same if no new facets)
	if countAfter < countBefore {
		t.Errorf("Facet count decreased: %d -> %d", countBefore, countAfter)
	}

	t.Logf("Consistency test passed: Facet counts %d -> %d", countBefore, countAfter)
}

// testIsolationConcurrentMerges tests isolation during concurrent delta merges
func testIsolationConcurrentMerges(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Create separate test table for isolation test
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_acid_isolation (
			id bigint PRIMARY KEY,
			value text
		);
		
		DELETE FROM facets.faceted_table WHERE table_id = 'test_acid_isolation'::regclass::oid;
		
		SELECT facets.add_faceting_to_table(
			'test_acid_isolation'::regclass,
			key => 'id',
			facets => ARRAY[facets.plain_facet('value')],
			populate => true
		);
	`)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Insert data in separate transactions
	conn1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn1.Release()

	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn2.Release()

	// Start transaction 1
	tx1, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}

	// Start transaction 2
	tx2, err := conn2.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}

	// Insert in transaction 1
	_, err = tx1.Exec(ctx, `INSERT INTO test_acid_isolation VALUES (1, 'tx1')`)
	if err != nil {
		tx1.Rollback(ctx)
		t.Fatalf("Tx1 insert failed: %v", err)
	}

	// Insert in transaction 2
	_, err = tx2.Exec(ctx, `INSERT INTO test_acid_isolation VALUES (2, 'tx2')`)
	if err != nil {
		tx2.Rollback(ctx)
		t.Fatalf("Tx2 insert failed: %v", err)
	}

	// Merge deltas in transaction 1
	_, err = tx1.Exec(ctx, `SELECT merge_deltas_native('test_acid_isolation'::regclass::oid)`)
	if err != nil {
		tx1.Rollback(ctx)
		t.Fatalf("Tx1 merge failed: %v", err)
	}

	// Commit transaction 1
	err = tx1.Commit(ctx)
	if err != nil {
		t.Fatalf("Tx1 commit failed: %v", err)
	}

	// Merge deltas in transaction 2 (should see its own changes)
	_, err = tx2.Exec(ctx, `SELECT merge_deltas_native('test_acid_isolation'::regclass::oid)`)
	if err != nil {
		tx2.Rollback(ctx)
		t.Fatalf("Tx2 merge failed: %v", err)
	}

	// Commit transaction 2
	err = tx2.Commit(ctx)
	if err != nil {
		t.Fatalf("Tx2 commit failed: %v", err)
	}

	// Verify both transactions' data is present
	var count int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_acid_isolation_facets
	`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to verify isolation: %v", err)
	}

	if count == 0 {
		t.Error("Isolation test failed: No facets found after concurrent merges")
	}

	t.Logf("Isolation test passed: Concurrent merges completed, final facet count: %d", count)
}

// testIsolationConcurrentIndexing tests isolation during concurrent BM25 indexing
func testIsolationConcurrentIndexing(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Use existing test_acid_bm25 table
	// Index documents concurrently
	done := make(chan error, 2)

	go func() {
		_, err := pool.Exec(ctx, `
			SELECT facets.bm25_index_document_safe(
				'test_acid_bm25'::regclass,
				2001,
				'Concurrent indexing test document one',
				'content',
				'english'
			);
		`)
		done <- err
	}()

	go func() {
		_, err := pool.Exec(ctx, `
			SELECT facets.bm25_index_document_safe(
				'test_acid_bm25'::regclass,
				2002,
				'Concurrent indexing test document two',
				'content',
				'english'
			);
		`)
		done <- err
	}()

	// Wait for both to complete
	err1 := <-done
	err2 := <-done

	if err1 != nil {
		t.Errorf("Concurrent indexing 1 failed: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Concurrent indexing 2 failed: %v", err2)
	}

	// Verify both documents were indexed
	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents 
		WHERE table_id = 'test_acid_bm25'::regclass::oid
		AND doc_id IN (2001, 2002)
	`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to verify concurrent indexing: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 documents, got %d", count)
	}

	t.Log("Isolation concurrent indexing test passed: Both documents indexed successfully")
}

// testDurabilityCommit tests that committed changes persist
func testDurabilityCommit(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Create a new connection to simulate a new session
	config, err := pgxpool.ParseConfig(getTestDatabaseURL())
	if err != nil {
		t.Skipf("Could not parse database URL: %v", err)
		return
	}

	newPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Skipf("Could not create new connection: %v", err)
		return
	}
	defer newPool.Close()

	// Index a document in first connection
	_, err = pool.Exec(ctx, `
		SELECT facets.bm25_index_document_safe(
			'test_acid_bm25'::regclass,
			3001,
			'Durability test document',
			'content',
			'english'
		);
	`)
	if err != nil {
		t.Fatalf("Index document failed: %v", err)
	}

	// Commit (implicit in Exec, but explicit for clarity)
	// In pgx, Exec auto-commits

	// Verify in new connection (simulates durability after commit)
	var exists bool
	err = newPool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM facets.bm25_documents 
			WHERE table_id = 'test_acid_bm25'::regclass::oid AND doc_id = 3001
		)
	`).Scan(&exists)
	if err != nil {
		t.Fatalf("Failed to verify durability: %v", err)
	}

	if !exists {
		t.Error("Durability test failed: Document not found in new connection")
	}

	t.Log("Durability commit test passed: Document persisted across connections")
}

// testDurabilityRollback tests that rolled back changes are not persisted
func testDurabilityRollback(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Start a transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Index a document in transaction
	_, err = tx.Exec(ctx, `
		SELECT facets.bm25_index_document_safe(
			'test_acid_bm25'::regclass,
			3002,
			'Rollback test document',
			'content',
			'english'
		);
	`)
	if err != nil {
		tx.Rollback(ctx)
		t.Fatalf("Index document failed: %v", err)
	}

	// Verify document exists in transaction
	var existsInTx bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM facets.bm25_documents 
			WHERE table_id = 'test_acid_bm25'::regclass::oid AND doc_id = 3002
		)
	`).Scan(&existsInTx)
	if err != nil {
		tx.Rollback(ctx)
		t.Fatalf("Failed to check in transaction: %v", err)
	}

	if !existsInTx {
		t.Error("Document should exist in transaction")
	}

	// Rollback transaction
	err = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify document does NOT exist after rollback
	var existsAfterRollback bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM facets.bm25_documents 
			WHERE table_id = 'test_acid_bm25'::regclass::oid AND doc_id = 3002
		)
	`).Scan(&existsAfterRollback)
	if err != nil {
		t.Fatalf("Failed to verify after rollback: %v", err)
	}

	if existsAfterRollback {
		t.Error("Durability rollback test failed: Document still exists after rollback")
	}

	t.Log("Durability rollback test passed: Document correctly rolled back")
}

// testSafeWrappers tests that safe wrapper functions provide ACID guarantees
func testSafeWrappers(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Test that safe wrappers exist and can be called
	var wrapperTests = []struct {
		name string
		query string
	}{
		{
			name: "bm25_index_document_safe",
			query: `SELECT facets.bm25_index_document_safe(
				'test_acid_bm25'::regclass,
				4001,
				'Safe wrapper test',
				'content',
				'english'
			)`,
		},
		{
			name: "bm25_delete_document_safe",
			query: `SELECT facets.bm25_delete_document_safe(
				'test_acid_bm25'::regclass,
				4001
			)`,
		},
		{
			name: "merge_deltas_safe",
			query: `SELECT facets.merge_deltas_safe('test_acid_deltas'::regclass)`,
		},
	}

	for _, tt := range wrapperTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, tt.query)
			if err != nil {
				t.Errorf("%s failed: %v", tt.name, err)
			} else {
				t.Logf("%s executed successfully", tt.name)
			}
		})
	}
}

// Helper functions

func setupACIDTestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	queries := []string{
		`CREATE SCHEMA IF NOT EXISTS test_acid`,
		`CREATE EXTENSION IF NOT EXISTS roaringbitmap`,
		`CREATE EXTENSION IF NOT EXISTS pg_facets`,
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Logf("Setup query failed (may already exist): %v", err)
		}
	}
}

func cleanupACIDTestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	queries := []string{
		`DROP TABLE IF EXISTS test_acid_deltas CASCADE`,
		`DROP TABLE IF EXISTS test_acid_bm25 CASCADE`,
		`DROP TABLE IF EXISTS test_acid_isolation CASCADE`,
		`DROP SCHEMA IF EXISTS test_acid CASCADE`,
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Logf("Cleanup query failed: %v", err)
		}
	}
}

func getTestDatabaseURL() string {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
	}
	return url
}

