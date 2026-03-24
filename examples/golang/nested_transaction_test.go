// nested_transaction_test.go
// Tests that pg_facets operations are safe when running inside nested transactions (DO blocks)
// where savepoint creation fails, ensuring ACID properties are maintained
//
// IMPORTANT: You MUST use ./run_tests.sh to run these tests!
// The script starts a Docker container with PostgreSQL and pg_facets extension.

package pgfaceting

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestNestedTransactionSafety tests ACID compliance inside nested transactions
// where savepoint creation fails (simulating DO blocks)
func TestNestedTransactionSafety(t *testing.T) {
	pool := getTestPool(t)
	if pool == nil {
		t.Skip("No database connection available")
		return
	}
	defer pool.Close()

	// Setup test schema
	setupNestedTransactionTestSchema(t, pool)
	defer cleanupNestedTransactionTestSchema(t, pool)

	t.Run("BM25IndexingAtomicity", func(t *testing.T) { testBM25IndexingAtomicity(t, pool) })
	t.Run("BM25RollbackPropagation", func(t *testing.T) { testBM25RollbackPropagation(t, pool) })
	t.Run("FacetDeltaMergeAtomicity", func(t *testing.T) { testFacetDeltaMergeAtomicity(t, pool) })
	t.Run("FacetDeltaMergeRollback", func(t *testing.T) { testFacetDeltaMergeRollback(t, pool) })
	t.Run("ErrorRecoveryAndConsistency", func(t *testing.T) { testErrorRecoveryAndConsistency(t, pool) })
	t.Run("MultipleDocsBatchWithError", func(t *testing.T) { testMultipleDocsBatchWithError(t, pool) })
}

// testBM25IndexingAtomicity tests that BM25 indexing is atomic inside nested transactions
func testBM25IndexingAtomicity(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Get initial counts
	var initialDocs, initialTerms int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&initialDocs)
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_index
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&initialTerms)

	t.Logf("Initial state: docs=%d, terms=%d", initialDocs, initialTerms)

	// Test indexing inside a DO block (simulates nested transaction)
	_, err := pool.Exec(ctx, `
		DO $$
		DECLARE
			v_final_docs int;
			v_final_terms int;
		BEGIN
			-- Insert document
			INSERT INTO test_nested.bm25_atomicity_go VALUES (1, 'The quick brown fox jumps over the lazy dog');

			-- Index document (this will attempt savepoint and log warning)
			PERFORM facets.bm25_index_document(
				'test_nested.bm25_atomicity_go'::regclass,
				1,
				'The quick brown fox jumps over the lazy dog',
				'content',
				'english'
			);

			-- Verify indexing worked atomically
			SELECT COUNT(*) INTO v_final_docs FROM facets.bm25_documents
			WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid;

			SELECT COUNT(*) INTO v_final_terms FROM facets.bm25_index
			WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid;

			-- Either all terms are indexed or none (atomicity)
			IF v_final_docs != 1 OR v_final_terms <= 0 THEN
				RAISE EXCEPTION 'Atomicity violation: docs=%, terms=%', v_final_docs, v_final_terms;
			END IF;
		END $$;
	`)

	if err != nil {
		t.Fatalf("BM25 indexing inside DO block failed: %v", err)
	}

	// Verify final state outside DO block
	var finalDocs, finalTerms int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&finalDocs)
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_index
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&finalTerms)

	if finalDocs != initialDocs+1 {
		t.Errorf("Expected %d docs, got %d", initialDocs+1, finalDocs)
	}
	if finalTerms <= initialTerms {
		t.Errorf("Expected more than %d terms, got %d", initialTerms, finalTerms)
	}

	t.Logf("SUCCESS: BM25 indexing atomic inside nested transaction. Final: docs=%d, terms=%d", finalDocs, finalTerms)
}

// testBM25RollbackPropagation tests that BM25 indexing rolls back properly with DO block
func testBM25RollbackPropagation(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Get baseline
	var baselineDocs int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&baselineDocs)

	// Execute DO block that indexes then rolls back
	_, err := pool.Exec(ctx, `
		DO $$
		BEGIN
			-- Insert and index document
			INSERT INTO test_nested.bm25_atomicity_go VALUES (2, 'This document will be rolled back');
			PERFORM facets.bm25_index_document(
				'test_nested.bm25_atomicity_go'::regclass,
				2,
				'This document will be rolled back',
				'content',
				'english'
			);

			-- Force rollback
			RAISE EXCEPTION 'Intentional rollback test';
		END $$;
	`)

	// The error is expected - it's the rollback test
	if err == nil {
		t.Fatal("Expected DO block to fail with rollback")
	}

	// Verify rollback worked
	var afterRollbackDocs int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&afterRollbackDocs)

	if afterRollbackDocs != baselineDocs {
		t.Errorf("Rollback failed: expected %d docs, got %d", baselineDocs, afterRollbackDocs)
	}

	t.Logf("SUCCESS: BM25 rollback propagation worked. Docs after rollback: %d", afterRollbackDocs)
}

// testFacetDeltaMergeAtomicity tests facet delta merging inside nested transactions
func testFacetDeltaMergeAtomicity(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Insert data to create deltas
	_, err := pool.Exec(ctx, `
		INSERT INTO test_nested.facet_atomicity_go VALUES
			(1, 'A', 'active'),
			(2, 'B', 'inactive'),
			(3, 'A', 'active');
	`)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Count deltas
	var deltaCount int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_nested.facet_atomicity_go_facets_deltas WHERE delta <> 0
	`).Scan(&deltaCount)

	if deltaCount == 0 {
		t.Skip("No deltas created - triggers may not be working")
	}

	// Merge deltas inside DO block
	_, err = pool.Exec(ctx, `
		DO $$
		DECLARE
			v_remaining int;
		BEGIN
			-- Merge deltas (may produce savepoint warnings)
			PERFORM facets.merge_deltas_safe('test_nested.facet_atomicity_go'::regclass);

			-- Verify no deltas remain
			SELECT COUNT(*) INTO v_remaining
			FROM test_nested.facet_atomicity_go_facets_deltas WHERE delta <> 0;

			IF v_remaining > 0 THEN
				RAISE EXCEPTION 'Delta merge not atomic: % deltas remain', v_remaining;
			END IF;
		END $$;
	`)

	if err != nil {
		t.Fatalf("Delta merge inside DO block failed: %v", err)
	}

	// Verify outside DO block
	var remainingDeltas int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_nested.facet_atomicity_go_facets_deltas WHERE delta <> 0
	`).Scan(&remainingDeltas)

	if remainingDeltas > 0 {
		t.Errorf("Delta merge not atomic: %d deltas remain after merge", remainingDeltas)
	}

	t.Logf("SUCCESS: Delta merge atomic inside nested transaction. Remaining deltas: %d", remainingDeltas)
}

// testFacetDeltaMergeRollback tests delta merge rollback from DO block
func testFacetDeltaMergeRollback(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Get baseline
	var baselineFacets int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_nested.facet_atomicity_go_facets
	`).Scan(&baselineFacets)

	// Execute DO block that merges then rolls back
	_, err := pool.Exec(ctx, `
		DO $$
		BEGIN
			-- Insert more data and merge
			INSERT INTO test_nested.facet_atomicity_go VALUES (4, 'C', 'pending');
			PERFORM facets.merge_deltas_safe('test_nested.facet_atomicity_go'::regclass);

			-- Force rollback
			RAISE EXCEPTION 'Intentional delta merge rollback test';
		END $$;
	`)

	// Error expected
	if err == nil {
		t.Fatal("Expected DO block to fail with rollback")
	}

	// Verify rollback worked
	var afterRollbackFacets int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_nested.facet_atomicity_go_facets
	`).Scan(&afterRollbackFacets)

	// Should be same as baseline (merge was rolled back)
	if afterRollbackFacets != baselineFacets {
		t.Errorf("Delta merge rollback failed: expected %d facets, got %d", baselineFacets, afterRollbackFacets)
	}

	t.Logf("SUCCESS: Delta merge rollback worked. Facets after rollback: %d", afterRollbackFacets)
}

// testErrorRecoveryAndConsistency tests database consistency after errors
func testErrorRecoveryAndConsistency(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Get baseline
	var baselineDocs int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&baselineDocs)

	// Execute DO block with error mid-batch
	_, err := pool.Exec(ctx, `
		DO $$
		BEGIN
			-- Insert multiple documents
			INSERT INTO test_nested.bm25_atomicity_go VALUES
				(10, 'First document'),
				(11, 'Second document'),
				(12, 'Third document - will cause error');

			-- Index first two
			PERFORM facets.bm25_index_document('test_nested.bm25_atomicity_go'::regclass, 10, 'First document', 'content', 'english');
			PERFORM facets.bm25_index_document('test_nested.bm25_atomicity_go'::regclass, 11, 'Second document', 'content', 'english');

			-- Third one with NULL content (should fail)
			PERFORM facets.bm25_index_document('test_nested.bm25_atomicity_go'::regclass, 12, NULL, 'content', 'english');

			-- Force rollback of entire batch
			RAISE EXCEPTION 'Rolling back entire batch due to error';
		END $$;
	`)

	// Error expected
	if err == nil {
		t.Fatal("Expected DO block to fail")
	}

	// Verify no partial indexing occurred
	var afterErrorDocs int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM facets.bm25_documents
		WHERE table_id = 'test_nested.bm25_atomicity_go'::regclass::oid
	`).Scan(&afterErrorDocs)

	if afterErrorDocs != baselineDocs {
		t.Errorf("Error recovery failed: expected %d docs, got %d (partial indexing occurred)", baselineDocs, afterErrorDocs)
	}

	t.Logf("SUCCESS: Error recovery maintained consistency. Docs after error: %d", afterErrorDocs)
}

// testMultipleDocsBatchWithError tests batch operations with mid-batch errors
func testMultipleDocsBatchWithError(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Execute batch that fails midway
	_, err := pool.Exec(ctx, `
		DO $$
		BEGIN
			-- Batch insert and index
			INSERT INTO test_nested.bm25_atomicity_go VALUES
				(20, 'Batch document 1'),
				(21, 'Batch document 2'),
				(22, 'Batch document 3');

			-- Index successfully
			PERFORM facets.bm25_index_document('test_nested.bm25_atomicity_go'::regclass, 20, 'Batch document 1', 'content', 'english');
			PERFORM facets.bm25_index_document('test_nested.bm25_atomicity_go'::regclass, 21, 'Batch document 2', 'content', 'english');

			-- Simulate mid-batch failure
			PERFORM 1/0;  -- Division by zero

			-- This should not execute
			PERFORM facets.bm25_index_document('test_nested.bm25_atomicity_go'::regclass, 22, 'Batch document 3', 'content', 'english');
		END $$;
	`)

	// Error expected
	if err == nil {
		t.Fatal("Expected batch operation to fail")
	}

	// Verify database remains functional - insert and index one more document
	_, err = pool.Exec(ctx, `
		INSERT INTO test_nested.bm25_atomicity_go VALUES (100, 'Recovery document');
	`)
	if err != nil {
		t.Fatalf("Database not functional after error: %v", err)
	}

	_, err = pool.Exec(ctx, `
		SELECT facets.bm25_index_document_safe(
			'test_nested.bm25_atomicity_go'::regclass,
			100,
			'Recovery document',
			'content',
			'english'
		);
	`)
	if err != nil {
		t.Fatalf("BM25 indexing not functional after error: %v", err)
	}

	t.Logf("SUCCESS: Database remains functional after nested transaction errors")
}

// Helper functions

func setupNestedTransactionTestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Clean up any existing test schema
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS test_nested CASCADE")

	// Create test schema
	_, err := pool.Exec(ctx, "CREATE SCHEMA test_nested")
	if err != nil {
		t.Fatalf("Failed to create test schema: %v", err)
	}

	// Create BM25 test table
	_, err = pool.Exec(ctx, `
		CREATE TABLE test_nested.bm25_atomicity_go (
			id bigint PRIMARY KEY,
			content text
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create BM25 test table: %v", err)
	}

	// Setup BM25 for the table
	_, err = pool.Exec(ctx, `
		SELECT facets.add_faceting_to_table(
			'test_nested.bm25_atomicity_go'::regclass,
			key => 'id',
			facets => ARRAY[]::facets.facet_definition[]
		);

		SELECT facets.bm25_set_language('test_nested.bm25_atomicity_go'::regclass, 'english');
	`)
	if err != nil {
		t.Fatalf("Failed to setup BM25: %v", err)
	}

	// Create facet test table
	_, err = pool.Exec(ctx, `
		CREATE TABLE test_nested.facet_atomicity_go (
			id bigint PRIMARY KEY,
			category text,
			status text
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create facet test table: %v", err)
	}

	// Setup faceting for the table
	_, err = pool.Exec(ctx, `
		SELECT facets.add_faceting_to_table(
			'test_nested.facet_atomicity_go'::regclass,
			key => 'id',
			facets => ARRAY[
				facets.plain_facet('category'),
				facets.plain_facet('status')
			],
			populate => true
		);
	`)
	if err != nil {
		t.Fatalf("Failed to setup faceting: %v", err)
	}
}

func cleanupNestedTransactionTestSchema(t *testing.T, pool *pgxpool.Pool) {
	ctx := context.Background()

	// Clean up test schema - ignore errors as tables may not exist
	_, _ = pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS test_nested CASCADE;

		-- Clean up any orphaned facet tables
		DO $$
		DECLARE
			r RECORD;
		BEGIN
			FOR r IN
				SELECT schemaname, tablename
				FROM pg_tables
				WHERE schemaname LIKE 'test_nested%'
			LOOP
				EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', r.schemaname, r.tablename);
			END LOOP;
		END $$;
	`)
}

