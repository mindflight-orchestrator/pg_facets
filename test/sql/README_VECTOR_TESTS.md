# BM25 + Vector Hybrid Search Tests

This directory contains comprehensive tests for hybrid search combining BM25 full-text search with pgvector embeddings.

## Files

- **bm25_vector_hybrid_test.sql**: Main test suite with 25 documents covering various topics
- **generate_embeddings.sql**: SQL script for embedding generation (placeholder)
- **generate_embeddings.py**: Python script to generate real embeddings using ollama, sentence-transformers, or OpenAI

## Prerequisites

1. **PostgreSQL** with extensions:
   - `pg_facets` extension
   - `pgvector` extension (`CREATE EXTENSION vector;`)
   - `pg_roaringbitmap` extension

2. **Embedding Generation** (choose one):
   - **Ollama**: Install from https://ollama.ai, then pull an embedding model
   - **sentence-transformers**: Python library for local embedding generation
   - **OpenAI API**: Requires API key

## Quick Start

### 1. Run the test suite (with placeholder embeddings)

```bash
psql -U postgres -d postgres -f bm25_vector_hybrid_test.sql
```

This will run tests with placeholder embeddings (zeros), which tests the structure but not actual vector similarity.

### 2. Generate real embeddings

#### Option A: Using Ollama

```bash
# Install ollama (if not already installed)
# Pull an embedding model
ollama pull nomic-embed-text

# Generate embeddings
python generate_embeddings.py \
    --method ollama \
    --model nomic-embed-text \
    --url http://localhost:11434 \
    --db-url postgresql://postgres:postgres@localhost:5432/postgres
```

**Note**: `nomic-embed-text` produces 768-dimensional embeddings. Update the table schema if needed:
```sql
ALTER TABLE hybrid_test.documents ALTER COLUMN embedding TYPE vector(768);
```

#### Option B: Using sentence-transformers

```bash
# Install dependencies
pip install sentence-transformers psycopg2-binary

# Generate embeddings (384 dimensions)
python generate_embeddings.py \
    --method transformers \
    --model sentence-transformers/all-MiniLM-L6-v2 \
    --db-url postgresql://postgres:postgres@localhost:5432/postgres
```

#### Option C: Using OpenAI API

```bash
# Install dependencies
pip install openai psycopg2-binary

# Generate embeddings (1536 dimensions)
python generate_embeddings.py \
    --method openai \
    --model text-embedding-3-small \
    --api-key YOUR_OPENAI_API_KEY \
    --db-url postgresql://postgres:postgres@localhost:5432/postgres
```

**Note**: OpenAI embeddings are 1536-dimensional. Update the table schema:
```sql
ALTER TABLE hybrid_test.documents ALTER COLUMN embedding TYPE vector(1536);
```

### 3. Re-run tests with real embeddings

After generating embeddings, re-run the test suite:

```bash
psql -U postgres -d postgres -f bm25_vector_hybrid_test.sql
```

## Test Coverage

The test suite includes:

### Section 1: BM25 Only Tests (Baseline)
- Basic BM25 search without vector
- BM25 search with facets

### Section 2: Vector Search Tests
- Vector similarity search (requires real embeddings)
- Direct vector similarity queries

### Section 3: Hybrid BM25 + Vector Tests
- Hybrid search structure verification
- Hybrid search with facets
- Vector weight variations (0.2, 0.5, 0.8)
- Rescoring/sorting verification
- Hybrid search with facet filters
- Comparison: BM25-only vs Hybrid results

### Section 4: Edge Cases
- Empty query with vector
- NULL query with vector

## Understanding the Results

### With Placeholder Embeddings (Zeros)
- Tests will verify the **structure** of hybrid search works
- Vector scores will be 0.0 or invalid
- Combined scores will equal BM25 scores
- Tests will pass but won't demonstrate real vector search benefits

### With Real Embeddings
- Vector scores will reflect semantic similarity
- Combined scores will blend BM25 and vector scores
- Results may differ from BM25-only search
- Hybrid search can find semantically similar documents even if they don't match exact keywords

## Example: Testing Rescoring

The test suite verifies that results are correctly sorted by `combined_score`:

```sql
-- BM25 weight: 0.5, Vector weight: 0.5 (balanced)
combined_score = (bm25_score * 0.5) + (vector_score * 0.5)

-- BM25 weight: 0.8, Vector weight: 0.2 (BM25-focused)
combined_score = (bm25_score * 0.8) + (vector_score * 0.2)

-- BM25 weight: 0.2, Vector weight: 0.8 (vector-focused)
combined_score = (bm25_score * 0.2) + (vector_score * 0.8)
```

## Troubleshooting

### "pgvector extension is not installed"
```sql
CREATE EXTENSION vector;
```

### "Embedding dimension mismatch"
Make sure the `embedding` column dimension matches your model:
- `nomic-embed-text`: 768 dimensions
- `all-MiniLM-L6-v2`: 384 dimensions
- `text-embedding-3-small`: 1536 dimensions

Update the table if needed:
```sql
ALTER TABLE hybrid_test.documents ALTER COLUMN embedding TYPE vector(768);
```

### "No real embeddings found"
The test detects placeholder embeddings (all zeros). Generate real embeddings using one of the methods above.

### Ollama connection errors
- Ensure ollama is running: `ollama serve`
- Check the URL: default is `http://localhost:11434`
- Verify the model is pulled: `ollama list`

## Notes

1. **Current Implementation**: The `search_documents_with_facets` function currently has simplified vector support. Full hybrid search is available in `search_documents`.

2. **Embedding Storage**: Embeddings are stored in the database to avoid regenerating them each test run. This makes tests faster and more reproducible.

3. **Model Selection**: Choose an embedding model based on:
   - **Dimension**: Affects storage and index size
   - **Quality**: Better models produce more accurate semantic similarity
   - **Speed**: Local models (ollama/transformers) vs API (OpenAI)

4. **Performance**: With real embeddings and proper indexes, hybrid search can be very fast. Consider creating a vector index:
   ```sql
   CREATE INDEX ON hybrid_test.documents USING ivfflat (embedding vector_cosine_ops);
   ```

## Next Steps

1. Generate embeddings for your test data
2. Run the full test suite
3. Experiment with different vector weights to find optimal balance
4. Add more test documents relevant to your use case
5. Benchmark performance with and without vector search
