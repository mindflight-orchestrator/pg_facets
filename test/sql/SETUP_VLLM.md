# Setting Up vLLM-server for Embedding Generation

## Current Status

✅ Test table `hybrid_test.documents` has been created with 25 documents
✅ BM25 indexing is complete
✅ Placeholder embeddings (zeros) are in place
⏳ Real embeddings need to be generated using vLLM-server

## Quick Start

### 1. Ensure vLLM-server is Running

Check if vLLM-server is accessible:
```bash
curl http://localhost:8000/v1/models
```

If it's on a different port or host, set the environment variable:
```bash
export VLLM_URL=http://your-server:port
```

### 2. Generate Embeddings

#### Option A: Using the helper script (recommended)
```bash
cd extensions/pg_facets/test/sql
./generate_embeddings_vllm.sh
```

The script will:
- Auto-detect vLLM-server
- List available models
- Generate embeddings for all 25 documents
- Save them to the database

#### Option B: Using Python script directly
```bash
cd extensions/pg_facets/test/sql
python3 generate_embeddings.py \
    --method vllm \
    --model your-model-name \
    --url http://localhost:8000 \
    --db-url postgresql://postgres:postgres@localhost:5432/postgres
```

### 3. Verify Embeddings

Check that real embeddings were generated:
```sql
-- In Docker container
docker exec pg_facets psql -U postgres -d postgres -c "
SELECT 
    COUNT(*) as total_docs,
    COUNT(embedding) as docs_with_embeddings,
    CASE 
        WHEN embedding = (SELECT array_agg(0.0::real)::vector(384) FROM generate_series(1, 384)) 
        THEN 'placeholder' 
        ELSE 'real' 
    END as type
FROM hybrid_test.documents
GROUP BY type;
"
```

### 4. Re-run Tests

Once embeddings are generated, re-run the test suite:
```bash
docker cp extensions/pg_facets/test/sql/bm25_vector_hybrid_test.sql pg_facets:/tmp/
docker exec pg_facets psql -U postgres -d postgres -f /tmp/bm25_vector_hybrid_test.sql
```

## Troubleshooting

### vLLM-server not accessible

1. **Check if vLLM-server is running:**
   ```bash
   ps aux | grep vllm
   # or
   docker ps | grep vllm
   ```

2. **Check the port:**
   - Default is 8000, but it might be different
   - Check vLLM-server logs or configuration

3. **If running in Docker:**
   - Ensure the port is exposed: `-p 8000:8000`
   - Use the container's IP or host network

### Model not found

1. **List available models:**
   ```bash
   curl http://localhost:8000/v1/models | python3 -m json.tool
   ```

2. **Specify the model explicitly:**
   ```bash
   export VLLM_MODEL=your-model-name
   ./generate_embeddings_vllm.sh
   ```

### Dimension mismatch

If your vLLM model produces embeddings with different dimensions:

1. **Check the dimension:**
   ```python
   import requests
   response = requests.post("http://localhost:8000/v1/embeddings", 
                           json={"model": "your-model", "input": "test"})
   dim = len(response.json()["data"][0]["embedding"])
   print(f"Dimension: {dim}")
   ```

2. **Update the table schema:**
   ```sql
   ALTER TABLE hybrid_test.documents ALTER COLUMN embedding TYPE vector(768);
   -- Replace 768 with your actual dimension
   ```

### Python dependencies missing

Install required packages:
```bash
pip3 install psycopg2-binary requests openai
```

## Current Test Data

The test suite includes 25 documents:
- **Technology**: 10 documents (ML, databases, programming, etc.)
- **Cooking**: 5 documents (Italian, French, Japanese, etc.)
- **Travel**: 5 documents (Paris, Tokyo, Barcelona, etc.)
- **Arts**: 5 documents (photography, painting, music, etc.)

All documents have:
- `title` and `body` columns
- `full_text` (generated: title + body) for BM25 indexing
- `category` for faceting
- `embedding` column (vector) for vector search

## Next Steps

Once embeddings are generated:

1. ✅ Run full test suite to verify hybrid search
2. ✅ Test different vector weights (0.2, 0.5, 0.8)
3. ✅ Verify rescoring/sorting works correctly
4. ✅ Compare BM25-only vs Hybrid results

## Notes

- Embeddings are saved in the database, so you only need to generate them once
- The test suite will detect if embeddings are real or placeholder
- With real embeddings, you'll see actual vector similarity scores
- Hybrid search combines BM25 and vector scores based on `vector_weight` parameter
