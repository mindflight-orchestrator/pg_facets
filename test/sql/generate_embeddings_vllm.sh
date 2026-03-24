#!/bin/bash
# Script to generate embeddings using vLLM-server
# This script will detect vLLM-server and generate embeddings for hybrid_test.documents

set -e

# Default values
VLLM_URL="${VLLM_URL:-http://localhost:8000}"
MODEL="${VLLM_MODEL:-}"
DB_URL="${DB_URL:-postgresql://postgres:postgres@localhost:5432/postgres}"

echo "=============================================="
echo "Generating Embeddings with vLLM-server"
echo "=============================================="
echo ""

# Check if vLLM server is accessible
echo "Checking vLLM-server at $VLLM_URL..."
if curl -s -f "$VLLM_URL/v1/models" > /dev/null 2>&1; then
    echo "✓ vLLM-server is accessible"
    
    # Try to get available models
    MODELS=$(curl -s "$VLLM_URL/v1/models" | python3 -c "import sys, json; data=json.load(sys.stdin); print('\n'.join([m['id'] for m in data.get('data', [])]))" 2>/dev/null || echo "")
    
    if [ -n "$MODELS" ]; then
        echo "Available models:"
        echo "$MODELS" | sed 's/^/  - /'
        echo ""
        
        # Use first model if not specified
        if [ -z "$MODEL" ]; then
            MODEL=$(echo "$MODELS" | head -1)
            echo "Using first available model: $MODEL"
        fi
    else
        if [ -z "$MODEL" ]; then
            echo "Error: Could not detect models. Please specify --model"
            echo "Usage: $0 [--model MODEL_NAME] [--url VLLM_URL] [--db-url DB_URL]"
            exit 1
        fi
    fi
else
    echo "✗ vLLM-server is not accessible at $VLLM_URL"
    echo ""
    echo "Please ensure vLLM-server is running and accessible."
    echo "You can specify a different URL with:"
    echo "  VLLM_URL=http://your-server:port $0"
    echo ""
    exit 1
fi

# Check if table exists
echo "Checking if hybrid_test.documents exists..."
if ! docker exec pg_facets psql -U postgres -d postgres -c "SELECT 1 FROM hybrid_test.documents LIMIT 1" > /dev/null 2>&1; then
    echo "Error: hybrid_test.documents table does not exist."
    echo "Please run bm25_vector_hybrid_test.sql first to create the table."
    exit 1
fi

echo "✓ Table exists"
echo ""

# Check if Python dependencies are available
echo "Checking Python dependencies..."
if ! python3 -c "import psycopg2, requests, openai" 2>/dev/null; then
    echo "Installing required Python packages..."
    pip3 install --user psycopg2-binary requests openai
fi
echo "✓ Dependencies available"
echo ""

# Generate embeddings
echo "Generating embeddings..."
echo "  Model: $MODEL"
echo "  vLLM URL: $VLLM_URL"
echo "  Database: $DB_URL"
echo ""

python3 generate_embeddings.py \
    --method vllm \
    --model "$MODEL" \
    --url "$VLLM_URL" \
    --db-url "$DB_URL"

echo ""
echo "=============================================="
echo "Embedding generation complete!"
echo "=============================================="
echo ""
echo "You can now re-run the test suite:"
echo "  docker exec pg_facets psql -U postgres -d postgres -f /tmp/bm25_vector_hybrid_test.sql"
echo ""
