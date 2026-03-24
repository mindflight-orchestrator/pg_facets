#!/usr/bin/env python3
"""
Generate embeddings for hybrid_test.documents using ollama, vLLM-server, sentence-transformers, or OpenAI.

Usage:
    # With ollama (nomic-embed-text model, 768 dimensions)
    python generate_embeddings.py --method ollama --model nomic-embed-text --url http://localhost:11434

    # With vLLM-server (OpenAI-compatible API, default port 8000)
    python generate_embeddings.py --method vllm --model your-model-name --url http://localhost:8000

    # With sentence-transformers (all-MiniLM-L6-v2, 384 dimensions)
    python generate_embeddings.py --method transformers --model sentence-transformers/all-MiniLM-L6-v2

    # With OpenAI API
    python generate_embeddings.py --method openai --api-key YOUR_KEY
"""

import argparse
import json
import sys
from typing import List, Optional

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)


def generate_embeddings_ollama(texts: List[str], model: str = "nomic-embed-text", url: str = "http://localhost:11434") -> List[List[float]]:
    """Generate embeddings using ollama API."""
    import requests
    
    embeddings = []
    for text in texts:
        try:
            response = requests.post(
                f"{url}/api/embeddings",
                json={"model": model, "prompt": text},
                timeout=30
            )
            response.raise_for_status()
            embedding = response.json()["embedding"]
            embeddings.append(embedding)
        except Exception as e:
            print(f"Error generating embedding for text '{text[:50]}...': {e}")
            # Return zero vector as fallback
            dim = 768 if "nomic" in model.lower() else 384
            embeddings.append([0.0] * dim)
    
    return embeddings


def generate_embeddings_transformers(texts: List[str], model: str = "sentence-transformers/all-MiniLM-L6-v2") -> List[List[float]]:
    """Generate embeddings using sentence-transformers."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("Error: sentence-transformers is required. Install with: pip install sentence-transformers")
        sys.exit(1)
    
    model_instance = SentenceTransformer(model)
    embeddings = model_instance.encode(texts, show_progress_bar=True)
    return embeddings.tolist()


def generate_embeddings_openai(texts: List[str], api_key: str = None, model: str = "text-embedding-3-small", base_url: str = None) -> List[List[float]]:
    """Generate embeddings using OpenAI-compatible API (OpenAI or vLLM-server)."""
    try:
        from openai import OpenAI
    except ImportError:
        print("Error: openai is required. Install with: pip install openai")
        sys.exit(1)
    
    # Create client with optional base_url for vLLM-server
    client_kwargs = {}
    if base_url:
        client_kwargs["base_url"] = base_url
    if api_key:
        client_kwargs["api_key"] = api_key
    else:
        # vLLM-server doesn't require API key, but OpenAI client needs something
        client_kwargs["api_key"] = "not-needed"
    
    client = OpenAI(**client_kwargs)
    embeddings = []
    
    # API accepts batches
    batch_size = 100
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        try:
            response = client.embeddings.create(
                model=model,
                input=batch
            )
            batch_embeddings = [item.embedding for item in response.data]
            embeddings.extend(batch_embeddings)
            print(f"Generated embeddings for batch {i//batch_size + 1}/{len(texts)//batch_size + 1}")
        except Exception as e:
            print(f"Error generating embeddings for batch {i//batch_size + 1}: {e}")
            # Return zero vectors as fallback
            dim = 1536 if "3" in model else 1536
            embeddings.extend([[0.0] * dim] * len(batch))
    
    return embeddings


def generate_embeddings_vllm(texts: List[str], model: str, url: str = "http://localhost:8000") -> List[List[float]]:
    """Generate embeddings using vLLM-server (OpenAI-compatible API)."""
    # vLLM uses OpenAI-compatible API, so we can reuse the OpenAI function
    return generate_embeddings_openai(texts, api_key=None, model=model, base_url=url)


def update_database(conn, embeddings: List[List[float]], ids: List[int], dimension: int):
    """Update the database with generated embeddings."""
    cursor = conn.cursor()
    
    # Convert to PostgreSQL vector format
    updates = []
    for doc_id, embedding in zip(ids, embeddings):
        # Ensure correct dimension
        if len(embedding) != dimension:
            print(f"Warning: Embedding dimension mismatch for doc {doc_id}: expected {dimension}, got {len(embedding)}")
            continue
        
        # Format as PostgreSQL vector string
        vector_str = "[" + ",".join(str(float(x)) for x in embedding) + "]"
        updates.append((doc_id, vector_str))
    
    # Update in batches
    batch_size = 100
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        for doc_id, vector_str in batch:
            cursor.execute(
                f"UPDATE hybrid_test.documents SET embedding = %s::vector({dimension}) WHERE id = %s",
                (vector_str, doc_id)
            )
    
    conn.commit()
    cursor.close()
    print(f"Updated {len(updates)} documents with embeddings")


def main():
    parser = argparse.ArgumentParser(description="Generate embeddings for hybrid_test.documents")
    parser.add_argument("--method", choices=["ollama", "vllm", "transformers", "openai"], required=True,
                       help="Embedding generation method")
    parser.add_argument("--model", default=None,
                       help="Model name (default varies by method)")
    parser.add_argument("--url", default=None,
                       help="API URL (default: http://localhost:11434 for ollama, http://localhost:8000 for vllm)")
    parser.add_argument("--api-key", default=None,
                       help="API key (for openai method, not needed for vllm)")
    parser.add_argument("--db-url", default="postgresql://postgres:postgres@localhost:5432/postgres",
                       help="PostgreSQL connection string")
    parser.add_argument("--dimension", type=int, default=None,
                       help="Embedding dimension (auto-detected if not specified)")
    
    args = parser.parse_args()
    
    # Set default URL if not provided
    if args.url is None:
        if args.method == "ollama":
            args.url = "http://localhost:11434"
        elif args.method == "vllm":
            args.url = "http://localhost:8000"
    
    # Set default models
    if args.model is None:
        if args.method == "ollama":
            args.model = "nomic-embed-text"
        elif args.method == "vllm":
            print("Error: --model is required for vllm method. Specify the model name used in vLLM-server.")
            sys.exit(1)
        elif args.method == "transformers":
            args.model = "sentence-transformers/all-MiniLM-L6-v2"
        elif args.method == "openai":
            args.model = "text-embedding-3-small"
    
    # Determine dimension (vLLM dimension depends on the model, default to 384)
    if args.dimension is None:
        if args.method == "ollama" and "nomic" in args.model.lower():
            args.dimension = 768
        elif args.method == "transformers":
            args.dimension = 384  # all-MiniLM-L6-v2
        elif args.method == "openai":
            args.dimension = 1536  # text-embedding-3-small
        elif args.method == "vllm":
            # vLLM dimension depends on model - common values: 384, 768, 1024, 1536
            # We'll try to detect from first embedding, but default to 384
            args.dimension = 384  # Will be auto-detected from first response
        else:
            args.dimension = 384  # default
    
    # Connect to database
    try:
        conn = psycopg2.connect(args.db_url)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)
    
    # Fetch documents
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, full_text 
        FROM hybrid_test.documents 
        ORDER BY id
    """)
    documents = cursor.fetchall()
    cursor.close()
    
    if not documents:
        print("No documents found in hybrid_test.documents")
        sys.exit(1)
    
    print(f"Found {len(documents)} documents")
    print(f"Using method: {args.method}, model: {args.model}, dimension: {args.dimension}")
    print("Generating embeddings...")
    
    # Extract texts
    ids = [doc[0] for doc in documents]
    texts = [doc[1] for doc in documents]
    
    # Generate embeddings
    if args.method == "ollama":
        embeddings = generate_embeddings_ollama(texts, args.model, args.url)
    elif args.method == "vllm":
        embeddings = generate_embeddings_vllm(texts, args.model, args.url)
        # Auto-detect dimension from first embedding if not specified
        if embeddings and args.dimension == 384 and len(embeddings[0]) != 384:
            args.dimension = len(embeddings[0])
            print(f"Auto-detected embedding dimension: {args.dimension}")
    elif args.method == "transformers":
        embeddings = generate_embeddings_transformers(texts, args.model)
    elif args.method == "openai":
        if not args.api_key:
            print("Error: --api-key is required for OpenAI method")
            sys.exit(1)
        embeddings = generate_embeddings_openai(texts, args.api_key, args.model)
    
    # Update database
    print("Updating database...")
    update_database(conn, embeddings, ids, args.dimension)
    
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
