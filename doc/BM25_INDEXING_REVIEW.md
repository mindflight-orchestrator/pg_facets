# BM25 Indexing Review and Troubleshooting Guide

## Overview

This document explains how BM25 indexing works in `pg_facets`, where the data is stored, and how to properly rebuild indexes.

## BM25 Data Storage

BM25 indexes are stored in **three tables** in the `facets` schema:

### 1. `facets.bm25_index` - Inverted Index
This is the main inverted index that maps terms to documents:
- `table_id` (oid): The OID of the indexed table
- `term_hash` (bigint): Hash of the lexeme (from PostgreSQL's `to_tsvector`)
- `term_text` (text): Original lexeme text (for debugging/prefix matching)
- `doc_ids` (roaringbitmap): Bitmap of document IDs containing this term
- `term_freqs` (jsonb): Map of `doc_id -> term_frequency` for each document
- `language` (text): Text search config used (default: 'english')

**This is where the actual search index is stored.**

### 2. `facets.bm25_documents` - Document Metadata
Stores metadata about each indexed document:
- `table_id` (oid): The OID of the indexed table
- `doc_id` (bigint): Document ID (matches the primary key of the source table)
- `doc_length` (integer): Number of tokens in the document
- `language` (text): Text search config used
- `created_at`, `updated_at` (timestamp): Timestamps

### 3. `facets.bm25_statistics` - Collection Statistics
Stores collection-level statistics needed for BM25 scoring:
- `table_id` (oid): The OID of the indexed table
- `total_documents` (bigint): Total number of indexed documents (N)
- `avg_document_length` (float): Average document length (avgdl)
- `last_updated` (timestamp): Last update time

## How BM25 Indexing Works

### Step 1: Register the Table
Before indexing, the table must be registered with `facets.add_faceting_to_table()`:

```sql
SELECT facets.add_faceting_to_table(
    'providers_imdb.title_basics'::regclass,
    key => 'tconst',  -- Your primary key column
    facets => ARRAY[
        -- Your facet definitions here
    ],
    populate => true
);
```

### Step 2: Index Each Document
For each document, call `facets.bm25_index_document()`:

```sql
SELECT facets.bm25_index_document(
    'providers_imdb.title_basics'::regclass,  -- Table to index
    doc_id,                                    -- Document ID (primary key value)
    content_text,                              -- Text content to index
    'content',                                 -- Column name (for reference, not used)
    'english'                                  -- Language config
);
```

### Step 3: Recalculate Statistics
After indexing all documents, recalculate statistics:

```sql
SELECT facets.bm25_recalculate_statistics('providers_imdb.title_basics'::regclass);
```

## Correct Rebuild Function Pattern

Here's the correct pattern for a rebuild function:

```sql
CREATE OR REPLACE FUNCTION providers_imdb.rebuild_title_basics_bm25()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_doc record;
    v_count bigint := 0;
    v_total bigint;
BEGIN
    -- Get total count for progress tracking
    SELECT COUNT(*) INTO v_total FROM providers_imdb.title_basics;
    
    RAISE NOTICE 'Starting BM25 rebuild for title_basics. Total documents: %', v_total;
    
    -- Clear existing index (optional - only if you want a fresh start)
    -- DELETE FROM facets.bm25_index WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
    -- DELETE FROM facets.bm25_documents WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
    -- DELETE FROM facets.bm25_statistics WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
    
    -- Index each document
    FOR v_doc IN 
        SELECT 
            tconst,  -- Your primary key
            COALESCE(primarytitle, '') || ' ' || COALESCE(originaltitle, '') || ' ' || COALESCE(description, '') AS content
        FROM providers_imdb.title_basics
        ORDER BY tconst
    LOOP
        -- Index the document
        PERFORM facets.bm25_index_document(
            'providers_imdb.title_basics'::regclass,
            v_doc.tconst::bigint,  -- Convert to bigint if needed
            v_doc.content,
            'content',
            'english'
        );
        
        v_count := v_count + 1;
        
        -- Progress update every 10000 documents
        IF v_count % 10000 = 0 THEN
            RAISE NOTICE 'Indexed % / % documents (%.1f%%)', 
                v_count, v_total, (v_count::float / v_total::float * 100);
        END IF;
    END LOOP;
    
    -- Final statistics recalculation
    PERFORM facets.bm25_recalculate_statistics('providers_imdb.title_basics'::regclass);
    
    RAISE NOTICE 'BM25 rebuild complete. Indexed % documents', v_count;
END;
$$;
```

## Common Issues and Solutions

### Issue 1: Tables Remain Empty After Rebuild

**Symptoms:**
- `bm25_documents`, `bm25_index`, and `bm25_statistics` tables are empty
- Rebuild function runs for hours but produces no data

**Possible Causes:**

1. **Table not registered in `facets.faceted_table`**
   ```sql
   -- Check if table is registered
   SELECT * FROM facets.faceted_table 
   WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
   ```
   
   **Solution:** Register the table first:
   ```sql
   SELECT facets.add_faceting_to_table(
       'providers_imdb.title_basics'::regclass,
       key => 'tconst',  -- Your primary key column name
       facets => ARRAY[],  -- Empty array if you only need BM25
       populate => false
   );
   ```

2. **Rebuild function not calling `bm25_index_document()`**
   - Check your rebuild function - it must call `facets.bm25_index_document()` for each document
   - The function should loop through all documents and index them individually

3. **Wrong table OID or table name**
   - Verify the table exists and the name is correct
   - Check that you're using `::regclass` correctly

4. **Silent errors in the rebuild function**
   - Add error handling and logging
   - Check PostgreSQL logs for errors

### Issue 2: Rebuild Takes Too Long

**Solutions:**

1. **Use batch indexing** (available in 0.4.1+):
   ```sql
   SELECT * FROM facets.bm25_index_documents_batch(
       'providers_imdb.title_basics'::regclass,
       (
           SELECT jsonb_agg(jsonb_build_object(
               'doc_id', tconst,
               'content', COALESCE(primarytitle, '') || ' ' || COALESCE(originaltitle, '')
           ))
           FROM providers_imdb.title_basics
       ),
       'content',
       'english',
       1000  -- batch size
   );
   ```

2. **Use parallel indexing** (for very large tables):
   ```sql
   SELECT * FROM facets.bm25_index_documents_parallel(
       'providers_imdb.title_basics'::regclass,
       'SELECT tconst, COALESCE(primarytitle, '''') || '' '' || COALESCE(originaltitle, '''') AS content FROM providers_imdb.title_basics',
       'content',
       'english',
       1000000,  -- total_docs
       4,        -- num_workers
       ''        -- connection_string (empty = same connection)
   );
   ```

### Issue 3: Statistics Not Updated

**Solution:** Always call `bm25_recalculate_statistics()` after indexing:
```sql
SELECT facets.bm25_recalculate_statistics('providers_imdb.title_basics'::regclass);
```

## Verification Queries

### Check if documents are indexed:
```sql
SELECT COUNT(*) as indexed_docs
FROM facets.bm25_documents
WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
```

### Check if terms are indexed:
```sql
SELECT COUNT(*) as indexed_terms
FROM facets.bm25_index
WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
```

### Check statistics:
```sql
SELECT * FROM facets.bm25_statistics
WHERE table_id = 'providers_imdb.title_basics'::regclass::oid;
```

### Test a search:
```sql
SELECT * FROM facets.bm25_search(
    'providers_imdb.title_basics'::regclass,
    'action movie',
    'english',
    false,  -- prefix_match
    false,  -- fuzzy_match
    0.3,    -- fuzzy_threshold
    1.2,    -- k1
    0.75,   -- b
    10      -- limit
);
```

### Check sample indexed terms:
```sql
SELECT term_text, rb_cardinality(doc_ids) as doc_count
FROM facets.bm25_index
WHERE table_id = 'providers_imdb.title_basics'::regclass::oid
ORDER BY doc_count DESC
LIMIT 20;
```

## Debugging Your Rebuild Function

If your rebuild function isn't working, add debugging:

```sql
CREATE OR REPLACE FUNCTION providers_imdb.rebuild_title_basics_bm25()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_doc record;
    v_count bigint := 0;
    v_table_oid oid;
    v_error_count int := 0;
BEGIN
    -- Get table OID
    v_table_oid := 'providers_imdb.title_basics'::regclass::oid;
    
    -- Verify table is registered
    IF NOT EXISTS (SELECT 1 FROM facets.faceted_table WHERE table_id = v_table_oid) THEN
        RAISE EXCEPTION 'Table not registered. Run facets.add_faceting_to_table() first.';
    END IF;
    
    RAISE NOTICE 'Table OID: %, Registered: Yes', v_table_oid;
    
    -- Test with a single document first
    FOR v_doc IN 
        SELECT tconst, primarytitle
        FROM providers_imdb.title_basics
        LIMIT 1
    LOOP
        BEGIN
            PERFORM facets.bm25_index_document(
                'providers_imdb.title_basics'::regclass,
                v_doc.tconst::bigint,
                COALESCE(v_doc.primarytitle, ''),
                'content',
                'english'
            );
            RAISE NOTICE 'Successfully indexed document: %', v_doc.tconst;
        EXCEPTION WHEN OTHERS THEN
            v_error_count := v_error_count + 1;
            RAISE WARNING 'Error indexing document %: %', v_doc.tconst, SQLERRM;
        END;
    END LOOP;
    
    -- Check if data was inserted
    SELECT COUNT(*) INTO v_count 
    FROM facets.bm25_documents 
    WHERE table_id = v_table_oid;
    
    RAISE NOTICE 'Documents in index: %, Errors: %', v_count, v_error_count;
END;
$$;
```

## Key Points to Remember

1. **The table MUST be registered** in `facets.faceted_table` before indexing
2. **Each document must be indexed individually** using `facets.bm25_index_document()`
3. **Statistics must be recalculated** after indexing using `facets.bm25_recalculate_statistics()`
4. **The primary key column** must be specified correctly when registering the table
5. **The content text** should be concatenated from relevant columns (title, description, etc.)

## Where Are the Indexes Stored?

The BM25 indexes are stored **entirely in PostgreSQL tables**:
- `facets.bm25_index` - The inverted index (term -> documents)
- `facets.bm25_documents` - Document metadata
- `facets.bm25_statistics` - Collection statistics

There are no separate files or external storage. Everything is in the database, which means:
- Indexes are backed up with your database
- Indexes are replicated if you use replication
- Indexes are transaction-safe
- You can query the index tables directly for debugging

## Next Steps

1. **Verify your table is registered:**
   ```sql
   SELECT * FROM facets.faceted_table 
   WHERE table_id::regclass::text LIKE '%title_basics%';
   ```

2. **Check your rebuild function** - ensure it's calling `facets.bm25_index_document()` for each document

3. **Test with a small subset** first (LIMIT 10) to verify it works

4. **Check PostgreSQL logs** for any errors during the rebuild

5. **Verify data is being inserted** by checking the tables during/after the rebuild

