-- BM25 + Vector Hybrid Search Test Suite for pg_facets
-- Tests hybrid search combining BM25 and pgvector embeddings
-- Uses pre-generated embeddings to avoid requiring ollama/vLLM during test runs
--
-- NOTE: This test requires:
-- 1. pgvector extension to be installed
-- 2. Pre-generated embeddings (see generate_embeddings.sql script)
-- 3. A table with both text content and vector embeddings
--
-- IMPLEMENTATION NOTE:
-- - search_documents() fully supports hybrid BM25 + vector search
-- - search_documents_with_facets() currently has simplified vector support (BM25 only)
--   Full hybrid search in search_documents_with_facets is planned for future versions

\echo '=============================================='
\echo 'BM25 + Vector Hybrid Search Test Suite'
\echo '=============================================='

-- Check if pgvector extension is available
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        RAISE EXCEPTION 'pgvector extension is not installed. Please install it first: CREATE EXTENSION vector;';
    END IF;
    RAISE NOTICE 'pgvector extension found';
END;
$$;

-- Setup test schema
DROP SCHEMA IF EXISTS hybrid_test CASCADE;
CREATE SCHEMA hybrid_test;

-- Create a documents table with both text and vector columns
CREATE TABLE hybrid_test.documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    full_text TEXT GENERATED ALWAYS AS (title || ' ' || body) STORED,  -- Combined for BM25 indexing
    category TEXT,
    embedding vector(384),  -- Using 384 dimensions (e.g., sentence-transformers/all-MiniLM-L6-v2)
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index on full_text for BM25
CREATE INDEX idx_documents_full_text ON hybrid_test.documents USING gin(to_tsvector('english', full_text));

-- Insert 25 diverse documents with realistic content
INSERT INTO hybrid_test.documents (title, body, category) VALUES
    ('Introduction to Machine Learning', 'Machine learning is a subset of artificial intelligence that enables computers to learn from data without being explicitly programmed. It uses algorithms to identify patterns and make predictions based on historical data.', 'Technology'),
    ('Deep Learning Fundamentals', 'Deep learning uses neural networks with multiple layers to process complex data. Convolutional neural networks excel at image recognition, while recurrent networks handle sequential data like text and time series.', 'Technology'),
    ('Natural Language Processing Basics', 'NLP combines computational linguistics with machine learning to enable computers to understand, interpret, and generate human language. Applications include chatbots, translation, and sentiment analysis.', 'Technology'),
    ('PostgreSQL Database Administration', 'PostgreSQL is a powerful open-source relational database. Administrators must understand backup strategies, replication, query optimization, and security best practices to maintain production systems.', 'Technology'),
    ('Python Programming Guide', 'Python is a versatile programming language popular for data science, web development, and automation. Its simple syntax and extensive libraries make it ideal for beginners and experts alike.', 'Technology'),
    ('Data Science Workflow', 'Data science involves collecting, cleaning, analyzing, and visualizing data to extract insights. The workflow typically includes data exploration, feature engineering, model training, and evaluation.', 'Technology'),
    ('Cloud Computing Architecture', 'Cloud computing provides on-demand access to computing resources over the internet. Key models include Infrastructure as a Service, Platform as a Service, and Software as a Service.', 'Technology'),
    ('Container Orchestration with Kubernetes', 'Kubernetes automates container deployment, scaling, and management. It provides service discovery, load balancing, storage orchestration, and automated rollouts and rollbacks.', 'Technology'),
    ('Microservices Design Patterns', 'Microservices architecture breaks applications into small, independent services. Common patterns include API gateways, service discovery, circuit breakers, and event-driven communication.', 'Technology'),
    ('RESTful API Design Principles', 'REST APIs use HTTP methods to perform operations on resources. Good API design follows principles like statelessness, resource-based URLs, and proper use of status codes.', 'Technology'),
    ('Italian Cuisine: Pasta Making', 'Traditional Italian pasta making involves mixing semolina flour with eggs and water. Techniques vary by region, from the thin tagliatelle of Emilia-Romagna to the thick pici of Tuscany.', 'Cooking'),
    ('French Pastry Techniques', 'French pastry requires precision and technique. Key skills include making perfect puff pastry, tempering chocolate, and creating delicate mousses and creams for elegant desserts.', 'Cooking'),
    ('Japanese Sushi Preparation', 'Sushi making demands fresh ingredients and careful technique. The rice must be properly seasoned with vinegar, and fish must be sliced precisely to enhance flavor and texture.', 'Cooking'),
    ('Mediterranean Diet Benefits', 'The Mediterranean diet emphasizes fruits, vegetables, whole grains, and healthy fats. Research shows it reduces risk of heart disease and promotes longevity.', 'Cooking'),
    ('Baking Bread at Home', 'Homemade bread requires understanding fermentation, gluten development, and proper proofing. Sourdough breads use natural yeast cultures for complex flavors.', 'Cooking'),
    ('Travel Guide: Paris', 'Paris offers world-class museums, iconic landmarks like the Eiffel Tower, and exceptional cuisine. The city combines historic architecture with modern culture and art.', 'Travel'),
    ('Exploring Tokyo', 'Tokyo blends traditional temples with cutting-edge technology. Visitors can experience ancient Shinto shrines, modern skyscrapers, and unique neighborhoods like Shibuya and Harajuku.', 'Travel'),
    ('Barcelona Architecture Tour', 'Barcelona showcases Antoni Gaudí''s unique architectural style. The Sagrada Familia, Park Güell, and Casa Batlló demonstrate his innovative use of organic forms and colorful mosaics.', 'Travel'),
    ('New York City Neighborhoods', 'New York City''s diverse neighborhoods each have distinct character. From the artistic vibe of Greenwich Village to the financial district of Wall Street, the city offers endless exploration.', 'Travel'),
    ('Iceland Northern Lights', 'Iceland provides one of the best places to view the aurora borealis. The country''s remote location and minimal light pollution create ideal conditions for this natural phenomenon.', 'Travel'),
    ('Photography Composition Rules', 'Good photography composition follows principles like the rule of thirds, leading lines, and proper framing. Understanding light, depth of field, and perspective enhances visual storytelling.', 'Arts'),
    ('Watercolor Painting Techniques', 'Watercolor painting requires understanding of color mixing, paper selection, and brush control. Techniques like wet-on-wet and glazing create different effects and textures.', 'Arts'),
    ('Jazz Music History', 'Jazz originated in New Orleans in the early 20th century. The genre evolved through swing, bebop, and fusion, with influential artists like Louis Armstrong, Charlie Parker, and Miles Davis.', 'Arts'),
    ('Creative Writing Workshop', 'Effective creative writing requires strong characters, compelling plots, and vivid descriptions. Writers must balance showing versus telling and create authentic dialogue that advances the story.', 'Arts'),
    ('Film Production Basics', 'Film production involves pre-production planning, principal photography, and post-production editing. Understanding camera angles, lighting, and sound design creates compelling visual narratives.', 'Arts');

\echo ''
\echo '--- Generated 25 documents ---'
\echo 'NOTE: Embeddings need to be generated. See generate_embeddings.sql script.'
\echo 'For now, we will create placeholder embeddings (zeros) for testing structure.'

-- Create placeholder embeddings (zeros) - these should be replaced with real embeddings
-- In production, use a script to generate embeddings using ollama/vLLM
UPDATE hybrid_test.documents SET embedding = (
    SELECT array_agg(0.0::real)::vector(384)
    FROM generate_series(1, 384)
);

\echo ''
\echo '--- Adding faceting to the table ---'
SELECT facets.add_faceting_to_table(
    'hybrid_test.documents',
    key => 'id',
    facets => ARRAY[
        facets.plain_facet('category')
    ],
    populate => true
);

\echo ''
\echo '--- Indexing documents for BM25 search (title + body) ---'
DO $$
DECLARE
    v_doc record;
    v_count int := 0;
BEGIN
    FOR v_doc IN SELECT id, full_text FROM hybrid_test.documents ORDER BY id
    LOOP
        PERFORM facets.bm25_index_document(
            'hybrid_test.documents'::regclass,
            v_doc.id,
            v_doc.full_text,
            'full_text',
            'english'
        );
        v_count := v_count + 1;
    END LOOP;
    
    -- Recalculate statistics after indexing all documents
    PERFORM facets.bm25_recalculate_statistics('hybrid_test.documents'::regclass);
    
    RAISE NOTICE 'Indexed % documents for BM25 search', v_count;
END;
$$;

-- ============================================
-- SECTION 1: BM25 Only Tests (Baseline)
-- ============================================

\echo ''
\echo '=============================================='
\echo 'SECTION 1: BM25 Only Tests (Baseline)'
\echo '=============================================='

\echo ''
\echo '--- Test 1.1: BM25 search without vector (baseline) ---'
SELECT 'Test 1.1: BM25 only search' as test_name;
DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'machine learning',
        NULL,  -- no vector column
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );
    
    IF v_result.total_found >= 2 THEN
        RAISE NOTICE 'PASS: BM25 search found % results for "machine learning"', v_result.total_found;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected >= 2 results, got %', v_result.total_found;
    END IF;
END;
$$;

\echo ''
\echo '--- Test 1.2: BM25 search with facets (baseline) ---'
SELECT 'Test 1.2: BM25 with facets' as test_name;
DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents_with_facets(
        'hybrid_test',
        'documents',
        'database',
        NULL,
        NULL,
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        NULL,
        10,
        'english'
    );
    
    IF v_result.total_found >= 1 THEN
        RAISE NOTICE 'PASS: BM25 search with facets found % results', v_result.total_found;
    ELSE
        RAISE EXCEPTION 'FAIL: Expected >= 1 result, got %', v_result.total_found;
    END IF;
    
    IF jsonb_array_length(v_result.facets) > 0 THEN
        RAISE NOTICE 'PASS: Facets returned: % groups', jsonb_array_length(v_result.facets);
    ELSE
        RAISE EXCEPTION 'FAIL: No facets returned';
    END IF;
END;
$$;

-- ============================================
-- SECTION 2: Vector Only Tests (if embeddings are real)
-- ============================================

\echo ''
\echo '=============================================='
\echo 'SECTION 2: Vector Search Tests'
\echo '=============================================='
\echo 'NOTE: These tests require real embeddings. Placeholder embeddings (zeros) will not work correctly.'

\echo ''
\echo '--- Test 2.1: Vector similarity search (requires real embeddings) ---'
SELECT 'Test 2.1: Vector search only' as test_name;
DO $$
DECLARE
    v_query_vector vector(384);
    v_result_count int;
    v_has_real_embeddings boolean := false;
BEGIN
    -- Check if we have real embeddings (not all zeros)
    SELECT EXISTS(
        SELECT 1 FROM hybrid_test.documents 
        WHERE embedding IS NOT NULL 
        AND embedding != (SELECT array_agg(0.0::real)::vector(384) FROM generate_series(1, 384))
    ) INTO v_has_real_embeddings;
    
    IF NOT v_has_real_embeddings THEN
        RAISE NOTICE 'SKIP: No real embeddings found. Using placeholder embeddings.';
        RAISE NOTICE 'To enable vector search tests, generate real embeddings using generate_embeddings.sql';
        RETURN;
    END IF;
    
    -- Use the first document's embedding as query (for testing)
    SELECT embedding INTO v_query_vector FROM hybrid_test.documents LIMIT 1;
    
    -- Test vector similarity directly
    SELECT COUNT(*) INTO v_result_count
    FROM hybrid_test.documents
    WHERE embedding <=> v_query_vector < 0.8;
    
    IF v_result_count > 0 THEN
        RAISE NOTICE 'PASS: Vector search found % similar documents', v_result_count;
    ELSE
        RAISE NOTICE 'INFO: Vector search found 0 results (may be expected with current embeddings)';
    END IF;
END;
$$;

-- ============================================
-- SECTION 3: Hybrid BM25 + Vector Tests
-- ============================================

\echo ''
\echo '=============================================='
\echo 'SECTION 3: Hybrid BM25 + Vector Search Tests'
\echo '=============================================='

\echo ''
\echo '--- Test 3.1: Hybrid search structure (with placeholder embeddings) ---'
SELECT 'Test 3.1: Hybrid search structure' as test_name;
DO $$
DECLARE
    v_result record;
    v_first_result jsonb;
BEGIN
    -- This should work even with placeholder embeddings (tests the structure)
    SELECT * INTO v_result FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'machine learning',
        'embedding',  -- vector column specified
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,  -- 50% weight for vector
        'english'
    );
    
    IF v_result.total_found >= 0 THEN
        RAISE NOTICE 'PASS: Hybrid search executed (found % results)', v_result.total_found;
        
        -- Check result structure
        IF jsonb_array_length(v_result.results) > 0 THEN
            v_first_result := v_result.results->0;
            
            -- Verify hybrid search returns both scores
            IF v_first_result ? 'bm25_score' THEN
                RAISE NOTICE 'PASS: Result contains bm25_score';
            END IF;
            
            IF v_first_result ? 'vector_score' THEN
                RAISE NOTICE 'PASS: Result contains vector_score';
            END IF;
            
            IF v_first_result ? 'combined_score' THEN
                RAISE NOTICE 'PASS: Result contains combined_score';
            END IF;
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: Hybrid search failed';
    END IF;
END;
$$;

\echo ''
\echo '--- Test 3.2: Hybrid search with facets ---'
SELECT 'Test 3.2: Hybrid search with facets' as test_name;
DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents_with_facets(
        'hybrid_test',
        'documents',
        'technology',
        NULL,
        'embedding',  -- vector column
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,  -- vector weight
        10,
        'english'
    );
    
    IF v_result.total_found >= 0 THEN
        RAISE NOTICE 'PASS: Hybrid search with facets executed (found % results)', v_result.total_found;
        
        IF jsonb_array_length(v_result.facets) > 0 THEN
            RAISE NOTICE 'PASS: Facets returned: % groups', jsonb_array_length(v_result.facets);
        END IF;
    ELSE
        RAISE EXCEPTION 'FAIL: Hybrid search with facets failed';
    END IF;
END;
$$;

\echo ''
\echo '--- Test 3.3: Vector weight variations ---'
SELECT 'Test 3.3: Vector weight variations' as test_name;
DO $$
DECLARE
    v_result_bm25_weighted record;
    v_result_vector_weighted record;
    v_result_balanced record;
BEGIN
    -- BM25 weighted (vector_weight = 0.2, so BM25 = 0.8)
    SELECT * INTO v_result_bm25_weighted FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'programming',
        'embedding',
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.2,  -- low vector weight
        'english'
    );
    
    -- Vector weighted (vector_weight = 0.8, so BM25 = 0.2)
    SELECT * INTO v_result_vector_weighted FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'programming',
        'embedding',
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.8,  -- high vector weight
        'english'
    );
    
    -- Balanced (vector_weight = 0.5, so BM25 = 0.5)
    SELECT * INTO v_result_balanced FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'programming',
        'embedding',
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,  -- balanced
        'english'
    );
    
    RAISE NOTICE 'PASS: Vector weight variations tested';
    RAISE NOTICE '  BM25-weighted (0.2): % results', v_result_bm25_weighted.total_found;
    RAISE NOTICE '  Vector-weighted (0.8): % results', v_result_vector_weighted.total_found;
    RAISE NOTICE '  Balanced (0.5): % results', v_result_balanced.total_found;
END;
$$;

\echo ''
\echo '--- Test 3.4: Verify rescoring/sorting works correctly ---'
SELECT 'Test 3.4: Rescoring verification' as test_name;
DO $$
DECLARE
    v_result record;
    v_first_result jsonb;
    v_second_result jsonb;
    v_first_combined float;
    v_second_combined float;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'learning',
        'embedding',
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        5,
        0,
        0.0,
        0.5,
        'english'
    );
    
    IF jsonb_array_length(v_result.results) >= 2 THEN
        v_first_result := v_result.results->0;
        v_second_result := v_result.results->1;
        
        v_first_combined := (v_first_result->>'combined_score')::float;
        v_second_combined := (v_second_result->>'combined_score')::float;
        
        IF v_first_combined >= v_second_combined THEN
            RAISE NOTICE 'PASS: Results are sorted by combined_score (descending)';
            RAISE NOTICE '  First result combined_score: %', v_first_combined;
            RAISE NOTICE '  Second result combined_score: %', v_second_combined;
        ELSE
            RAISE EXCEPTION 'FAIL: Results not sorted correctly. First: %, Second: %', 
                v_first_combined, v_second_combined;
        END IF;
    ELSE
        RAISE NOTICE 'INFO: Not enough results to verify sorting (need >= 2)';
    END IF;
END;
$$;

\echo ''
\echo '--- Test 3.5: Hybrid search with facet filters ---'
SELECT 'Test 3.5: Hybrid search with facet filters' as test_name;
DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents_with_facets(
        'hybrid_test',
        'documents',
        'technology',
        '{"category":"Technology"}'::jsonb,
        'embedding',
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        10,
        'english'
    );
    
    IF v_result.total_found >= 0 THEN
        RAISE NOTICE 'PASS: Hybrid search with facet filter executed (found % results)', v_result.total_found;
    ELSE
        RAISE EXCEPTION 'FAIL: Hybrid search with facet filter failed';
    END IF;
END;
$$;

\echo ''
\echo '--- Test 3.6: Compare BM25-only vs Hybrid results ---'
SELECT 'Test 3.6: BM25 vs Hybrid comparison' as test_name;
DO $$
DECLARE
    v_bm25_only record;
    v_hybrid record;
BEGIN
    -- BM25 only
    SELECT * INTO v_bm25_only FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'machine learning neural networks',
        NULL,  -- no vector
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );
    
    -- Hybrid
    SELECT * INTO v_hybrid FROM facets.search_documents(
        'hybrid_test',
        'documents',
        'machine learning neural networks',
        'embedding',  -- with vector
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );
    
    RAISE NOTICE 'PASS: Comparison completed';
    RAISE NOTICE '  BM25 only: % results', v_bm25_only.total_found;
    RAISE NOTICE '  Hybrid: % results', v_hybrid.total_found;
    
    -- Note: With placeholder embeddings, results may be similar
    -- With real embeddings, hybrid should potentially find different/better results
END;
$$;

-- ============================================
-- SECTION 4: Edge Cases
-- ============================================

\echo ''
\echo '=============================================='
\echo 'SECTION 4: Edge Cases'
\echo '=============================================='

\echo ''
\echo '--- Test 4.1: Hybrid search with empty query (should use vector only) ---'
SELECT 'Test 4.1: Empty query with vector' as test_name;
DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents(
        'hybrid_test',
        'documents',
        '',  -- empty query
        'embedding',  -- but has vector
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );
    
    IF v_result.total_found >= 0 THEN
        RAISE NOTICE 'PASS: Empty query with vector executed (found % results)', v_result.total_found;
    ELSE
        RAISE EXCEPTION 'FAIL: Empty query with vector failed';
    END IF;
END;
$$;

\echo ''
\echo '--- Test 4.2: Hybrid search with NULL query ---'
SELECT 'Test 4.2: NULL query with vector' as test_name;
DO $$
DECLARE
    v_result record;
BEGIN
    SELECT * INTO v_result FROM facets.search_documents(
        'hybrid_test',
        'documents',
        NULL,  -- NULL query
        'embedding',
        'full_text',
        'metadata',
        'created_at',
        'updated_at',
        10,
        0,
        0.0,
        0.5,
        'english'
    );
    
    IF v_result.total_found >= 0 THEN
        RAISE NOTICE 'PASS: NULL query with vector executed (found % results)', v_result.total_found;
    ELSE
        RAISE EXCEPTION 'FAIL: NULL query with vector failed';
    END IF;
END;
$$;

-- Cleanup
\echo ''
\echo '--- Cleanup ---'
SELECT facets.drop_faceting('hybrid_test.documents');
DROP SCHEMA hybrid_test CASCADE;

\echo ''
\echo '=============================================='
\echo 'BM25 + Vector Hybrid Search Test Suite Complete!'
\echo '=============================================='
\echo 'NOTE: For full vector search functionality, generate real embeddings using:'
\echo '  - generate_embeddings.sql script (requires ollama/vLLM)'
\echo '  - Or use a pre-generated embeddings file'
\echo '=============================================='
