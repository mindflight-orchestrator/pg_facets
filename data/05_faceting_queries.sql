-- 05_faceting_queries.sql
-- Converted queries using facets.search_documents_with_facets()
-- Based on 01_basic_queries.sql, 02_intermediate_queries.sql, and 03_advanced_queries.sql

-- ============================================================================
-- BASIC QUERIES (converted from 01_basic_queries.sql)
-- ============================================================================

-- 1. List the first 10 movies (not adult) with their titles and start years
-- Original: SELECT primaryTitle, startYear FROM title_basics WHERE titleType = 'movie' AND isAdult = 0 ORDER BY startYear LIMIT 10;
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryTitle' AS primaryTitle,
    (result->'metadata'->>'startYear')::integer AS startYear,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',  -- Empty query to get all matching filters
        '{"title_type": "movie", "is_adult": false}'::jsonb,
        NULL,  -- p_vector_column
        'content',  -- p_content_column
        'metadata',  -- p_metadata_column
        'created_at',  -- p_created_at_column
        'updated_at',  -- p_updated_at_column
        10,  -- p_limit
        0,  -- p_offset
        0.0,  -- p_min_score
        0.5,  -- p_vector_weight
        5  -- p_facet_limit
    )
) AS search_results
ORDER BY (result->'metadata'->>'startYear')::integer NULLS LAST
LIMIT 10;

-- 2. Find the top 10 highest-rated movies (with at least 100,000 votes)
-- Original: SELECT b.primaryTitle, r.averageRating, r.numVotes FROM title_basics b JOIN title_ratings r ON b.tconst = r.tconst WHERE b.titleType = 'movie' AND r.numVotes >= 100000 ORDER BY r.averageRating DESC LIMIT 10;
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryTitle' AS primaryTitle,
    (result->'metadata'->>'averageRating')::numeric AS averageRating,
    (result->'metadata'->>'numVotes')::bigint AS numVotes,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        '{"title_type": "movie"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        1000,  -- Get more results to filter by numVotes
        0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'numVotes')::bigint >= 100000
ORDER BY (result->'metadata'->>'averageRating')::numeric DESC
LIMIT 10;

-- 3. Get the list of genres for TV shows
-- Original: SELECT DISTINCT unnest(string_to_array(genres, ',')) AS genre FROM title_basics WHERE titleType = 'tvSeries';
-- Note: This query uses facets to get genre information
SELECT DISTINCT
    facet_value->>'value' AS genre
FROM (
    SELECT jsonb_array_elements(facets) AS facet_group
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        '{"title_type": "tvSeries"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        0, 0, 0.0, 0.5, 1000  -- Large facet_limit to get all genres
    )
) AS facet_groups,
LATERAL jsonb_array_elements(facet_group->'values') AS facet_value
WHERE facet_group->>'name' = 'primary_genre'
ORDER BY genre;

-- Alternative approach: Extract from metadata
SELECT DISTINCT
    TRIM(unnest(string_to_array(result->'metadata'->>'genres', ','))) AS genre
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        '{"title_type": "tvSeries"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        10000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE result->'metadata'->>'genres' IS NOT NULL
  AND result->'metadata'->>'genres' != ''
ORDER BY genre;

-- 4. Count the number of titles by titleType
-- Original: SELECT titleType, COUNT(*) AS total_titles FROM title_basics GROUP BY titleType ORDER BY total_titles DESC;
SELECT 
    facet_value->>'value' AS titleType,
    (facet_value->>'count')::bigint AS total_titles
FROM (
    SELECT jsonb_array_elements(facets) AS facet_group
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,  -- No filters
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        0, 0, 0.0, 0.5, 1000
    )
) AS facet_groups,
LATERAL jsonb_array_elements(facet_group->'values') AS facet_value
WHERE facet_group->>'name' = 'title_type'
ORDER BY (facet_value->>'count')::bigint DESC;

-- 5. Find all movies with more than one genre
-- Original: SELECT tconst, primaryTitle, genres FROM title_basics WHERE titleType = 'movie' AND genres LIKE '%,%';
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'tconst' AS tconst,
    result->'metadata'->>'primaryTitle' AS primaryTitle,
    result->'metadata'->>'genres' AS genres
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        '{"title_type": "movie"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        10000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE result->'metadata'->>'genres' IS NOT NULL
  AND result->'metadata'->>'genres' LIKE '%,%';

-- 6. Find all titles containing the word "Matrix" with rating ≥ 7
-- Original: SELECT b.primaryTitle, r.averageRating FROM title_basics b JOIN title_ratings r ON b.tconst = r.tconst WHERE b.primaryTitle ILIKE '%matrix%' AND r.averageRating >= 7 ORDER BY r.averageRating DESC;
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryTitle' AS primaryTitle,
    (result->'metadata'->>'averageRating')::numeric AS averageRating,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        'Matrix',  -- Text search for "Matrix"
        NULL::jsonb,  -- No facet filters
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        1000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'averageRating')::numeric >= 7
ORDER BY (result->'metadata'->>'averageRating')::numeric DESC;

-- 7. Get the top 10 most voted titles per decade
-- Original: Complex query with CASE statements for decades
-- Note: This requires post-processing since decade is not a direct facet
SELECT 
    CASE
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1900 AND 1909 THEN '1900s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1910 AND 1919 THEN '1910s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1920 AND 1929 THEN '1920s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1930 AND 1939 THEN '1930s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1940 AND 1949 THEN '1940s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2010 AND 2019 THEN '2010s'
        WHEN (result->'metadata'->>'startYear')::integer >= 2020 THEN '2020s'
        ELSE 'Unknown'
    END AS decade,
    result->'metadata'->>'primaryTitle' AS primaryTitle,
    (result->'metadata'->>'numVotes')::bigint AS numVotes
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        '{"title_type": "movie"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        10000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'startYear')::integer IS NOT NULL
  AND (result->'metadata'->>'numVotes')::bigint IS NOT NULL
  AND (result->'metadata'->>'numVotes')::bigint > 10000
ORDER BY decade, (result->'metadata'->>'numVotes')::bigint DESC;

-- ============================================================================
-- INTERMEDIATE QUERIES (converted from 02_intermediate_queries.sql)
-- ============================================================================

-- 1. List the 5 actors with the most appearances in the dataset
-- Original: Uses title_principals and name_basics tables
-- Note: This requires the name_basics_faceting_mv table and title_principals join
-- For now, we'll show how to search actors using facets
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryName' AS primaryname,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'name_basics_faceting_mv',
        '',
        '{"primary_profession": "actor"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        5, 0, 0.0, 0.5, 5
    )
) AS search_results
ORDER BY (result->>'combined_score')::numeric DESC
LIMIT 5;

-- Note: To get actual appearance counts, you'd need to join with title_principals
-- This would require a more complex query or a separate faceting table for principals

-- 2. Show the number of titles released each year from 1990 to 2020
-- Original: SELECT startyear, COUNT(*) AS total_titles FROM title_basics WHERE startyear BETWEEN 1990 AND 2020 GROUP BY startyear ORDER BY startyear;
-- Note: This uses the start_year bucket facet
SELECT 
    facet_value->>'value' AS year_bucket,
    (facet_value->>'count')::bigint AS total_titles
FROM (
    SELECT jsonb_array_elements(facets) AS facet_group
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        0, 0, 0.0, 0.5, 1000
    )
) AS facet_groups,
LATERAL jsonb_array_elements(facet_group->'values') AS facet_value
WHERE facet_group->>'name' = 'start_year'
  AND (facet_value->>'value')::text LIKE '1980%'  -- Filter for relevant buckets
   OR (facet_value->>'value')::text LIKE '2000%'
   OR (facet_value->>'value')::text LIKE '2010%'
   OR (facet_value->>'value')::text LIKE '2015%'
   OR (facet_value->>'value')::text LIKE '2020%'
ORDER BY year_bucket;

-- Alternative: Extract from metadata and group
SELECT 
    (result->'metadata'->>'startYear')::integer AS startyear,
    COUNT(*) AS total_titles
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        100000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'startYear')::integer BETWEEN 1990 AND 2020
GROUP BY (result->'metadata'->>'startYear')::integer
ORDER BY startyear;

-- 3. Find all titles that belong to both 'Action' and 'Thriller' genres
-- Original: SELECT primarytitle, genres FROM title_basics WHERE genres ILIKE '%Action%' AND genres ILIKE '%Thriller%' LIMIT 10;
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryTitle' AS primarytitle,
    result->'metadata'->>'genres' AS genres
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        10000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE result->'metadata'->>'genres' ILIKE '%Action%'
  AND result->'metadata'->>'genres' ILIKE '%Thriller%'
LIMIT 10;

-- 4. Count how many titles belong to more than one genre
-- Original: SELECT COUNT(*) AS multi_genre_titles FROM title_basics WHERE genres ILIKE '%,%';
SELECT 
    COUNT(*) AS multi_genre_titles
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        100000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE result->'metadata'->>'genres' ILIKE '%,%';

-- 5. Find titles containing the word 'Matrix' with rating >= 7
-- Original: SELECT tb.primarytitle, tr.averagerating FROM title_basics AS tb JOIN title_ratings AS tr ON tb.tconst = tr.tconst WHERE tb.primarytitle ILIKE '%Matrix%' AND tr.averagerating >= 7;
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryTitle' AS primarytitle,
    (result->'metadata'->>'averageRating')::numeric AS averagerating,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        'Matrix',  -- Text search
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        1000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'averageRating')::numeric >= 7;

-- 6. List the top 5 directors by number of titles they directed
-- Original: Uses title_principals with category = 'director'
-- Note: Similar to query 1, this requires joining with title_principals
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryName' AS primaryname,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'name_basics_faceting_mv',
        '',
        '{"primary_profession": "director"}'::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        5, 0, 0.0, 0.5, 5
    )
) AS search_results
ORDER BY (result->>'combined_score')::numeric DESC
LIMIT 5;

-- 7. Titles with more than 1 person credited as principal cast
-- Original: Uses title_principals join
-- Note: This requires a separate faceting table for title_principals or a different approach
-- This query cannot be directly converted without additional setup

-- ============================================================================
-- ADVANCED QUERIES (converted from 03_advanced_queries.sql)
-- ============================================================================

-- 1. Top 3 decades with the highest number of published titles
-- Original: Uses (startyear / 10) * 10 AS decade
SELECT 
    CASE
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1900 AND 1909 THEN '1900s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1910 AND 1919 THEN '1910s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1920 AND 1929 THEN '1920s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1930 AND 1939 THEN '1930s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1940 AND 1949 THEN '1940s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2010 AND 2019 THEN '2010s'
        WHEN (result->'metadata'->>'startYear')::integer >= 2020 THEN '2020s'
        ELSE 'Unknown'
    END AS decade,
    COUNT(*) AS total_titles
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        100000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'startYear')::integer IS NOT NULL
GROUP BY decade
ORDER BY total_titles DESC
LIMIT 3;

-- 2. For each decade, show the title with the highest number of votes
-- Original: Uses window functions with RANK()
WITH decade_titles AS (
    SELECT 
        CASE
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1900 AND 1909 THEN '1900s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1910 AND 1919 THEN '1910s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1920 AND 1929 THEN '1920s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1930 AND 1939 THEN '1930s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1940 AND 1949 THEN '1940s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1950 AND 1959 THEN '1950s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1960 AND 1969 THEN '1960s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1970 AND 1979 THEN '1970s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1980 AND 1989 THEN '1980s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1990 AND 1999 THEN '1990s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2000 AND 2009 THEN '2000s'
            WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2010 AND 2019 THEN '2010s'
            WHEN (result->'metadata'->>'startYear')::integer >= 2020 THEN '2020s'
            ELSE 'Unknown'
        END AS decade,
        result->'metadata'->>'primaryTitle' AS primarytitle,
        (result->'metadata'->>'numVotes')::bigint AS numvotes
    FROM (
        SELECT jsonb_array_elements(results) AS result
        FROM facets.search_documents_with_facets(
            'providers_imdb',
            'title_basics_faceting_mv',
            '',
            NULL::jsonb,
            NULL, 'content', 'metadata', 'created_at', 'updated_at',
            100000, 0, 0.0, 0.5, 5
        )
    ) AS search_results
    WHERE (result->'metadata'->>'startYear')::integer IS NOT NULL
      AND (result->'metadata'->>'numVotes')::bigint IS NOT NULL
),
ranked AS (
    SELECT 
        decade,
        primarytitle,
        numvotes,
        RANK() OVER (PARTITION BY decade ORDER BY numvotes DESC) AS rank_vote
    FROM decade_titles
)
SELECT decade, primarytitle, numvotes
FROM ranked
WHERE rank_vote = 1
ORDER BY decade;

-- 3. Find people who have acted and directed at least once
-- Original: Uses self-join on title_principals
-- Note: This requires additional setup with title_principals faceting or a different approach
-- This query cannot be directly converted without additional faceting tables

-- 4. Show titles that have more than one type of contributor (e.g., actor, director, writer)
-- Original: Uses title_principals with COUNT(DISTINCT tp.category)
-- Note: This requires title_principals data which is not in the faceting table
-- This query cannot be directly converted without additional setup

-- 5. Average rating per decade
-- Original: SELECT (tb.startyear / 10) * 10 AS decade, ROUND(AVG(tr.averagerating), 2) AS avg_rating FROM title_basics AS tb JOIN title_ratings AS tr ON tb.tconst = tr.tconst WHERE tb.startyear IS NOT NULL GROUP BY decade ORDER BY decade;
SELECT 
    CASE
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1900 AND 1909 THEN '1900s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1910 AND 1919 THEN '1910s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1920 AND 1929 THEN '1920s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1930 AND 1939 THEN '1930s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1940 AND 1949 THEN '1940s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1950 AND 1959 THEN '1950s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1960 AND 1969 THEN '1960s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1970 AND 1979 THEN '1970s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1980 AND 1989 THEN '1980s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 1990 AND 1999 THEN '1990s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2000 AND 2009 THEN '2000s'
        WHEN (result->'metadata'->>'startYear')::integer BETWEEN 2010 AND 2019 THEN '2010s'
        WHEN (result->'metadata'->>'startYear')::integer >= 2020 THEN '2020s'
        ELSE 'Unknown'
    END AS decade,
    ROUND(AVG((result->'metadata'->>'averageRating')::numeric), 2) AS avg_rating
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        100000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'startYear')::integer IS NOT NULL
GROUP BY decade
ORDER BY decade;

-- 6. Top 5 highest rated titles with over 10,000 votes
-- Original: SELECT tb.primarytitle, tr.averagerating, tr.numvotes FROM title_basics AS tb JOIN title_ratings AS tr ON tb.tconst = tr.tconst WHERE tr.numvotes > 10000 ORDER BY tr.averagerating DESC, tr.numvotes DESC LIMIT 5;
SELECT 
    (result->>'id')::integer AS id,
    result->'metadata'->>'primaryTitle' AS primarytitle,
    (result->'metadata'->>'averageRating')::numeric AS averagerating,
    (result->'metadata'->>'numVotes')::bigint AS numvotes,
    (result->>'combined_score')::numeric AS combined_score
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        10000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE (result->'metadata'->>'numVotes')::bigint > 10000
ORDER BY (result->'metadata'->>'averageRating')::numeric DESC,
         (result->'metadata'->>'numVotes')::bigint DESC
LIMIT 5;

-- 7. Most common genre combinations
-- Original: SELECT genres, COUNT(*) AS count FROM title_basics WHERE genres IS NOT NULL GROUP BY genres ORDER BY count DESC LIMIT 10;
SELECT 
    result->'metadata'->>'genres' AS genres,
    COUNT(*) AS count
FROM (
    SELECT jsonb_array_elements(results) AS result
    FROM facets.search_documents_with_facets(
        'providers_imdb',
        'title_basics_faceting_mv',
        '',
        NULL::jsonb,
        NULL, 'content', 'metadata', 'created_at', 'updated_at',
        100000, 0, 0.0, 0.5, 5
    )
) AS search_results
WHERE result->'metadata'->>'genres' IS NOT NULL
GROUP BY result->'metadata'->>'genres'
ORDER BY count DESC
LIMIT 10;

-- ============================================================================
-- NOTES ON CONVERSION
-- ============================================================================
-- 
-- 1. All queries now use facets.search_documents_with_facets() instead of
--    direct table queries
--
-- 2. Results are extracted from the JSONB results array returned by the function
--
-- 3. Metadata fields are accessed via JSONB operators (->, ->>)
--
-- 4. Facet filters are passed as JSONB in the p_facets parameter
--
-- 5. Text search is done via the p_query parameter (BM25 search)
--
-- 6. Some queries that require joins with title_principals cannot be fully
--    converted without additional faceting tables for principals
--
-- 7. For queries requiring large result sets, increase p_limit accordingly
--
-- 8. The function returns: results (JSONB array), facets (JSONB array),
--    total_found (bigint), and search_time (integer)
--
-- 9. To get facet counts/values, extract from the facets JSONB array
--
-- 10. Remember to create GIN indexes on the content column before using
--     these queries (see 05_faceting_indexes.sql)
--
-- ============================================================================
