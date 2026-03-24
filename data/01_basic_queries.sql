-- 01_basic_queries.sql
-- Basic Queries on IMDb Dataset

-- 1. List the first 10 movies (not adult) with their titles and start years
SELECT primaryTitle, startYear
FROM title_basics
WHERE titleType = 'movie' AND isAdult = 0
ORDER BY startYear
LIMIT 10;

-- 2. Find the top 10 highest-rated movies (with at least 100,000 votes)
SELECT b.primaryTitle, r.averageRating, r.numVotes
FROM title_basics b
JOIN title_ratings r ON b.tconst = r.tconst
WHERE b.titleType = 'movie' AND r.numVotes >= 100000
ORDER BY r.averageRating DESC
LIMIT 10;

-- 3. Get the list of genres for TV shows
SELECT DISTINCT unnest(string_to_array(genres, ',')) AS genre
FROM title_basics
WHERE titleType = 'tvSeries';

-- 4. Count the number of titles by titleType
SELECT titleType, COUNT(*) AS total_titles
FROM title_basics
GROUP BY titleType
ORDER BY total_titles DESC;

-- 5. Find all movies with more than one genre
SELECT tconst, primaryTitle, genres
FROM title_basics
WHERE titleType = 'movie'
  AND genres LIKE '%,%';

-- 6. Find all titles containing the word "Matrix" with rating ≥ 7
SELECT b.primaryTitle, r.averageRating
FROM title_basics b
JOIN title_ratings r ON b.tconst = r.tconst
WHERE b.primaryTitle ILIKE '%matrix%'
  AND r.averageRating >= 7
ORDER BY r.averageRating DESC;

-- 7. Get the top 10 most voted titles per decade
SELECT
  CASE
    WHEN startYear BETWEEN 1900 AND 1909 THEN '1900s'
    WHEN startYear BETWEEN 1910 AND 1919 THEN '1910s'
    WHEN startYear BETWEEN 1920 AND 1929 THEN '1920s'
    WHEN startYear BETWEEN 1930 AND 1939 THEN '1930s'
    WHEN startYear BETWEEN 1940 AND 1949 THEN '1940s'
    WHEN startYear BETWEEN 1950 AND 1959 THEN '1950s'
    WHEN startYear BETWEEN 1960 AND 1969 THEN '1960s'
    WHEN startYear BETWEEN 1970 AND 1979 THEN '1970s'
    WHEN startYear BETWEEN 1980 AND 1989 THEN '1980s'
    WHEN startYear BETWEEN 1990 AND 1999 THEN '1990s'
    WHEN startYear BETWEEN 2000 AND 2009 THEN '2000s'
    WHEN startYear BETWEEN 2010 AND 2019 THEN '2010s'
    WHEN startYear >= 2020 THEN '2020s'
    ELSE 'Unknown'
  END AS decade,
  b.primaryTitle,
  r.numVotes
FROM title_basics b
JOIN title_ratings r ON b.tconst = r.tconst
WHERE b.startYear IS NOT NULL
  AND b.titleType = 'movie'
  AND r.numVotes IS NOT NULL
  AND r.numVotes > 10000
ORDER BY decade, r.numVotes DESC;
