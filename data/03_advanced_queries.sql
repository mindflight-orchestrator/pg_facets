-- 1. Top 3 decades with the highest number of published titles
SELECT 
  (startyear / 10) * 10 AS decade,
  COUNT(*) AS total_titles
FROM title_basics
WHERE startyear IS NOT NULL
GROUP BY decade
ORDER BY total_titles DESC
LIMIT 3;

-- 2. For each decade, show the title with the highest number of votes
SELECT decade, primarytitle, numvotes
FROM (
  SELECT 
    (tb.startyear / 10) * 10 AS decade,
    tb.primarytitle,
    tr.numvotes,
    RANK() OVER (PARTITION BY (tb.startyear / 10) * 10 ORDER BY tr.numvotes DESC) AS rank_vote
  FROM title_basics AS tb
  JOIN title_ratings AS tr ON tb.tconst = tr.tconst
  WHERE tb.startyear IS NOT NULL
) AS ranked_votes
WHERE rank_vote = 1
ORDER BY decade;

-- 3. Find people who have acted and directed at least once
SELECT nb.primaryname
FROM name_basics AS nb
JOIN title_principals AS tp1 ON nb.nconst = tp1.nconst AND tp1.category = 'actor'
JOIN title_principals AS tp2 ON nb.nconst = tp2.nconst AND tp2.category = 'director'
GROUP BY nb.primaryname
LIMIT 10;

-- 4. Show titles that have more than one type of contributor (e.g., actor, director, writer)
SELECT tb.primarytitle, COUNT(DISTINCT tp.category) AS categories_count
FROM title_basics AS tb
JOIN title_principals AS tp ON tb.tconst = tp.tconst
GROUP BY tb.primarytitle
HAVING COUNT(DISTINCT tp.category) > 1
ORDER BY categories_count DESC
LIMIT 10;

-- 5. Average rating per decade
SELECT 
  (tb.startyear / 10) * 10 AS decade,
  ROUND(AVG(tr.averagerating), 2) AS avg_rating
FROM title_basics AS tb
JOIN title_ratings AS tr ON tb.tconst = tr.tconst
WHERE tb.startyear IS NOT NULL
GROUP BY decade
ORDER BY decade;

-- 6. Top 5 highest rated titles with over 10,000 votes
SELECT tb.primarytitle, tr.averagerating, tr.numvotes
FROM title_basics AS tb
JOIN title_ratings AS tr ON tb.tconst = tr.tconst
WHERE tr.numvotes > 10000
ORDER BY tr.averagerating DESC, tr.numvotes DESC
LIMIT 5;

-- 7. Most common genre combinations
SELECT genres, COUNT(*) AS count
FROM title_basics
WHERE genres IS NOT NULL
GROUP BY genres
ORDER BY count DESC
LIMIT 10;
