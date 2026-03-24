-- 1. List the 5 actors with the most appearances in the dataset
SELECT nb.primaryname, COUNT(*) AS appearances
FROM title_principals AS tp
JOIN name_basics AS nb ON tp.nconst = nb.nconst
WHERE tp.category = 'actor'
GROUP BY nb.primaryname
ORDER BY appearances DESC
LIMIT 5;

-- 2. Show the number of titles released each year from 1990 to 2020
SELECT startyear, COUNT(*) AS total_titles
FROM title_basics
WHERE startyear BETWEEN 1990 AND 2020
GROUP BY startyear
ORDER BY startyear;

-- 3. Find all titles that belong to both 'Action' and 'Thriller' genres
SELECT primarytitle, genres
FROM title_basics
WHERE genres ILIKE '%Action%' AND genres ILIKE '%Thriller%'
LIMIT 10;

-- 4. Count how many titles belong to more than one genre
SELECT COUNT(*) AS multi_genre_titles
FROM title_basics
WHERE genres ILIKE '%,%';

-- 5. Find titles containing the word 'Matrix' with rating >= 7
SELECT tb.primarytitle, tr.averagerating
FROM title_basics AS tb
JOIN title_ratings AS tr ON tb.tconst = tr.tconst
WHERE tb.primarytitle ILIKE '%Matrix%' AND tr.averagerating >= 7;

-- 6. List the top 5 directors by number of titles they directed
SELECT nb.primaryname, COUNT(*) AS total_directed
FROM title_principals AS tp
JOIN name_basics AS nb ON tp.nconst = nb.nconst
WHERE tp.category = 'director'
GROUP BY nb.primaryname
ORDER BY total_directed DESC
LIMIT 5;

-- 7. Titles with more than 1 person credited as principal cast
SELECT t.tconst, t.primarytitle, COUNT(tp.nconst) AS num_people
FROM title_basics AS t
JOIN title_principals AS tp ON t.tconst = tp.tconst
GROUP BY t.tconst, t.primarytitle
HAVING COUNT(tp.nconst) > 1
ORDER BY num_people DESC
LIMIT 10;
