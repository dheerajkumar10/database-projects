--a) Retrieve the average ratings of the movies for each year from 2010 to 2015. 
--Consider the movies having Drama, Horror, Sci-Fi or Thriller as one of their 
--genres.  Should 6 output rows, one for each year

	SELECT  STARTYEAR, AVG(AVERAGERATING) AS AVG_RATING FROM sharmac.TITLE_RATINGS R, sharmac.TITLE_BASICS B 
	WHERE R.TCONST = B.TCONST AND TITLETYPE = 'movie' AND  STARTYEAR LIKE '201%' AND (GENRES LIKE '%Drama%' OR GENRES LIKE '%Horror%' OR GENRES LIKE '%Sci-Fi%' OR GENRES LIKE '%Thriller%')
	GROUP BY STARTYEAR 
	ORDER BY STARTYEAR
	FETCH FIRST 6 ROWS ONLY; 


--b) Retrieve the average ratings of the movies by genre for each year during 
--2010 and 2015 for each genre, Drama, Horror, Sci-Fi and Thriller. 
--Should have 24 rows of output, for 6 years and 4 genres. 

SELECT STARTYEAR,Genre,YEARLY_AVG FROM (
SELECT  STARTYEAR,Genre,AVG(AVERAGERATING) AS YEARLY_AVG FROM (
(SELECT  STARTYEAR,'Drama'as Genre,AVERAGERATING FROM sharmac.TITLE_RATINGS R, sharmac.TITLE_BASICS B 
WHERE R.TCONST = B.TCONST AND TITLETYPE = 'movie' AND  STARTYEAR LIKE '201%' AND GENRES LIKE '%Drama%') 
UNION ALL
(SELECT  STARTYEAR,'Horror'as Genre,AVERAGERATING FROM sharmac.TITLE_RATINGS R, sharmac.TITLE_BASICS B 
WHERE R.TCONST = B.TCONST AND TITLETYPE = 'movie' AND  STARTYEAR LIKE '201%' AND GENRES LIKE '%Horror%') 
UNION ALL
(SELECT  STARTYEAR,'Sci-Fi'as Genre,AVERAGERATING FROM sharmac.TITLE_RATINGS R, sharmac.TITLE_BASICS B 
WHERE R.TCONST = B.TCONST AND TITLETYPE = 'movie' AND  STARTYEAR LIKE '201%' AND GENRES LIKE '%Sci-Fi%' )
UNION ALL
(SELECT  STARTYEAR,'Thriller'as Genre,AVERAGERATING FROM sharmac.TITLE_RATINGS R, sharmac.TITLE_BASICS B 
WHERE R.TCONST = B.TCONST AND TITLETYPE = 'movie' AND  STARTYEAR LIKE '201%' AND GENRES LIKE '%Thriller%'))
GROUP BY ROLLUP(STARTYEAR,Genre))
WHERE Genre IS NOT NULL
ORDER BY STARTYEAR
FETCH FIRST 24 ROWS ONLY;