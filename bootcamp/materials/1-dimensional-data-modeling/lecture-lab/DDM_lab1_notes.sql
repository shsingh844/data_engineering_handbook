-- SELECT * FROM player_seasons;
-- --Create struct 
-- CREATE TYPE season_stats AS (
-- 						season INTEGER,
-- 						gp REAL,
-- 						pts REAL,
-- 						reb REAL,
-- 						ast REAL
-- 					)
-- -- create table with player stats (non changing) + season_stats + current_season column
-- current_season will hold latest value as we perform OUTER JOINS in future
-- CREATE TABLE players (
-- 		player_name TEXT,
-- 		height TEXT,
-- 		college TEXT,
-- 		country TEXT,
-- 		draft_year TEXT,
-- 		draft_round TEXT,
-- 		draft_number TEXT,
-- 		season_stats season_stats[],
-- 		current_season INTEGER,
-- 		PRIMARY KEY(player_name, current_season)
-- )

-- FULL OUTER JOIN Logic
-- find first year when seasons started
--SELECT MIN(season) FROM player_seasons;
-- Get cumulation between today and yesterday
-- This is called "Seed Query", where regardless of multiple nulls you get, 
-- you want to check if the query is returning full outer join result as expected.
-- INSERT INTO players
-- WITH yesterday AS (
-- 	SELECT * FROM players
-- 	WHERE current_season = 2000
-- ),
-- 	today AS (
-- 		SELECT * FROM player_seasons
-- 		WHERE season = 2001
-- 	 )
	 
-- SELECT
-- -- using coalesce to add non-null value(s) since these attributes are non changing
-- 	COALESCE(t.player_name, y.player_name) AS player_name,
-- 	COALESCE(t.height, y.height) AS height,
-- 	COALESCE(t.college, y.college) AS college,
-- 	COALESCE(t.country, y.country) AS country,
-- 	COALESCE(t.draft_year, y.draft_year) AS draft_year,
-- 	COALESCE(t.draft_round, y.draft_round) AS draft_round,
-- 	COALESCE(t.draft_number, y.draft_number) AS draft_number,
-- for temporal deminsions, we do case by case
-- case 1: if yesterday value is NULL, then show season stats as an array of
-- attributes season, gp, pts, reb, ast from today
-- :: is used to cast the result of this query as a data type struct, as defined earlier
-- with formation of TYPE season_stats
-- case 2: Else, we concat the values in t and y season_stats array
-- case 3: if t.season value is NULL ie a retired player who played yesterday but not today
-- 	CASE WHEN y.season_stats IS NULL 
-- 		THEN ARRAY[ROW(
-- 					t.season, t.gp, t.pts, t.reb, t.ast
-- 		)::season_stats]
-- 		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
-- 					t.season, t.gp, t.pts, t.reb, t.ast
-- 		)::season_stats]
-- 		ELSE y.season_stats
-- 		END as season_stats,
-- 		COALESCE(t.season, y.current_season + 1) AS current_season

-- 	FROM today t FULL OUTER JOIN yesterday y
-- 	ON t.player_name = y.player_name

-- SELECT * from players;

-- use case: flattened out table giving insights into a player's stats
-- SELECT * FROM players WHERE current_season = 2001 AND player_name = 'Michael Jordan'

--use case: "UNNEST" to explode seasonal_stat array into individual rows
-- SELECT player_name,
	-- 	UNNEST(season_stats) AS season_stats
	-- 	FROM players	
	-- WHERE current_season = 2001 
	-- AND player_name = 'Michael Jordan'

-- use case: explode season_stats further, which each attribute in a row casted in a column using CTE
-- WITH unnested AS (
-- 	SELECT player_name,
-- 		UNNEST(season_stats) AS season_stats
-- 		FROM players	
-- 	WHERE current_season = 2001 
-- 	AND player_name = 'Michael Jordan'
-- )

-- SELECT player_name, (season_stats::season_stats).*
-- FROM unnested

-- use case: Solving runlength encoding issue, as now that we run above query for whole dataset, 
-- it maintains the sorting.
-- WITH unnested AS (
-- 	SELECT player_name,
-- 		UNNEST(season_stats) AS season_stats
-- 		FROM players	
-- 	WHERE current_season = 2001 
-- )

-- SELECT player_name, (season_stats::season_stats).*
-- FROM unnested

-- Analytical queries
-- starting fresh to edit the table "players" for analytical queries
-- DROP TABLE players;

-----------------------------------------------------------------------------------------------------------------------------

-- creating "scoring_class" to rate a player based on attribute "pts"
-- "years_since_last_played" dimension will add insight about gap
-- CREATE TYPE scoring_class AS ENUM ('star', 'good', 'avg', 'bad');
-- drop table players;
-- CREATE TABLE players (
-- 		player_name TEXT,
-- 		height TEXT,
-- 		college TEXT,
-- 		country TEXT,
-- 		draft_year TEXT,
-- 		draft_round TEXT,
-- 		draft_number TEXT,
-- 		season_stats season_stats[],
-- 		scoring_class scoring_class,
-- 		years_since_last_season INTEGER,
-- 		current_season INTEGER,
-- 		PRIMARY KEY(player_name, current_season)
-- );

INSERT INTO players
WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000
),
	today AS (
		SELECT * FROM player_seasons
		WHERE season = 2001
	 )
	 
SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS height,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,

	CASE WHEN y.season_stats IS NULL 
		THEN ARRAY[ROW(
					t.season, t.gp, t.pts, t.reb, t.ast
		)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
					t.season, t.gp, t.pts, t.reb, t.ast
		)::season_stats]
		ELSE y.season_stats
		END as season_stats,

-- case 4: scoring class based on t.pts. If player is retired, 
-- pts are pulled from their last record
		CASE 
			WHEN t.season IS NOT NULL THEN
				CASE WHEN t.pts > 20 THEN 'star'
					WHEN t.pts > 15 THEN 'good'
					WHEN t.pts > 10 THEN 'avg'
					ELSE 'bad'
				END::scoring_class
			ELSE y.scoring_class
		END AS scoring_class,

-- case 4(a): if y.scoring_class is NULL, for previous year, pull last non-null value
		
-- case 5: player currently active, took break earlier, but how long?
		CASE WHEN t.season IS NOT NULL THEN 0
			ELSE y.years_since_last_season + 1
		END AS years_since_last_played,
		COALESCE(t.season, y.current_season + 1) AS current_season

	FROM today t FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name

-- Basic analytical query to pull records with rating and gap years
SELECT * FROM players 
WHERE current_season = 2001
AND player_name = 'Michael Jordan'

-- compare earliest and latest season_stats
SELECT player_name,
		season_stats[1]::season_stats AS first_season,
		season_stats[CARDINALITY(season_stats)]::season_stats AS latest_season
FROM players

-- generate a metric showing "how much times a player improved" in terms of "pts"
SELECT player_name,
		(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
		CASE WHEN
			(season_stats[1]::season_stats).pts = 0 THEN 1
			ELSE (season_stats[1]::season_stats).pts	
		END
FROM players WHERE current_season = 2001 AND scoring_class = 'star'
ORDER BY 2 DESC
-- This query is super fast because we are not using GROUP BY, or aggregations like MIN, MAX
-- This cumulative pattern is super powerful because:
	-- it gives you incremental history
	-- access to historical analysis
	-- avoids shuffle to get result
