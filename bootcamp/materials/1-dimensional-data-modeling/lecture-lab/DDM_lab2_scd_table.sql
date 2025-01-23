-- -- DROP TABLE players;
-- --CREATE TYPE scoring_class AS ENUM ('star', 'good', 'avg', 'bad');
-- -- CREATE TABLE players (
-- --      player_name TEXT,
-- --      height TEXT,
-- --      college TEXT,
-- --      country TEXT,
-- --      draft_year TEXT,
-- --      draft_round TEXT,
-- --      draft_number TEXT,
-- --      seasons season_stats[],
-- --      scoring_class scoring_class,
-- --      years_since_last_active INTEGER,
-- --      is_active BOOLEAN,
-- --      current_season INTEGER,
-- --      PRIMARY KEY (player_name, current_season)
-- --  );

-- INSERT INTO players
-- WITH years AS (
--     SELECT *
--     FROM GENERATE_SERIES(1996, 2022) AS season
-- ), p AS (
--     SELECT
--         player_name,
--         MIN(season) AS first_season
--     FROM player_seasons
--     GROUP BY player_name
-- ), players_and_seasons AS (
--     SELECT *
--     FROM p
--     JOIN years y
--         ON p.first_season <= y.season
-- ), windowed AS (
--     SELECT
--         pas.player_name,
--         pas.season,
--         ARRAY_REMOVE(
--             ARRAY_AGG(
--                 CASE
--                     WHEN ps.season IS NOT NULL
--                         THEN ROW(
--                             ps.season,
--                             ps.gp,
--                             ps.pts,
--                             ps.reb,
--                             ps.ast
--                         )::season_stats
--                 END)
--             OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
--             NULL
--         ) AS seasons
--     FROM players_and_seasons pas
--     LEFT JOIN player_seasons ps
--         ON pas.player_name = ps.player_name
--         AND pas.season = ps.season
--     ORDER BY pas.player_name, pas.season
-- ), static AS (
--     SELECT
--         player_name,
--         MAX(height) AS height,
--         MAX(college) AS college,
--         MAX(country) AS country,
--         MAX(draft_year) AS draft_year,
--         MAX(draft_round) AS draft_round,
--         MAX(draft_number) AS draft_number
--     FROM player_seasons
--     GROUP BY player_name
-- )
-- SELECT
--     w.player_name,
--     s.height,
--     s.college,
--     s.country,
--     s.draft_year,
--     s.draft_round,
--     s.draft_number,
--     seasons AS season_stats,
--     CASE
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'avg'
--         ELSE 'bad'
--     END::scoring_class AS scoring_class,
--     w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
--     (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active,
-- 	w.season
-- FROM windowed w
-- JOIN static s
--     ON w.player_name = s.player_name;

-- create table with SCDs
-- CREATE TABLE players_scd (
-- 	player_name TEXT,
-- 	scoring_class scoring_class,
-- 	is_active BOOLEAN,
-- 	current_season INTEGER,
-- 	start_season INTEGER,
-- 	end_season INTEGER,
-- 	PRIMARY KEY(player_name, start_season)
-- );

-- creating SCD table that track changes in multiple columns
-- using window function LAG to see the dimension before
-- creating CTE
INSERT INTO players_scd
WITH with_previous AS (
SELECT 
	player_name, 
	current_season,
	scoring_class,
	is_active,
	LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
	LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active	
FROM players
WHERE current_season <= 2021
),

-- calculate the streak of a player, like how long were they
-- in a current dimension


-- club 2 indicators into 1, just tracking change in scoring_class or is_active
with_indicators AS (
	SELECT *, 
			CASE 
				WHEN scoring_class <> previous_scoring_class THEN 1 
				WHEN is_active <> previous_is_active THEN 1 
				ELSE 0 
			END AS change_indicator
	FROM with_previous
	),
	with_streaks AS (
		SELECT *,
				SUM(change_indicator) 
					OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM with_indicators
	)

--
-- ps: hard coding 2021 here to build incrementally for next season
	SELECT player_name,
-- commenting out "streak_identifier from schema as its not in original players_scd table"
		   -- streak_identifier,
		   scoring_class,
		   is_active,
		   MIN(current_season) as start_season,
		   MAX(current_season) as end_season,
		   2021 AS current_season
		  FROM with_streaks
	GROUP BY player_name, streak_identifier, is_active, scoring_class
	ORDER BY player_name, streak_identifier

SELECT * FROM players_scd;

--Limitations of query above:
-- expensive in terms of time as using window ffunctions 
-- LAG, SUM, MIN, MAX, GROUP BY
-- create an indicator whether or not dimension is changed (v0)
-- SELECT *, 
-- 			CASE 
-- 				WHEN scoring_class <> previous_scoring_class THEN 1 
-- 				ELSE 0 
-- 			END AS scoring_class_change_indicator,
-- 			CASE 
-- 				WHEN is_active <> previous_is_active THEN 1 
-- 				ELSE 0 
-- 			END AS is_active_change_indicator
-- FROM with_previous;



















