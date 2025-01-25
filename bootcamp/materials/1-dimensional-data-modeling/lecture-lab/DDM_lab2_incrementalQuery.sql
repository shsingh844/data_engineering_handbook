-- Incremental query
-- CREATE TYPE scd_type AS (
-- 	scoring_class scoring_class,
-- 	is_active BOOLEAN,
-- 	start_season INTEGER,
-- 	end_season INTEGER
-- )

WITH last_season_scd AS (
	SELECT * FROM players_scd
	WHERE current_season = 2021
	AND end_season = 2021
),
	historical_scd AS (
		SELECT 
			player_name,
			scoring_class,
			is_active,
			start_season,
			end_season		
		FROM players_scd
		WHERE current_season = 2021
		AND end_season < 2021
	),
	this_season_data AS (
		SELECT * FROM players
		WHERE current_season = 2022
	),
-- this will include changed and new records
-- SELECT 
-- 	ts. player_name,
-- 	ts.scoring_class, ts.is_active,
-- 	ls.scoring_class, ls.is_active
-- FROM this_season_data ts
-- 	LEFT JOIN last_season_scd ls
-- 	ON ts.player_name = ls.player_name

-- this will include changed, new, and unchanged 
-- for the records that don't change, we increase it by 1
	unchanged_records AS (
		SELECT 
			ts. player_name,
			ts.scoring_class, 
			ts.is_active,
			ls.start_season, 
			ts.current_season as end_season
		FROM this_season_data ts
		JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
		WHERE ts.scoring_class = ls.scoring_class
		AND ts.is_active = ls.is_active
	),

-- changed record
-- changed rec will have 2 elements:
-- recs that changed, recs that closed (eg retired player)

	changed_records AS (
		SELECT ts. player_name,
			UNNEST(ARRAY[
					ROW(
						ls.scoring_class,
						ls.is_active,
						ls.start_season,
						ls.end_season
					)::scd_type,
					ROW(
						ts.scoring_class,
						ts.is_active,
						ts.current_season,
						ts.current_season
					)::scd_type
			]) as records
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
		WHERE ts.scoring_class <> ls.scoring_class
		OR ts.is_active <> ls.is_active
	),

-- to flatten out the table
-- cause array will contain recs like (bad, true, 2001, 2022)
-- unnesting will put them into their own columns
	unnested_changed_records AS (
		SELECT player_name,
		(records::scd_type).scoring_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season
		FROM changed_records
	),

-- new records
	new_records AS (
		SELECT 
				ts.player_name,
				ts.scoring_class,
				ts.is_active,
				ts.current_season AS start_season,
				ts.current_season AS end_season
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
		WHERE ls.player_name IS NULL
	)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records

-- this query excludes many assumption like what is scoring_class 
-- or is_active is null? query will break
-- since we depend on yesterday's data, its hard to backfill

