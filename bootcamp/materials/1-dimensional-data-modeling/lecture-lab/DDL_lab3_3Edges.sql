
SELECT * FROM edges;

SELECT * FROM vertices v JOIN edges e
		ON v.identifier = e.subject_identifier
		AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type

-- points per game query
SELECT 
	v.properties->>'player_name',
	CAST(v.properties->>'number_of_games' as REAL)/
	CASE WHEN CAST(v.properties->>'total_points' as REAL) = 0 THEN 1
	ELSE CAST(v.properties->>'total_points' as REAL)
	END
FROM vertices v JOIN edges e
		ON v.identifier = e.subject_identifier
		AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type

-- query gives subject's game play properties as compared to object like global average, retc.
SELECT 
	v.properties->>'player_name' as subject_player_name,
	e.object_identifier,
	CAST(v.properties->>'number_of_games' as REAL)/
	CASE WHEN CAST(v.properties->>'total_points' as REAL) = 0 THEN 1
	ELSE CAST(v.properties->>'total_points' as REAL)
	END as points_per_game,
	e.properties->>'subject_points' as subject_points,
	e.properties->>'num_games' as num_games
FROM vertices v JOIN edges e
		ON v.identifier = e.subject_identifier
		AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type

--EDGES {'plays_in'}

INSERT INTO edges
WITH deduped AS (
	SELECT *, row_number() over (partition by player_id, game_id) as row_num
FROM game_details
)
	
SELECT 
	player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	game_id as object_identifier,
	'game'::vertex_type as object_type,
	'plays_in'::edge_type as edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) as properties
FROM deduped
WHERE row_num = 1

-- analytical queries
SELECT type, COUNT(1)
FROM vertices
GROUP BY 1

--
SELECT  
	v.properties->>'player_name',
	MAX(CAST(e.properties->>'pts' AS INTEGER))
	FROM vertices v JOIN edges e
	ON e.subject_identifier = v.identifier
	AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC

-- EDGE 'plays_against'
-- also includes cases where 2 players are in the same team, playing against each other
-- this creates an edge per case but we don't want that. so we aggregate it all
INSERT INTO edges
WITH deduped AS (
	SELECT *, row_number() over (partition by player_id, game_id) as row_num
FROM game_details
),
	filtered AS (
		SELECT * FROM deduped
		WHERE row_num = 1
	),
	aggregated AS (
	SELECT 
		f1.player_id as subject_player_id,
		f2.player_id as object_player_id,		
		CASE WHEN f1.team_abbreviation = f2.team_abbreviation
			 THEN 'shares_team'::edge_type
		ELSE 'plays_against'::edge_type
		END as edge_type,
-- some players might have same id but diff name. eg player changed name
			MAX(f1.player_name) as subject_player_name,
			MAX(f2.player_name) as object_player_name,
		COUNT(1) AS num_games,
		SUM(f1.pts) AS subject_points,
		SUM(f2.pts) AS object_points
	FROM filtered f1
		JOIN filtered f2
		ON f1.game_id = f2.game_id
		AND f1.player_name <> f2.player_name
-- since this is a double sided connection, similar records will show up twice
-- eg: A plays with B (rec 1), and B plays with A (rec 2) depicting same data
	WHERE f1.player_id > f2.player_id
	GROUP BY 
		f1.player_id,
		f2.player_id,
		CASE WHEN f1.team_abbreviation = f2.team_abbreviation
			 THEN 'shares_team'::edge_type
		ELSE 'plays_against'::edge_type
		END
	)
	SELECT 
		subject_player_id as subject_identifier,
		'player'::vertex_type as subject_type,
		object_player_id as object_identifier,
		'player'::vertex_type as object_type,
		edge_type as edge_type,
		json_build_object(
						'num_games', num_games,
						'subject_points', subject_points,
						'object_points', object_points
		)
	FROM aggregated
	
