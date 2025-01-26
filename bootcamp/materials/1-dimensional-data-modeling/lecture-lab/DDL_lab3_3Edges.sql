
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

WITH deduped AS (
	SELECT *, row_number() over (partition by player_id, game_id) as row_num
FROM game_details
),
	filtered AS (
		SELECT * FROM deduped
		WHERE row_num = 1
	)
	SELECT * FOR
