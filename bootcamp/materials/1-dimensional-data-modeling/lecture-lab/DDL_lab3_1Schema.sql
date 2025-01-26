CREATE TYPE vertex_type
AS ENUM('player', 'team', 'game');
INSERT INTO vertices
SELECT game_id AS identifier,
		'game'::vertex_type AS type,
		json_build_object(
			'pts_home', pts_home,
			'pts_away', pts_away,
			'winning_team', CASE WHEN home_team_wins = 1 
			THEN home_team_id
			ELSE visitor_team_id 
			END
		) as properties
FROM games;

CREATE TABLE vertices (
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY (identifier, type)
)

CREATE TYPE edge_type
 AS ENUM('plays_against', 'shares_team', 'plays_in', 'plays_on')

 CREATE TABLE edges (
	subject_identifier TEXT,
	subject_type vertex_type,
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	Properties JSON,
	PRIMARY KEY (subject_identifier,
				 subject_type,
				 object_identifier,
			     object_type,
			 	 edge_type)
 )