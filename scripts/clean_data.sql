CREATE TEMP TABLE airyards_model_data AS
SELECT
	player_id
	, player_name
	, recent_team
	, season
	, week
	, receptions
	, targets
	, receiving_yards
	, receiving_tds
	, receiving_air_yards
	, fantasy_points_ppr as actual_fantasy_pts	
	, LAG(season_avg_racr, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_racr
	, LAG(season_avg_tgt_share, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_target_share
	, LAG(season_avg_air_yards_share, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_air_yards_share
	, LAG(season_avg_wopr, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_wopr
	, LAG(season_avg_fantasy_points_ppr, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_fantasy_points_ppr
	, LAG(recent_racr, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_racr
	, LAG(recent_tgt_share, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_target_share
	, LAG(recent_air_yards_share, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_air_yards_share
	, LAG(recent_wopr, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_wopr
	, LAG(recent_fantasy_points_ppr, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_fantasy_points_ppr
FROM
(SELECT player_id
, player_name
, recent_team
, season
, week
, receptions
, targets
, receiving_yards
, receiving_tds
, receiving_air_yards
, fantasy_points_ppr
, racr
, target_share
, air_yards_share
, wopr
, AVG(racr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS UNBOUNDED PRECEDING) as season_avg_racr
, AVG(target_share) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS UNBOUNDED PRECEDING) as season_avg_tgt_share
, AVG(air_yards_share) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS UNBOUNDED PRECEDING) as season_avg_air_yards_share
, AVG(wopr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS UNBOUNDED PRECEDING) as season_avg_wopr
, AVG(fantasy_points_ppr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS UNBOUNDED PRECEDING) as season_avg_fantasy_points_ppr
, AVG(racr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_racr
, AVG(target_share) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_tgt_share
, AVG(air_yards_share) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_air_yards_share
, AVG(wopr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_wopr
, AVG(fantasy_points_ppr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_fantasy_points_ppr
, AVG(fantasy_points_ppr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS UNBOUNDED PRECEDING)	- 
AVG(fantasy_points_ppr) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS diff					 
from nfl_weekly a
JOIN player_mapping b ON (a.player_id = b.gsis_id )
WHERE b.position IN ('RB', 'WR', 'TE')
) a
;

COPY airyards_model_data to 'C:/Users/ryanh/Documents/air_yards_model/data/airyards_data.csv' csv header;