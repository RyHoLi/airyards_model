drop table if exists airyards_model_data;
CREATE TEMP TABLE airyards_model_data AS
SELECT
	player_id
	, name
	, recent_team
	, season
	, week
	, CASE WHEN position = 'WR' THEN 1 ELSE 0 END AS position_wr
	, CASE WHEN position = 'TE' THEN 1 ELSE 0 END AS position_te
	, LAG(season_tot_receptions, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_tot_receptions
	, LAG(season_tot_targets, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_tot_targets
	, LAG(season_tot_receiving_yards, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_tot_receiving_yards
	, LAG(season_tot_receiving_tds, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_tot_receiving_tds
	, LAG(season_tot_receiving_air_yards, 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_tot_receiving_air_yards
	, LAG(season_avg_racr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_racr
	, LAG(season_avg_tgt_share::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_target_share
	, LAG(season_avg_air_yards_share::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_air_yards_share
	, LAG(season_avg_wopr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_wopr
	, LAG(season_avg_fantasy_points_ppr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS season_avg_fantasy_points_ppr
	, LAG(recent_racr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_racr
	, LAG(recent_tgt_share::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_target_share
	, LAG(recent_air_yards_share::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_air_yards_share
	, LAG(recent_wopr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_wopr
	, LAG(recent_fantasy_points_ppr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS recent_fantasy_points_ppr
	, LAG(fantasy_points_ppr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS lw_fantasy_points_ppr
	, LAG(wopr::DECIMAL(5,2), 2) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS lw_wopr	
	, LAG(fantasy_points_ppr::DECIMAL(5,2), 1) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS lw2_fantasy_points_ppr
	, LAG(wopr::DECIMAL(5,2), 2) OVER (PARTITION BY player_id, season ORDER BY player_id, season, week) AS lw2_wopr	
	, career_fpts::DECIMAL(5,2)
	, fantasy_points_ppr::DECIMAL(5,2) as actual_fantasy_pts	
FROM
(SELECT a.player_id
, b.name
, a.recent_team
, a.season
, a.week
, b.position
, fantasy_points_ppr
, wopr
, SUM(receptions) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receptions
, SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_targets
, SUM(receiving_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receiving_yards
, SUM(receiving_tds) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receiving_tds
, SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receiving_air_yards
, SUM(receiving_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING)
/ CASE WHEN SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) = 0 THEN 1
 ELSE SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) END as season_avg_racr
, SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_avg_tgt_share
, SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) 
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_avg_air_yards_share
, 1.5 * (SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) )
+ .7 * (SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING)
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING)) as season_avg_wopr
, AVG(fantasy_points_ppr) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_avg_fantasy_points_ppr
, SUM(receiving_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ CASE WHEN SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) = 0 THEN 1
 ELSE SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) END as recent_racr
, SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_tgt_share
, SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_air_yards_share 
, 1.5 * (SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) )
+ .7 * (SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) as recent_wopr
, AVG(fantasy_points_ppr) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_fantasy_points_ppr	
, c.career_fpts 
from nfl_weekly a
JOIN player_mapping b ON (a.player_id = b.gsis_id )
JOIN
(SELECT a.player_id, b.name, season, week, AVG(fantasy_points_ppr) OVER (PARTITION BY player_id ORDER BY player_id, season, week
																		ROWS UNBOUNDED PRECEDING) as career_fpts
FROM nfl_weekly a
JOIN player_mapping b ON (a.player_id = b.gsis_id)
) c ON (a.player_id = c.player_id
	   AND b.gsis_id = c.player_id
	   AND a.season = c.season
	   and a.week = c.week)
JOIN
(SELECT recent_team, season, week, SUM(targets) AS team_targets, SUM(receiving_air_yards) AS team_air_yards
FROM nfl_weekly a
GROUP BY 1,2,3
) d ON (a.recent_team = d.recent_team
	   AND a.season = d.season
	   and a.week = d.week)	   
WHERE b.position IN ('WR', 'TE')
AND a.season >= 2006
) a
;

drop table if exists airyards_model_predict_data;
CREATE TEMP TABLE airyards_model_predict_data AS
SELECT
	player_id
	, name
	, recent_team
	, season
	, week
	, CASE WHEN position = 'WR' THEN 1 ELSE 0 END AS position_wr
	, CASE WHEN position = 'TE' THEN 1 ELSE 0 END AS position_te
	, season_tot_receptions 
	, season_tot_targets 
	, season_tot_receiving_yards 
	, season_tot_receiving_tds 
	, season_tot_receiving_air_yards 
	, season_avg_racr::DECIMAL(5,2) 
	, season_avg_tgt_share::DECIMAL(5,2) 
	, season_avg_air_yards_share::DECIMAL(5,2) 
	, season_avg_wopr::DECIMAL(5,2) 
	, season_avg_fantasy_points_ppr::DECIMAL(5,2)
	, recent_racr::DECIMAL(5,2)
	, recent_tgt_share::DECIMAL(5,2)
	, recent_air_yards_share::DECIMAL(5,2)
	, recent_wopr::DECIMAL(5,2)
	, recent_fantasy_points_ppr::DECIMAL(5,2)
	, fantasy_points_ppr 
	, COALESCE(wopr,0) as wopr
	, LAG(fantasy_points_ppr, 1) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week) AS fantasy_points_ppr2
	, LAG(COALESCE(wopr,0),1) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week) AS wopr2
	, career_fpts::DECIMAL(5,2)::DECIMAL(5,2)
	, fantasy_points_ppr::DECIMAL(5,2) as actual_fantasy_pts	
FROM
(SELECT a.player_id
, b.name
, a.recent_team
, a.season
, a.week
, fantasy_points_ppr
, b.position
, a.wopr
, SUM(receptions) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receptions
, SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_targets
, SUM(receiving_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receiving_yards
, SUM(receiving_tds) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receiving_tds
, SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_tot_receiving_air_yards
, SUM(receiving_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING)
/ CASE WHEN SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) = 0 THEN 1
 ELSE SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) END as season_avg_racr
, SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_avg_tgt_share
, SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) 
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_avg_air_yards_share
, 1.5 * (SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) )
+ .7 * (SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING)
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING)) as season_avg_wopr
, AVG(fantasy_points_ppr) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) as season_avg_fantasy_points_ppr
, SUM(receiving_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ CASE WHEN SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) = 0 THEN 1
 ELSE SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS UNBOUNDED PRECEDING) END as recent_racr
, SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_tgt_share
, SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_air_yards_share 
, 1.5 * (SUM(targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
/ SUM(team_targets) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) )
+ .7 * (SUM(receiving_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
/ SUM(team_air_yards) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) as recent_wopr
, AVG(fantasy_points_ppr) OVER (PARTITION BY a.player_id, a.season ORDER BY a.player_id, a.season, a.week
						 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as recent_fantasy_points_ppr
, c.career_fpts 
from nfl_weekly a
JOIN player_mapping b ON (a.player_id = b.gsis_id )
JOIN
(SELECT a.player_id, b.name, season, week, AVG(fantasy_points_ppr) OVER (PARTITION BY player_id ORDER BY player_id) as career_fpts
FROM nfl_weekly a
JOIN player_mapping b ON (a.player_id = b.gsis_id)
) c ON (a.player_id = c.player_id
	   AND b.gsis_id = c.player_id
	   AND a.season = c.season
	   and a.week = c.week)
JOIN
(SELECT recent_team, season, week, SUM(targets) AS team_targets, SUM(receiving_air_yards) AS team_air_yards
FROM nfl_weekly a
GROUP BY 1,2,3
) d ON (a.recent_team = d.recent_team
	   AND a.season = d.season
	   and a.week = d.week)	   
WHERE b.position IN ('WR', 'TE')
AND a.season = 2022
) a
;

DROP TABLE IF EXISTS airyards_model_predict_data2;
CREATE TEMP TABLE airyards_model_predict_data2 AS
SELECT
	a.player_id
	, name
	, recent_team
	, season
	, 6 AS week
	, position_wr
	, position_te
	, season_tot_receptions 
	, season_tot_targets 
	, season_tot_receiving_yards 
	, season_tot_receiving_tds 
	, season_tot_receiving_air_yards 
	, season_avg_racr::DECIMAL(5,2) 
	, season_avg_tgt_share::DECIMAL(5,2) 
	, season_avg_air_yards_share::DECIMAL(5,2) 
	, season_avg_wopr::DECIMAL(5,2) 
	, season_avg_fantasy_points_ppr::DECIMAL(5,2)
	, recent_racr::DECIMAL(5,2)
	, recent_tgt_share::DECIMAL(5,2)
	, recent_air_yards_share::DECIMAL(5,2)
	, recent_wopr::DECIMAL(5,2)
	, recent_fantasy_points_ppr::DECIMAL(5,2)
	, fantasy_points_ppr::DECIMAL(5,2) as lw_fpts
	, wopr::DECIMAL(5,2)	as lw_wopr
	, fantasy_points_ppr::DECIMAL(5,2) as lw2_fpts
	, wopr::DECIMAL(5,2)	as lw2_wopr
	, career_fpts::DECIMAL(5,2)
	, actual_fantasy_pts
FROM airyards_model_predict_data a
JOIN
(SELECT player_id, MAX(week) AS week
FROM nfl_weekly
WHERE season = 2022
GROUP BY 1) b
ON (a.player_id = b.player_id
   AND a.week =  b.week)
;


COPY airyards_model_data to 'C:/Users/Ryan/Documents/air_yards_model/data/airyards_data.csv' csv header;
COPY airyards_model_predict_data2 to 'C:/Users/Ryan/Documents/air_yards_model/data/airyards_predict_data_wk6.csv' csv header;