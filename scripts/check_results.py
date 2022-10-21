import pandas as pd
import psycopg2
import pandas.io.sql as sqlio

conn = psycopg2.connect(database='nfl_viz_proj', user = 'postgres', password ='Worksmarter1$', host='localhost', port ='5432')
conn.autocommit=True
cursor = conn.cursor()

sql_query = '''SELECT
name
, fantasy_points_ppr
FROM nfl_weekly a
JOIN player_mapping b
ON a.player_id = b.gsis_id
WHERE week = 5
AND season = 2022;'''
  
results_df = sqlio.read_sql_query(sql_query,conn)
conn = None

predictions_df = pd.read_csv('C:\\Users\\Ryan\\Documents\\air_yards_model\\data\\Week_6\\airyards_clean_predictions_wk6.csv')

final_df = predictions_df.merge(results_df, on='name', how='left')

final_df = final_df.rename(columns={'fantasy_points_ppr': 'actual_fpts', 'season_avg_fantsay_points_ppr': 'season_avg_fpts'})

final_df.to_csv('C:\\Users\\Ryan\\Documents\\air_yards_model\\data\\Week_6\\airyards_clean_predictions_wk6_results.csv', index=False)