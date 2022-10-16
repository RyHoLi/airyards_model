import pandas as pd

results_df = pd.read_csv('C:\\Users\\Ryan\\Documents\\air_yards_model\\data\\week5_results.csv')
predictions_df = pd.read_csv('C:\\Users\\Ryan\\Documents\\air_yards_model\\data\\airyards_clean_predictions_wk5.csv')

final_df = predictions_df.merge(results_df, on='name', how='left')

final_df = final_df.rename(columns={'fantasy_points_ppr': 'actual_fpts', 'season_avg_fantsay_points_ppr': 'season_avg_fpts'})

final_df.to_csv('C:\\Users\\Ryan\\Documents\\air_yards_model\\data\\airyards_clean_predictions_wk5_results.csv', index=False)