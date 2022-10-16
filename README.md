This trains air yards data on the 2006-2021 season to predict fantasy points based on usage stats like air yards, target share, and wopr. It is used to predict players that will likely bounce back in terms of fantasy production with usage that is usually consistent. The Player Trend.ipynb file shows that this trend exists in a few sample WRs.

Predictors used:
- Season
- Week
- Position (WR/TE)
- Total Receptions in the season until each week
- Total Targets in the season until each week
- Total Receiving Yards in the season until each week
- Total Receiving TDs in the season until each week
- Total Air Yards in the season until each week
- Season Average RACR until each week
- Season Average Target Share until each week
- Season Average Air Yards Share until each week
- Season Average WOPR until each week
- Season Average Fantasy Pts PPR
- Recent (last 3 weeks) RACR until each week
- Recent Target Share until each week
- Recent Air Yards Share until each week
- Recent WOPR until each week
- Recent Fantasy Points Scored until each week
- Career Average Fantasy Points until each week
- Last week's fantasy points scored
- Last week's wopr
- Two weeks ago's fantasy points scored
- Two weeks ago's wopr

Summary of steps:
1. The script clean_data.sql pulls and formats training data from a postgres database. The career stats begin from 2000 whereas the training data begins from 2006.
1a. The query also pulls the data to be predicted for each week.
2. The airyards_model_script.py script further cleans the data in a format that can be used to train the model to make predictions and then makes predictions for each week.


