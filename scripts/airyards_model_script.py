
# py -m pip install sklearn
# py -m pip install xgboost

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from xgboost import XGBRegressor
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import accuracy_score
import numpy as np

model_data_df = pd.read_csv('C:/Users/ryanh/Documents/air_yards_model/data/airyards_data.csv')

# predictors are all fields except for player_id, name, pred_season, pred_fantasy_points, pct_increase, breakout
df_predictors = model_data_df.iloc[:, np.r_[4:10, 11:21]]
# drop records with NaN in them, first games of each season for each player
df_predictors = df_predictors.dropna(subset=['season_avg_fantasy_points_ppr'])

# predicting fantasy_points
df_target = model_data_df.dropna(subset=['season_avg_fantasy_points_ppr'])['actual_fantasy_pts']

# not sure if this is necessary, but wanted to scale all of the predictors on a similar scale
scaler = MinMaxScaler(feature_range=(0,1))
df_predictors2 = scaler.fit_transform(df_predictors)

X_train, X_test, y_train, y_test = train_test_split(df_predictors2, df_target, test_size = 0.2, random_state=42)

model = XGBRegressor()
model = model.fit(X_train, y_train)

# make predictions for test data
y_pred = model.predict(X_test)