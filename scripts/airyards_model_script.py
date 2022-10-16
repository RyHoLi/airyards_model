
# py -m pip install sklearn
# py -m pip install xgboost
# pd.set_option('display.max_columns', None)

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from xgboost import XGBRegressor
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import accuracy_score
import numpy as np
from sklearn import linear_model
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor
import joblib

model_data_df = pd.read_csv('C:/Users/Ryan/Documents/air_yards_model/data/airyards_data.csv')
model_data_df_train = model_data_df[(model_data_df['season'] < 2022)]

# predictors are all fields except for player_id, name, pred_season, pred_fantasy_points, pct_increase, breakout
df_predictors = model_data_df_train.iloc[:, np.r_[3:27]]
# drop records with NaN in them, first games of each season for each player
df_predictors = df_predictors.dropna(subset=['season_avg_racr'])
df_predictors = df_predictors.dropna(subset=['recent_racr'])
df_predictors = df_predictors.dropna(subset=['lw_wopr'])

# predicting fantasy_points
df_target = model_data_df_train.dropna(subset=['season_avg_racr'])['actual_fantasy_pts']
df_target = model_data_df_train.dropna(subset=['recent_racr'])['actual_fantasy_pts']
df_target = model_data_df_train.dropna(subset=['lw_wopr'])['actual_fantasy_pts']

# scale all of the predictors
scaler = MinMaxScaler(feature_range=(0,1))
df_predictors2 = scaler.fit_transform(df_predictors)

X_train, X_test, y_train, y_test = train_test_split(df_predictors2, df_target, test_size = 0.2, random_state=23)

# Function to get model results
def print_results(results):
    print('BEST PARAMS: {}\n'.format(results.best_params_))
    
    means = results.cv_results_['mean_test_score']
    stds = results.cv_results_['std_test_score']
    for mean, std, params in zip(means, stds, results.cv_results_['params']):
        print('{} (+/-{}) for {}'.format(round(mean, 3), round(std * 2, 3), params))

model_path = 'C:/Users/Ryan/Documents/air_yards_model/scripts/'


#RF model
rfr = RandomForestRegressor()
parameters = {
    'n_estimators': [5, 10, 50, 100, 200],
    'max_depth': [2,4,8,16,32, None]
}

cv = GridSearchCV(rfr, parameters, cv=5)
cv.fit(X_train, y_train.values.ravel())
print_results(cv)

joblib.dump(cv.best_estimator_, f'{model_path}Airyards_RF.pkl')
model_rf = joblib.load(f'{model_path}Airyards_RF.pkl')

# make predictions on test data
y_pred_rf = model_rf.predict(X_test)
corr_matrix_rf = np.corrcoef(y_test, y_pred_rf)
corr_rf = corr_matrix_rf[0,1]
R_sq_rf = corr_rf**2
print(R_sq_rf)

'''
R^2 = 0.2826857471840979
'''

# GBM model
from sklearn.ensemble import GradientBoostingRegressor
gb = GradientBoostingRegressor()
parameters = {
    'n_estimators': [5,50, 100],
    'max_depth': [1,3,5,7],
    'learning_rate': [0.01, 0.1, 1]
}

cv = GridSearchCV(gb, parameters, cv=5)
cv.fit(X_train, y_train.values.ravel())
print_results(cv)

joblib.dump(cv.best_estimator_, f'{model_path}Airyards_GBM.pkl')
model_gbm = joblib.load(f'{model_path}Airyards_GBM.pkl')

# make predictions on test data
y_pred_gbm = model_gbm.predict(X_test)
corr_matrix_gbm = np.corrcoef(y_test, y_pred_gbm)
corr_gbm = corr_matrix_gbm[0,1]
R_sq_gbm = corr_gbm**2
print(R_sq_gbm)
'''
R^2 = 0.2783655990061132
'''


# predict on most recent data
scaler = MinMaxScaler(feature_range=(0,1))
predict_data = pd.read_csv('C:/Users/Ryan/Documents/air_yards_model/data/Week_6/airyards_predict_data_wk6.csv')
predict_data2 = predict_data.iloc[:, np.r_[3:27]]
predict_data3 = predict_data2.dropna().reset_index(drop=True)
predict_data4 = scaler.fit_transform(predict_data3)

predictions_rf = pd.DataFrame(model_rf.predict(predict_data4), columns=['pred_fpts_rf'])
predictions_gbm = pd.DataFrame(model_gbm.predict(predict_data4), columns=['pred_fpts_gbm'])

final_df = pd.concat([predict_data.iloc[:, :-1].reset_index(drop=True), predictions_rf, predictions_gbm], axis=1)
final_df['pred_fpts'] = (final_df['pred_fpts_rf'] + final_df['pred_fpts_gbm'])/2
final_df['difference'] =  final_df['pred_fpts'] - final_df['season_avg_fantasy_points_ppr']
final_df_results = final_df[['name', 'season_avg_fantasy_points_ppr', 'pred_fpts', 'difference']]

final_df.to_csv('C:/Users/Ryan/Documents/air_yards_model/data/Week_6/airyards_predictions_wk6.csv', index=False)
final_df_results.to_csv('C:/Users/Ryan/Documents/air_yards_model/data/Week_6/airyards_clean_predictions_wk6.csv', index=False)