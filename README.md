This trains air yards data on the 2015-2021 season to predict fantasy points. It is used to predict players that will likely bounce back in terms of fantasy production with consistently high usage.

Summary of steps:
1. clean_data.sql pulls and formats training data from a postgres database. 
2. airyards_model_script.py further cleans the data in a format that can be used to train the model to make predictions.