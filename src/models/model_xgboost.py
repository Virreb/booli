

def train(location='gothenburg', df=None):
    import pandas as pd
    from xgboost import XGBRegressor
    import time
    from sklearn.model_selection import train_test_split
    from sklearn.model_selection import KFold
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import mean_squared_error, r2_score
    from sklearn.preprocessing import StandardScaler
    from math import sqrt

    print('Training model')
    training_start_time = time.time()

    if df is None:
        df = pd.read_pickle(f'data/features/{location}/features.pkl')

    target_col = 'sold_price'
    x = df.drop(columns=[target_col]).values   # remove object type
    y = df.loc[:, target_col].values

    # TODO: check scaler
    # x = StandardScaler().fit_transform(x)
    # y = StandardScaler().fit_transform(y)

    model = XGBRegressor()
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.20, random_state=42)
    model.fit(x_train, y_train)

    y_pred = model.predict(x_test)

    # mean squared error
    mse = mean_squared_error(y_test, y_pred)
    print('Mean squared error:', round(mse, 2))

    # root mean squared error
    rmse = sqrt(mse)
    print('Root mean squared error:', round(rmse, 2))

    # coefficient of determination: 1 is perfect regression
    r2 = r2_score(y_test, y_pred)
    print('Coefficient of determination:', round(r2, 2))

    model.save_model('models/xgboost.pkl')

    print(f'Training done after {round((time.time() - training_start_time)/60, 1)}min')

