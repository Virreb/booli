from src.data.make_dataset import combine_to_df, fetch_data, load_dataset
from src.data.transform_data import clean_data
from src.features.create_features import create_features

import pandas as pd
pd.set_option('display.max_columns', 500)

endpoint = 'sold'
# fetch_data(endpoint=endpoint, continue_from_last_area=True)
# combine_to_df(endpoint=endpoint, save_filename=endpoint)
# clean_data(save_filename=endpoint, load_filename=endpoint)

df = load_dataset(f'processed/{endpoint}')
mask = df['municipality'].isin(['Göteborg'])
gbg_df = df[mask]

# create_features(save_filename='sold_gbg_features', df=df[mask])
# gbg_df = load_dataset('processed/sold_gbg_features')

print(gbg_df.head())
print(gbg_df.shape)

print(gbg_df.groupby('district')['sold_price'].mean().sort_values(ascending=False))
