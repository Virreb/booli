from src.data.make_dataset import combine_to_df, fetch_data, fetch_bbox_data, load_dataset, combine_bbox_to_df
from src.data.transform_data import clean_data
from src.features.create_features import create_features, calculate_distances_to_point
from src.models import model_xgboost
import pandas as pd
pd.set_option('display.max_columns', 500)

# endpoint = 'sold'
endpoint = 'listings'
location = 'gothenburg'

# fetch_data(endpoint=endpoint, continue_from_last_area=True)
# combine_to_df(endpoint=endpoint, save_filename=endpoint)

# fetch_bbox_data(endpoint=endpoint, location=location)

# clean_data(load_filename=f'data/raw/bbox/{location}/{endpoint}',
#            save_filename=f'data/processed/bbox/{location}/{endpoint}')

# combine_bbox_to_df(loading_path=f'data/processed/bbox/{location}')
# model_xgboost.train()
df = create_features(location=location)
exit(0)

# df = load_dataset(f'processed/{endpoint}')
df = load_dataset(f'processed/bbox/{location}/all')

print(df.info())
exit(0)

lat, lon = 57.675177, 11.992865    # Hökegårdsgatan
df = calculate_distances_to_point(df=df, location=location, lat=lat, lon=lon)
df['rent_per_area'] = df['rent']/df['living_area']
df['sold_price_per_area'] = df['sold_price']/df['living_area']

# mask_close = (df['distance'] < 0.2) & (df['rooms'] == 3.0)
mask_close = df['distance'] < 0.2

cols_to_keep = ['living_area', 'rooms', 'rent', 'floor',
                'sold_date', 'sold_price', 'rent_per_area', 'sold_price_per_area']

hoke_df = df.loc[mask_close, cols_to_keep].sort_values(by=['sold_date'])
hoke_df.to_excel('hoke_dataframe.xlsx', index=False)
# mask = df['municipality'].isin(['Göteborg'])
# gbg_df = df[mask]

# create_features(save_filename='sold_gbg_features', df=df[mask])
# gbg_df = load_dataset('processed/sold_gbg_features')

print(hoke_df.head(50))
print(hoke_df.shape)

# prin(gbg_df.groupby('district')['sold_price'].mean().sort_values(ascending=False))
