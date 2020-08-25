from src.data.make_dataset import fetch_data, load_dataset, combine_to_df
from src.data.transform_data import clean_data
from src.features.create_features import create_features
from src.models import model_xgboost
import pandas as pd
pd.set_option('display.max_columns', 500)

# endpoint = 'sold'
endpoint = 'listings'
location = 'gothenburg'

# fetch_data(endpoint=endpoint, location=location)

clean_data(load_filename=f'data/raw/{location}/{endpoint}',
           save_filename=f'data/processed/{location}/{endpoint}')

combine_to_df(loading_path=f'data/processed/{location}')
df = create_features(location=location)
model_xgboost.train()
