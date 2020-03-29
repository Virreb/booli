from src.data.make_dataset import combine_to_df, fetch_data, load_dataset
import pandas as pd
pd.set_option('display.max_columns', 500)

# fetch_data('listings', limit=500, continue_from_last_area=True, stop_at_area_id=15000)
# combine_to_df(endpoint='listings', save_name='listings')
# exit(0)
df = load_dataset(save_name='sold')
# print(df['location_namedAreas'].str[0].head())
# print(df['location_namedAreas'].apply(lambda x: x[0]).head())

df['area'] = df['location_namedAreas'].str[0]
df = df.drop(columns=['location_namedAreas'])
# print(df.columns)
print(df.info())
print(df.describe())
print(df.head(5))
# print(df['area'].unique())
exit(0)

# print(df['location_region_countyName'].head(20))
# print(df['location_region_municipalityName'].head(20))

# print(df.shape, df.drop_duplicates().shape, df['booliId'].nunique())
# exit(0)
gbg_mask = df['location_region_municipalityName'].isin(['Göteborg'])
# gbg_mask = df['location_region_municipalityName'].isin(['Öckerö'])
gbg_df = df[gbg_mask].sort_values('booliId')
# print(gbg_df['location_namedAreas'].head(20))
print(gbg_df['area'].unique())
print(gbg_df['area'].nunique())
print(gbg_df['location_address_city'].unique())
print(gbg_df['location_address_city'].nunique())
# exit(0)
print(gbg_df.shape, gbg_df.drop_duplicates().shape, gbg_df['booliId'].nunique())
# print(gbg_df.head(20))
print(gbg_df.groupby('location_address_city')['listPrice'].count().sort_values(ascending=False).head(21))
print(gbg_df.groupby('area')['listPrice'].count().sort_values(ascending=False).head(21))
