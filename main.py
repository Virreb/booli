from src.data.make_dataset import combine_to_df, fetch_data, load_dataset

# fetch_data('sold', limit=500, continue_from_last_area=True, start_from_area_id=13646, stop_at_area_id=15000)
# combine_to_df(endpoint='sold', save_name='sold')

df = load_dataset(save_name='sold')

print(df.columns)
print(df.info())


# print(df['location_region_countyName'].head(20))
# print(df['location_region_municipalityName'].head(20))

# print(df.shape, df.drop_duplicates().shape, df['booliId'].nunique())
exit(0)
gbg_mask = df['location_region_municipalityName'].isin(['Göteborg', 'Mölndal'])
gbg_df = df[gbg_mask]
# print(gbg_df['location_namedAreas'].head(20))
print(gbg_df['location_namedAreas'].unique())
print(gbg_df['location_namedAreas'].nunique())
