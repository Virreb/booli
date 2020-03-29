from pandas import DataFrame


def clean_data(save_filename: str, load_filename: str = None, df: DataFrame = None) -> DataFrame:
    import pandas as pd

    if df is None:
        df = pd.read_pickle(f'data/interim/{load_filename}.pkl')

    # take first element of list to string (list always length 1, I think)
    # need to do this first as it creates errors having lists as values
    df['district_written'] = df['location_namedAreas'].str[0]
    df.drop(columns=['location_namedAreas'], inplace=True)

    df.drop_duplicates(inplace=True)

    # snake style column names
    rename_cols = {
        'booliId': 'booli_id',
        'livingArea': 'living_area',
        'additionalArea': 'additional_area',
        'plotArea': 'plot_area',
        'constructionYear': 'construction_year',
        'objectType': 'object_type',
        'soldDate': 'sold_date',
        'soldPrice': 'sold_price',
        'isNewConstruction': 'is_new_construction',
        'location_position_latitude': 'latitude',
        'location_position_longitude': 'longitude',
        'location_distance_ocean': 'distance_to_ocean',
        'location_address_city': 'district',
        'location_address_streetAddress': 'street_address',
        'location_region_municipalityName': 'municipality',
        'location_region_countyName': 'county',
        'listPrice': 'list_price',
        'soldPriceSource': 'sold_price_source',
        'apartmentNumber': 'apartment_number'
    }
    df.rename(columns=rename_cols, inplace=True)

    # Fix datetime
    df['published'] = pd.to_datetime(df['published'], infer_datetime_format=True)
    df['sold_date'] = pd.to_datetime(df['sold_date'], infer_datetime_format=True)

    # save to file
    df.to_pickle(f'data/processed/{save_filename}.pkl')

    return df


