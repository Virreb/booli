from pandas import DataFrame

IMPORTANT_PLACES = {
    'gothenburg': {
        'gotaplatsen': {
            'lat': 57.6973742,
            'lon': 11.977125
        },
        'brunnsparken': {
            'lat': 57.7067117,
            'lon': 11.9668964
        },
        'jarntorget': {
            'lat': 57.6995911,
            'lon': 11.9507111
        },
        'delsjon': {
            'lat': 57.689076,
            'lon': 12.034248
        },
        'angardsbergen': {
            'lat': 57.6718488,
            'lon': 11.9563947
        },
        'frolunda_torg': {
            'lat': 57.6537616,
            'lon': 11.9047157
        },
        'molndal_centrum': {
            'lat': 57.6551431,
            'lon': 12.0144383
        },
        'slottsskogen': {
            'lat': 57.6867615,
            'lon': 11.9312228
        },
        'gamlestadstorget': {
            'lat': 57.7295314,
            'lon': 12.0055653
        },
        'centralstationen': {
            'lat': 57.7067665,
            'lon': 11.9696058
        }
    }
}


def create_features(df: DataFrame = None, location='gothenburg') -> DataFrame:
    import pandas as pd

    if df is None:
        df = pd.read_pickle(f'data/processed/bbox/{location}/all.pkl')

    # only use sold objects
    df = df[df['sold'] == 1]

    # use lat and lon diff from central station
    df['lat_diff'] = df['latitude'] - IMPORTANT_PLACES[location]['centralstationen']['lat']
    df['lon_diff'] = df['longitude'] - IMPORTANT_PLACES[location]['centralstationen']['lon']

    # get distances in km to important places
    df = get_distances_to_important_places(location, df)

    # normalize rent and price to area
    df['rent_per_area'] = df['rent'] / df['living_area']
    df['sold_price_per_area'] = df['sold_price'] / df['living_area']

    # cols_to_drop = ['living_area', 'additional_area', 'plot_area', 'rooms', 'rent', 'floor',
    #                 'construction_year', 'object_type', 'published', 'sold_date', 'sold_price', 'district']

    # TODO: use list_price or not? maybe two different models?

    target = df.loc[:, ['sold_price', 'list_price']]

    cols_to_drop = ['published', 'booli_id', 'apartment_number', 'sold_price_source', 'url', 'street_address',
                    'latitude', 'longitude', 'county', 'source_id', 'source_type', 'source_url',
                    'location_position_isApproximate', 'district_written', 'biddingOpen', 'mortgageDeed',
                    'seniorLiving', 'listPriceChangeDate', 'listPriceChange', 'sold', 'sold_price', 'list_price']

    df.drop(columns=cols_to_drop, inplace=True)

    print(df.groupby('district')['sold_price'].count().sort_values())
    print(df.head())
    print(df.columns)
    exit()

    columns_to_one_hot = ['object_type', 'district', 'source_type', 'source_name', 'municipality']
    dummies_df = pd.get_dummies(df, columns=columns_to_one_hot)  # add areas later

    df.drop(columns=columns_to_one_hot, inplace=True)
    df = pd.concat([df, dummies_df], axis=1)

    # TODO: Look into how to OHE areas in GBG
    # TODO: Look into how to OHE seller

    df.to_pickle(f'data/features/bbox/{location}/features.pkl')

    return df


def calculate_distances_to_point(location, lat, lon, df=None):
    import pandas as pd

    if df is None:
        df = pd.read_pickle(f'data/processed/bbox{location}/all.pkl')

    df['distance'] = df.apply(lambda row: haversine(row, lat, lon), axis=1)

    return df


def haversine(row, lat1, lon1):
    from numpy import cos, sin, arcsin, sqrt
    from math import radians
    lon2 = row['longitude']
    lat2 = row['latitude']
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * arcsin(sqrt(a))
    km = 6367 * c
    return km


def get_distances_to_important_places(location, df=None, save_path=None):

    for place_name in IMPORTANT_PLACES[location]:
        place = IMPORTANT_PLACES[location][place_name]
        df[f'distance_to_{place_name}'] = df.apply(lambda row: haversine(row, place['lat'], place['lon']), axis=1)

    return df
