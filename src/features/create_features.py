from pandas import DataFrame


def create_features(save_filename: str, df: DataFrame = None) -> DataFrame:
    import pandas as pd

    cols_to_keep = ['living_area', 'additional_area', 'plot_area', 'rooms', 'rent', 'floor',
                    'construction_year', 'object_type', 'published', 'sold_date', 'sold_price', 'district']

    df = df[cols_to_keep]

    columns_to_one_hot = ['object_type', 'district']
    dummies_df = pd.get_dummies(df, columns=columns_to_one_hot, prefix=['otype', 'district'])  # add areas later

    df.drop(columns=columns_to_one_hot, inplace=True)
    df = pd.concat([df, dummies_df], axis=1)

    # TODO: Look into how to OHE areas in GBG
    # TODO: Look into how to OHE seller

    df.to_pickle(f'data/processed/{save_filename}.pkl')

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

