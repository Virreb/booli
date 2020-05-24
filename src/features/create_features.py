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
    import datetime
    from tqdm import tqdm
    import time
    import multiprocessing
    from joblib import Parallel, delayed

    print('Calculating features')
    features_start_time = time.time()

    nbr_cores = multiprocessing.cpu_count()
    nbr_workers = max([nbr_cores - 1, 1])  # have at least one worker

    if df is None:
        df = pd.read_pickle(f'data/processed/bbox/{location}/all.pkl')

    # only use sold objects
    df = df[df['sold'] == 1]

    # only use apartments
    # 1.7k radhus, 5k villa, 44k lägenhet, 0.5k fritidshus, 38 gård, 361 kedjehus, 340 parhus, 327 tomt/mark
    df = df[df['object_type'] == 'Lägenhet']

    # only use apartments in central GBG while testing
    df = calculate_distances_to_point(lat=57.68187, lon=11.95904, df=df)
    df = df[df['distance'] <= 3]

    # use lat and lon diff from central station
    df['lat_diff'] = df['latitude'] - IMPORTANT_PLACES[location]['centralstationen']['lat']
    df['lon_diff'] = df['longitude'] - IMPORTANT_PLACES[location]['centralstationen']['lon']

    # get distances in km to important places
    df = get_distances_to_important_places(location, df)

    # normalize rent and price to area
    df['rent_per_area'] = df['rent'] / df['living_area']
    df['sold_price_per_area'] = df['sold_price'] / df['living_area']

    # don't use small agencies
    agencies_df = df.groupby('source_name')['source_name'].count()
    small_agencies_list = agencies_df.loc[agencies_df < 100].index  # only OHE agents that have sold more than 100
    small_agencies_mask = df['source_name'].isin(small_agencies_list)
    df.loc[small_agencies_mask, 'source_name'] = 'small_agent'    # replace name with "small_agent" if less than 100

    df.reset_index(inplace=True)    # after filtering out data, create new index TODO: Memorize this

    # Parallize vicinity features
    split_size = 200
    list_of_dfs = tqdm([df.loc[i:i + split_size - 1, :] for i in range(0, df.shape[0], split_size)])

    print(f'Calculating vicinity price_per_area statistics, {len(list_of_dfs)} chunks, {nbr_workers} in parallel')
    par_result = Parallel(n_jobs=nbr_workers)(
        delayed(calculate_vicinity_statistics)(df, tmp_df) for tmp_df in list_of_dfs
    )
    df = pd.concat(par_result, axis=0)

    # columns_to_one_hot = ['district', 'source_name', 'municipality']
    columns_to_one_hot = ['source_name', 'municipality']    # TODO: one model per object type? use district?
    print(f'One hot encoding {columns_to_one_hot}')
    dummies_df = pd.get_dummies(df, columns=columns_to_one_hot)  # add areas later

    df.drop(columns=columns_to_one_hot, inplace=True)
    df = pd.concat([df, dummies_df], axis=1)

    # TODO: Look into how to OHE areas in GBG. District is too aggregated. GeoJSON over primärområden?

    cols_to_drop = ['published', 'booli_id', 'apartment_number', 'sold_price_source', 'url', 'street_address',
                    'latitude', 'longitude', 'county', 'source_id', 'source_type', 'source_url',
                    'location_position_isApproximate', 'district_written', 'biddingOpen', 'mortgageDeed',
                    'seniorLiving', 'listPriceChangeDate', 'listPriceChange', 'sold']

    df.drop(columns=cols_to_drop, inplace=True)

    df.to_pickle(f'data/features/bbox/{location}/features.pkl')
    print(f'Done after {round((time.time() - features_start_time)/60, 1)}min with a total of {df.shape[1]} features on'
          f'{df.shape[0]} objects')

    return df


def calculate_vicinity_statistics(df, slice_df):
    import datetime
    import pandas as pd

    mean_price_dist_limit = 0.5  # km
    all_months_back = [3, 6, 12, 24]
    all_vicinity_stats = []
    df = df.copy(deep=True)
    for idx, obj in slice_df.iterrows():
        # TODO: use mean price for district here instead of vicinity?
        df = calculate_distances_to_point(lat=obj['latitude'], lon=obj['longitude'], df=df)
        vicinity_mask = df['distance'] <= mean_price_dist_limit

        obj_vicinity_stats = [idx]
        for months_back in all_months_back:
            date_limit = obj['sold_date'] - datetime.timedelta(days=months_back * 30)
            mask = (df['sold_date'] >= date_limit) & vicinity_mask

            obj_vicinity_stats.extend([
                df.loc[mask, 'sold_price_per_area'].min(),
                df.loc[mask, 'sold_price_per_area'].mean(),
                df.loc[mask, 'sold_price_per_area'].max(),
                df.loc[mask, 'sold_price_per_area'].count()
            ])

        all_vicinity_stats.append(obj_vicinity_stats)

    cols = [f'vicinity_{agg}_{months}_months' for months in all_months_back
            for agg in ['min_price_per_area', 'mean_price_per_area', 'max_price_per_area', 'count']]
    cols.insert(0, 'index')

    stats_df = pd.DataFrame(data=all_vicinity_stats, columns=cols)
    stats_df.set_index('index', inplace=True)
    slice_df = pd.concat([slice_df, stats_df], axis=1)

    return slice_df


def calculate_distances_to_point(lat, lon, location=None, df=None):
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
    from tqdm import tqdm
    nbr_places = len(IMPORTANT_PLACES[location])

    print(f'Calculating distances to {nbr_places} important places')
    for place_name in tqdm(IMPORTANT_PLACES[location], total=nbr_places):
        place = IMPORTANT_PLACES[location][place_name]
        df[f'distance_to_{place_name}'] = df.apply(lambda row: haversine(row, place['lat'], place['lon']), axis=1)

    return df
