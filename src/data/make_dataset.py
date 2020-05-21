import pandas as pd


def load_dataset(path) -> pd.DataFrame():
    return pd.read_pickle(f'data/{path}.pkl')


def combine_bbox_to_df(loading_path, save_path=None):
    import pandas as pd

    if save_path is None:
        save_path = f'{loading_path}/all'

    all_df = []
    for endpoint, status in zip(['sold', 'listings'], [1, 0]):
        path = f'{loading_path}/{endpoint}.pkl'
        tmp_df = pd.read_pickle(path)
        tmp_df['sold'] = status
        all_df.append(tmp_df)

    df = pd.concat(all_df)
    df.to_pickle(f'{save_path}.pkl')

    return df


def combine_to_df(endpoint, save_filename=None):
    import pandas as pd
    import os
    import datetime
    import time

    print('Combining pickles to one dataframe')
    loading_path = f'data/raw/{endpoint}'
    date_today = datetime.date.today()

    if save_filename is None:
        save_filename = f'{endpoint}_{date_today}'

    all_df = []
    start_time = time.time()
    for file_name in os.listdir(loading_path):
        path = f'{loading_path}/{file_name}'
        all_df.append(pd.read_pickle(path))

    df = pd.concat(all_df)

    df.to_pickle(f'data/interim/{save_filename}.pkl')
    print(f'Job took {round((time.time() - start_time)/60, 2)} min')

    return df


def fetch_data(endpoint, limit=100, continue_from_last_area=False, start_from_area_id=1, stop_at_area_id=15000):
    import time
    import os
    import pandas as pd
    import json
    from src.data.api import get_response

    status_file_path = f'data/raw/status_{endpoint}.json'   # not in the endpoint folder
    base_save_path = f'data/raw/{endpoint}'

    if os.path.exists(base_save_path) is False:
        os.makedirs(base_save_path)

    if os.path.exists(status_file_path) is False:
        init_status = {'last_checked_area': 1}
        with open(status_file_path, 'w') as f:
            json.dump(init_status, f)
    else:
        with open(status_file_path, 'r') as f:
            status = json.load(f)

            if continue_from_last_area is True:
                start_from_area_id = status['last_checked_area']

    # TODO: Test to fetch data in a box instead of looping over areas
    # params = base_params.copy()
    # params['bbox'] = "54.66870,11.3769,69.44847,24.9539"
    # params['bbox'] = "59.34674,18.0603,59.64674,18.3603"
    # params['bbox'] = "58.34674,17.0603,60.64674,19.3603"
    # resp = requests.get(url, headers=headers, params=params)
    # print(resp.status_code)
    # if resp.status_code == 200:
    #     print(resp.json())
    #     result = resp.json()
    # exit()

    # area_id = 1789
    area_id = start_from_area_id
    while area_id <= stop_at_area_id:
        print(f'Fetching data from: endpoint={endpoint}, area={area_id}')

        total_count, offset = 1, 0      # init
        all_data = []
        while total_count > offset:
            resp = get_response(endpoint, offset=offset, limit=limit, area_id=area_id)

            if resp.status_code == 200:
                result = resp.json()
                total_count = result['totalCount']
                offset += result['count']
                all_data.extend(result[endpoint])

            else:
                print(f'Error {resp.status_code}')
                break

            time.sleep(0.2)     # sleep between every call to not create pressure on API

        if len(all_data) == 0:
            print(f'No items found.\n')
        else:
            save_path = f'{base_save_path}/{area_id}.pkl'
            df = pd.json_normalize(all_data, sep='_')       # flatten dict
            df.to_pickle(save_path)

            print(f'Fetched {total_count} items. Saved to {save_path}\n')

        # update status file and continue to next area
        status = {'last_checked_area': area_id}
        with open(status_file_path, 'w') as f:
            json.dump(status, f)

        area_id += 1
        time.sleep(1)   # rest to be gentle on the api


def fetch_bbox_data(endpoint, limit=100, location='gothenburg'):
    import time
    import os
    import pandas as pd
    from src.data.api import get_bbox_response
    from tqdm import tqdm

    for stage in ['raw', 'interim', 'processed', 'features']:
        for path in [f'data/{stage}/bbox', f'data/{stage}/bbox/{location}']:
            if os.path.exists(path) is False:
                os.makedirs(path)

    if location == 'gothenburg':
        lat_low, long_low = 57.506682, 11.595467
        lat_high, long_high = 57.885870, 12.326569
    else:
        print(f'"{location}" has not been implemented yet!')
        exit(1)

    total_count, offset = 1, 0      # init
    all_data = []
    with tqdm(total=100) as progress_bar:
        while total_count > offset:
            resp = get_bbox_response(endpoint, offset, limit, lat_low, long_low, lat_high, long_high)

            if resp.status_code == 200:
                result = resp.json()
                total_count = result['totalCount']

                if offset == 0:
                    update_step = 100/(total_count/limit)   # nbr_runs = total_count/limit

                offset += result['count']
                all_data.extend(result[endpoint])

            else:
                print(f'Error {resp.status_code}')
                break

            time.sleep(0.2)     # sleep between every call to not create pressure on API
            progress_bar.update(update_step)

    print(f'Found {total_count} items')
    progress_bar.close()

    if len(all_data) == 0:
        print(f'No items found.\n')
    else:
        save_path = f'data/raw/bbox/{location}/{endpoint}.pkl'
        df = pd.json_normalize(all_data, sep='_')       # flatten dict
        df.to_pickle(save_path)

        print(f'Fetched {total_count} items. Saved to {save_path}\n')
