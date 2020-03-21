

def get_response(endpoint, offset, limit, area_id):
    import secrets
    from hashlib import sha1
    import time
    import requests
    from init import BASE_URL, CALLER_ID, PRIVATE_KEY, HEADERS

    url = f'{BASE_URL}/{endpoint}'
    unix_time = str(int(time.time()))
    unique_string = secrets.token_hex(8)
    hash_str = sha1((CALLER_ID + unix_time + PRIVATE_KEY + unique_string).encode('utf-8')).hexdigest()

    params = {'callerId': CALLER_ID, 'time': unix_time, 'unique': unique_string, 'hash': hash_str, 'limit': limit,
              'offset': offset, 'areaId': area_id}

    return requests.get(url, headers=HEADERS, params=params)


def get_data(endpoint, limit=100, start_from_area_id=0):
    import time
    import os
    import pandas as pd

    base_save_path = f'data/{endpoint}'
    if os.path.exists(base_save_path) is False:
        os.makedirs(base_save_path)


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
    while area_id < 1790:
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

            time.sleep(0.1)     # sleep between every call to not create pressure on API

        if len(all_data) == 0:
            print(f'No items found.\n')
        else:
            save_path = f'{base_save_path}/{area_id}.pkl'
            df = pd.json_normalize(all_data, sep='_')       # flatten dict
            df.to_pickle(save_path)

            print(f'Fetched {total_count} items. Saved to {save_path}\n')

        area_id += 1
        time.sleep(5)


# a = get_response('sold', offset=0, limit=5, area_id=1)
# print(a.status_code)
# print(a.json())

get_data('sold', limit=500, start_from_area_id=3)



