def get_response(endpoint, offset, limit, area_id):
    import secrets
    from hashlib import sha1
    import time
    import requests
    from references.init import BASE_URL, CALLER_ID, PRIVATE_KEY, HEADERS

    url = f'{BASE_URL}/{endpoint}'
    unix_time = str(int(time.time()))
    unique_string = secrets.token_hex(8)
    hash_str = sha1((CALLER_ID + unix_time + PRIVATE_KEY + unique_string).encode('utf-8')).hexdigest()

    params = {'callerId': CALLER_ID, 'time': unix_time, 'unique': unique_string, 'hash': hash_str, 'limit': limit,
              'offset': offset, 'areaId': area_id}

    return requests.get(url, headers=HEADERS, params=params)

