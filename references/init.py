import json, os

with open('references/credentials.json', 'r') as f:
    creds = json.load(f)

CALLER_ID = creds['public_key']
PRIVATE_KEY = creds['private_key']

HEADERS = {'Accept': 'application/vnd.booli-v2+json'}
BASE_URL = 'https://api.booli.se'

if os.path.exists('data') is False:
    os.makedirs('data')

for location in ['gothenburg']:
    for stage in ['raw', 'interim', 'processed', 'features']:
        for path in [f'data/{stage}', f'data/{stage}/{location}']:
            if os.path.exists(path) is False:
                os.makedirs(path)

