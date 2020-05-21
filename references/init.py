import json, os

with open('references/credentials.json', 'r') as f:
    creds = json.load(f)

CALLER_ID = creds['public_key']
PRIVATE_KEY = creds['private_key']

HEADERS = {'Accept': 'application/vnd.booli-v2+json'}
BASE_URL = 'https://api.booli.se'

if os.path.exists('data') is False:
    os.makedirs('data')

for data_stage in ['raw', 'interim', 'processed', 'features']:
    if os.path.exists(f'data/{data_stage}') is False:
        os.makedirs(f'data/{data_stage}')

