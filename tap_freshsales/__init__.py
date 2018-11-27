#!/usr/bin/env python3
import os
import json
import sys
import time
import backoff
import requests
from requests.exceptions import HTTPError
import singer
from singer import utils, metadata

from tap_freshsales import tap_utils


REQUIRED_CONFIG_KEYS = ["api_key", "domain", "start_date"]
PER_PAGE = 100
BASE_URL = "https://{}.freshsales.io"
CONFIG = {}
STATE = {}
LOGGER = singer.get_logger()
SESSION = requests.Session()

endpoints = {
    "leads": "/api/leads/view/",
    "contacts": "/api/contacts/view/",
    "accounts": "/api/sales_accounts/{query}",
    "deals": "/api/deals/view/",
    "tasks": "/api/tasks?filter=",
    "appointments": "/api/appointments?filter=",
    "sales": "/api/sales_activities/"
}

@tap_utils.ratelimit(1, 2)
def request(url, params=None):
    params = params or {}
    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']
    
    if 'api_key' in CONFIG:
        headers['Authorization'] = 'Token token='+CONFIG['api_key']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if 'Retry-After' in resp.headers:
        retry_after = int(resp.headers['Retry-After'])
        LOGGER.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
        time.sleep(retry_after)
        return request(url, params)

    resp.raise_for_status()

    return resp

def get_url(endpoint, **kwargs):
    return BASE_URL.format(CONFIG['domain']) + endpoints[endpoint].format(**kwargs)

# Generate request for a given REST API URL
def gen_request(url, params=None):
    params = params or {}
    params["per_page"] = PER_PAGE
    page = 1
    while True:
        params['page'] = page
        data = request(url, params).json()
        if(len(data)==1):
            yield data
        for row in data:
            yield row

        if len(data) == PER_PAGE:
            page += 1
        else:
            break

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(tap_utils.get_abs_path('schemas')):
        path = tap_utils.get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams

# Sync accounts
def sync_accounts():
    '''
    Sync Sales Accounts Data, Standard schema is kept as columns,
    Custom fields are saved as JSON content
    '''
    bookmark_property = 'updated_at'
    singer.write_schema("accounts",
                        tap_utils.load_schema("accounts"),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    sync_accounts_by_filter(bookmark_property)

# Batch sync accounts while bookmarking updated at
def sync_accounts_by_filter(bookmark_prop):
    endpoint = 'accounts'
    url = get_url(endpoint,query='filters')
    print(url)
    filters = list(gen_request(url))[0]['filters']
    for fil in filters:
        fil_id = fil['id']
        accounts = list(gen_request(get_url(endpoint,query='view/'+str(fil_id))))
        for acc in accounts:
            print(acc)

# Batch sync deals and stages of deals
def sync_deals():
    '''
    Sync deals for every view
    '''
    return

def sync(config, state, catalog):
    LOGGER.info("Starting FreshSales sync")

    try:
        sync_accounts()
        sync_deals()
    except HTTPError as e:
        LOGGER.critical(
            "Error making request to FreshSales API: GET %s: [%s - %s]",
            e.request.url, e.response.status_code, e.response.content)
        sys.exit(1)

    LOGGER.info("Completed sync")

@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover()

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
