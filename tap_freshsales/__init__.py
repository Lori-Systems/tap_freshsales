#!/usr/bin/env python3
"""FreshSales Tap main entry point

Returns:
    [str] -- [FreshSales Tap]
"""

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
    "leads": "/api/leads/{query}",
    "contacts": "/api/contacts/{query}",
    "accounts": "/api/sales_accounts/{query}",
    "deals": "/api/deals/{query}",
    "tasks": "/api/tasks?filter={filter}&include={include}",
    "appointments": "/api/appointments?filter={filter}&include={include}",
    "sales_activities": "/api/sales_activities/"
}


@tap_utils.ratelimit(1, 2)
def request(url, params=None):
    """
    Rate limited API requests to fetch data from
    FreshSales API
    """
    params = params or {}
    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    if 'api_key' in CONFIG:
        headers['Authorization'] = 'Token token=' + CONFIG['api_key']

    req = requests.Request('GET', url, params=params,
                           headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)

    if 'Retry-After' in resp.headers:
        retry_after = int(resp.headers['Retry-After'])
        LOGGER.info(
            "Rate limit reached. Sleeping for {} seconds".format(retry_after))
        time.sleep(retry_after)
        return request(url, params)

    resp.raise_for_status()

    return resp


def get_url(endpoint, **kwargs):
    """
    Create approprate freshsales URL to create API call to relevant stream
    """
    return BASE_URL.format(CONFIG['domain']) + \
        endpoints[endpoint].format(**kwargs)

# Generate request for a given REST API URL


def gen_request(url, params={}):
    """
    Generator to yields rows of data for given stream
    """
    params["per_page"] = PER_PAGE
    page = 1
    # TODO: Meta tag carries number of pages
    # Use generator to scan across all pages of output
    while True:
        params['page'] = page
        data = request(url, params).json()
        data_list = []
        if isinstance(data, type({})):
            # TODO: Most API endpoint results the first key is data
            first_key = list(data.keys())[0]
            if first_key == 'filters':
                yield data
            elif first_key == 'meta':
                break
            else:
                data_list = data[first_key]
                for row in data_list:
                    yield row
            if len(data_list) == PER_PAGE:
                page += 1
            else:
                break


def load_schemas():
    """
    Load schemas from schemas folder and yield for all streams
    """
    schemas = {}

    for filename in os.listdir(tap_utils.get_abs_path('schemas')):
        if filename.endswith(".json"):
            path = tap_utils.get_abs_path('schemas') + '/' + filename
            file_raw = filename.replace('.json', '')
            with open(path) as file:
                schemas[file_raw] = json.load(file)

    return schemas


def discover():
    """
    Allow discovery of all streams and metadata
    """
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        mdata = metadata.new()
        mdata = metadata.write(mdata, (), 'table-key-properties', ['id'])
        mdata = metadata.write(mdata, ('properties', 'id'), 'inclusion', 'automatic')
        mdata = metadata.write(mdata, (), 'valid-replication-keys', ['updated_at'])
        mdata = metadata.write(mdata, ('properties', 'updated_at'), 'inclusion', 'automatic')
        for field_name in schema['properties'].keys():
            if field_name not in {'id', 'updated_at'}:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata),
            'key_properties': ['id']
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = metadata.to_map(stream['metadata'])
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams


def get_filters(endpoint):
    """
    Use Freshsales API structure to derive filters for an
    endpoint in the supported streams
    """
    url = get_url(endpoint, query='filters')
    return list(gen_request(url))[0]['filters']


def get_start(endpoint, filter_id=None):
    """
    Get bookmarked start time for specific entity
    (defined as combination of endpoint and filter)
    data before this start time is ignored
    """
    if endpoint in endpoints_mapper:
        params = endpoints_mapper[endpoint]
        entity = "{}{}".format(params['entity'], filter_id) if \
            filter_id else endpoint
        if entity not in STATE:
            STATE[entity] = CONFIG['start_date']
        return STATE[entity]
    return None

# TODO: This is very WET code , clean it up with streams mechanism
# Sync accounts


def sync_endpoint(endpoint):
    """
    Sync Sales Accounts Data, Standard schema is kept as columns,
    Custom fields are saved as JSON content
    """

    bookmark_properties = endpoints_mapper[endpoint]['bookmark_property']
    schema = tap_utils.load_schema(endpoint)
    singer.write_schema(endpoint,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])
    filters = endpoints_mapper[endpoint]['filters'] or get_filters(endpoint)
    if endpoint == 'sales_activities':
        start = get_start(endpoint)
        singer.write_schema(endpoint,
                            tap_utils.load_schema(endpoint),
                            ["id"],
                            bookmark_properties=[bookmark_property])
        sales = gen_request(get_url(endpoint))
        for sale in sales:
            if sale[bookmark_property] >= start:
                LOGGER.info("Sale {}: Syncing details".format(sale['id']))
                singer.write_record("sale_activities", sale,
                                    time_extracted=singer.utils.now())
    else:
        for fil in filters:
            sync_endpoint_by_filter(endpoint, fil)


def sync_endpoint_by_filter(endpoint, filter):
    """
    Sync accounts by view based filters, use bookmark property
    to manage state and fetch data updated since particular time
    """
    params = endpoints_mapper[endpoint]
    bookmark_property = params['bookmark_property']
    filter_id = filter if endpoint in ['tasks'] else filter['id']
    start = get_start(endpoint, str(filter_id))
    query = params['query'] + str(filter_id) if \
        params['query'] else ''

    if endpoint == 'tasks':
        items = gen_request(
            get_url(
                endpoint,
                filter=filter_id,
                include='owner,users,targetable'))
    elif endpoint == 'appointments':
        items = gen_request(
            get_url(
                endpoint,
                filter=filter_id,
                include='creater,targetable,appointment_attendees'))
    else:
        items = gen_request(get_url(endpoint, query=query))

    for item in items:
        if endpoint == 'accounts':
            if item[bookmark_property] >= start:
                item['custom_field'] = json.dumps(item['custom_field'])
                singer.write_record(
                    endpoint, item, time_extracted=singer.utils.now())
        elif endpoint == 'contacts':
            if item[bookmark_property] >= start:
                tap_utils.update_state(STATE, endpoint + "_" + str(fil_id),
                                       item[bookmark_property])
                singer.write_state(STATE)
        elif endpoint == 'leads':
            if item[bookmark_property] >= start:
                singer.write_record(
                    endpoint, item, time_extracted=singer.utils.now())
        elif endpoint == 'tasks':
            singer.write_record(
                endpoint, item, time_extracted=singer.utils.now())
        elif endpoint == 'appointments':
            singer.write_record(endpoint, item,
                                time_extracted=singer.utils.now())
        elif endpoint == 'deals':
            if item[bookmark_property] >= start:
                # get all sub-entities and save them
                item['amount'] = float(item['amount'])  # cast amount to float
                item['custom_field'] = json.dumps(
                    item['custom_field'])  # Make JSON String to store
                singer.write_record(
                    endpoint, item, time_extracted=singer.utils.now())

        LOGGER.info("{} {}: Syncing details".format(endpoint, item['id']))


endpoints_mapper = {
    'accounts': {
        'bookmark_property': 'updated_at',
        'filters': None,
        'query': 'view/',
        'entity': 'accounts_'},
    'contacts': {
        'bookmark_property': 'updated_at',
        'filters': None,
        'query': 'view/',
        'entity': 'contacts_'},
    'deals': {
        'bookmark_property': 'updated_at',
        'filters': None,
        'query': 'view/',
        'entity': 'deals_'},
    'leads': {
        'bookmark_property': 'updated_at',
        'filters': None,
        'query': 'view/',
        'entity': 'leads_'},
    'tasks': {
        'bookmark_property': 'updated_at',
        'filters': [
            'open',
            'due today',
            'due tomorrow',
            'overdue',
            'completed'],
        'query': '',
        'entity': 'tasks_'},
    'sales_activities': {
        'bookmark_property': 'updated_at',
        'filters': None,
        'query': '',
        'entity': None},
    'appointments': {
        'bookmark_property': 'updated_at',
        'filters': [
            'past',
            'upcoming'],
        'query': '',
        'entity': 'appointments'}}


def sync(config, state, catalog):
    """Sync some/all data streams

    Arguments:
        config {[dict]} -- [Variables to access source stream e.g. API endpoint and access credentials]
        state {[str]} -- [State of previous ETL loaded from file]
        catalog {[str]} -- [All streams catalog string (JSON formatted)]
    """

    LOGGER.info("Starting FreshSales sync")
    STATE.update(state)
    # Synchronize x7 data-streams
    # TODO: Use selected streams only
    # TODO: Use map based function compresenion to link fetch
    # function and stream name
    selected_streams = get_selected_streams(catalog)
    items = set(endpoints_mapper.keys())
    try:
        for x in items:
            if x in selected_streams:
                sync_endpoint(x)
    except HTTPError as e:
        LOGGER.critical(
            "Error making request to FreshSales API: GET %s: [%s - %s]",
            e.request.url, e.response.status_code, e.response.content)
        sys.exit(1)

    LOGGER.info("Completed sync")


@utils.handle_top_exception(LOGGER)
def main():
    """Main function call
    """

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog_string = json.dumps(catalog, indent=2)
        # LOGGER.info(catalog_string)
        print(catalog_string)
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog.to_dict()
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
