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
        headers['Authorization'] = 'Token token='+CONFIG['api_key']

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
    return BASE_URL.format(CONFIG['domain']) + endpoints[endpoint].format(**kwargs)

# Generate request for a given REST API URL


def gen_request(url, params=None):
    """
    Generator to yields rows of data for given stream
    """
    params = params or {}
    params["per_page"] = PER_PAGE
    page = 1
    # TODO: Meta tag carries number of pages
    # Use generator to scan across all pages of output
    while True:
        params['page'] = page
        data = request(url, params).json()
        data_list = []
        if type(data) == type({}):
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

        # Default metadata templated on
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md
        default_meta = {
            "metadata": {
                "inclusion": "automatic",
                "table-key-properties": ["id"],
                "valid-replication-keys": ["updated_at"],
                "schema-name": schema_name,
            },
            "breadcrumb": []
        }
        # Each stream uses id as the primary key
        id_meta = {
            "metadata": {
                "inclusion": "automatic",
            },
            "breadcrumb": ["properties", "id"]
        }
        # Each stream has updated_at times
        bookmark_meta = {
            "metadata": {
                "inclusion": "automatic",
            },
            "breadcrumb": ["properties", "updated_at"]
        }

        stream_metadata = [default_meta, id_meta, bookmark_meta]
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': stream_metadata,
            'key_properties': stream_key_properties
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
    # TODO: Resolve why cookie-cutter uses arribute dict notation
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
    # Moved filter edge-cases for `tasks` and `appointments` to this function
    # TODO: Find out why there is a special use-case for `tasks` filter and remove hard-coded filter if possible
    edge_case_filters = {
        'tasks': ['open', 'due today', 'due tomorrow', 'overdue', 'completed'],
        'appointments': ['past', 'upcoming']
    }

    if endpoint not in edge_case_filters.keys():
        url = get_url(endpoint, query='filters')
        filters = list(gen_request(url))[0]['filters']

    else:
        filters = edge_case_filters[endpoint]
    return filters


def get_start(entity):
    """
    Get bookmarked start time for specific entity
    (defined as combination of endpoint and filter)
    data before this start time is ignored
    """
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']
    return STATE[entity]


def sync_current_endpoint_data_stream(endpoint):
    """
    Sync current data resource. Standard schema is kept as columns,
    Custom fields are saved as JSON content
    :return:
    """
    bookmark_property = 'updated_at'
    schema = tap_utils.load_schema(endpoint)
    singer.write_schema(endpoint, schema, ["id"], bookmark_properties=[bookmark_property])
    filters = get_filters(endpoint)

    for fil in filters:
        sync_current_endpoint_by_filter(bookmark_property, fil)


def get_endpoint_resource(endpoint, fil):
    """

    :param endpoint: current endpoint
    :param fil: current filter
    :return:
    """
    fil_id = fil['id']

    if endpoint in ['accounts', 'contacts', 'deals', 'leads']:
        endpoint_resources = gen_request(get_url(endpoint, query='view/' + str(fil_id)))
    elif endpoint == 'appointments':
        endpoint_resources = gen_request(get_url(endpoint, filter=fil, include='creater,targetable,appointment_attendees'))
    elif endpoint == 'tasks':
        endpoint_resources = gen_request(get_url(endpoint, filter=fil, include='owner,users,targetable'))
    else:
        endpoint_resources = gen_request(get_url(endpoint))

    return endpoint_resources


def sync_current_endpoint_by_filter(endpoint, bookmark_property, fil):
    """
    Sync current endpoint while bookmarking updated at by view based filters, use bookmark property
    to manage state and fetch data updated since particular time

    # Batch sync accounts
    """
    fil_id = fil['id']
    state_entity = endpoint + "_" + str(fil_id)
    # TODO: Verify updated_at exists for `appointments` endpoint
    start = get_start(state_entity)

    endpoint_resources = get_endpoint_resource(endpoint, fil)
    for endpoint_resource in endpoint_resources:
        LOGGER.info("{} {}: Syncing details".format(endpoint, endpoint_resource['id']))

        if endpoint_resource[bookmark_property] >= start:
            # Specific edge-case handling for deals endpoint
            if endpoint == 'deals' and endpoint_resource[bookmark_property]:
                endpoint_resource['amount'] = float(endpoint_resource['amount'])

            tap_utils.update_state(STATE, state_entity, endpoint_resource[bookmark_property])
            endpoint_resource['custom_field'] = json.dumps(endpoint_resource['custom_field'])
            singer.write_record(endpoint, endpoint_resource, time_extracted=singer.utils.now())
            singer.write_state(STATE)

        singer.write_record(endpoint, endpoint_resource, time_extracted=singer.utils.now())


def sync(config, state, catalog):
    """Sync some/all data streams

    Arguments:
        config {[dict]} -- [Variables to access source stream e.g. API endpoint and access credentials]
        state {[str]} -- [State of previous ETL loaded from file]
        catalog {[str]} -- [All streams catalog string (JSON formatted)]
    """

    LOGGER.info("Starting FreshSales sync")
    STATE.update(state)
    selected_streams = get_selected_streams(catalog)
    try:
        [sync_current_endpoint_data_stream(stream_endpoint) for stream_endpoint in selected_streams]

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
        #LOGGER.info(catalog_string)
        print(catalog_string)
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
