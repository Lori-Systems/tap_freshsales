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
                "inclusion": "available",
                "table-key-properties": ["id"],
                "selected": True,
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
    url = get_url(endpoint, query='filters')
    filters = list(gen_request(url))[0]['filters']
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

# TODO: This is very WET code , clean it up with streams mechanism
# Sync accounts


def sync_accounts():
    """
    Sync Sales Accounts Data, Standard schema is kept as columns,
    Custom fields are saved as JSON content
    """
    bookmark_property = 'updated_at'
    endpoint = 'accounts'
    schema = tap_utils.load_schema(endpoint)
    singer.write_schema(endpoint,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])
    filters = get_filters(endpoint)
    for fil in filters:
        sync_accounts_by_filter(bookmark_property, fil)

# Batch sync accounts while bookmarking updated at


def sync_accounts_by_filter(bookmark_prop, fil):
    """
    Sync accounts by view based filters, use bookmark property
    to manage state and fetch data updated since particular time
    """
    endpoint = 'accounts'
    fil_id = fil['id']
    state_entity = endpoint + "_" + str(fil_id)
    start = get_start(state_entity)
    accounts = gen_request(get_url(endpoint, query='view/'+str(fil_id)))
    for acc in accounts:
        if acc[bookmark_prop] >= start:
            LOGGER.info("Account {}: Syncing details".format(acc['id']))
            acc['custom_field'] = json.dumps(acc['custom_field'])
            singer.write_record(
                "accounts", acc, time_extracted=singer.utils.now())


def sync_contacts():
    """
    Sync Sales Accounts Data, Standard schema is kept as columns,
    Custom fields are saved as JSON content
    """
    bookmark_property = 'updated_at'
    endpoint = 'contacts'
    schema = tap_utils.load_schema(endpoint)
    singer.write_schema(endpoint,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])
    filters = get_filters(endpoint)
    for fil in filters:
        sync_contacts_by_filter(bookmark_property, fil)

# Batch sync contacts while bookmarking updated at


def sync_contacts_by_filter(bookmark_prop, fil):
    """
    Sync all contacts updated after bookmark time
    """
    endpoint = 'contacts'
    fil_id = fil['id']
    state_entity = endpoint + "_" + str(fil_id)
    start = get_start(state_entity)
    contacts = gen_request(get_url(endpoint, query='view/'+str(fil_id)))
    for con in contacts:
        if con[bookmark_prop] >= start:
            LOGGER.info("Contact {}: Syncing details".format(con['id']))
            tap_utils.update_state(STATE, state_entity, con[bookmark_prop])
            singer.write_record(
                endpoint, con, time_extracted=singer.utils.now())
            singer.write_state(STATE)

# Batch sync deals and stages of deals


def sync_deals():
    """
    Sync deals for every view
    """
    bookmark_property = 'updated_at'
    endpoint = 'deals'
    singer.write_schema(endpoint,
                        tap_utils.load_schema(endpoint),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    filters = get_filters(endpoint)
    for fil in filters:
        sync_deals_by_filter(bookmark_property, fil)

# Batch sync deals with bookmarking on update time


def sync_deals_by_filter(bookmark_prop, fil):
    """
    Iterate over all deal filter to sync all deal data
    """
    endpoint = 'deals'
    fil_id = fil['id']
    state_entity = endpoint + "_" + str(fil_id)
    start = get_start(state_entity)
    deals = gen_request(get_url(endpoint, query='view/'+str(fil_id)))
    for deal in deals:
        if deal[bookmark_prop] >= start:
            # get all sub-entities and save them
            deal['amount'] = float(deal['amount'])  # cast amount to float
            deal['custom_field'] = json.dumps(
                deal['custom_field'])  # Make JSON String to store
            LOGGER.info("Deal {}: Syncing details".format(deal['id']))
            singer.write_record(
                "deals", deal, time_extracted=singer.utils.now())

# Sync leads across all filters


def sync_leads():
    """
    Sync leads data and call out to per-filter sync
    """
    bookmark_property = 'updated_at'
    endpoint = 'leads'
    singer.write_schema(endpoint,
                        tap_utils.load_schema(endpoint),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    filters = get_filters(endpoint)
    for fil in filters:
        sync_leads_by_filter(bookmark_property, fil)

# Fetch leads for a particular filter for sync


def sync_leads_by_filter(bookmark_prop, fil):
    """
    Iterate over all leads in a filter and consume generator
    to yield schema rows
    """
    endpoint = 'leads'
    fil_id = fil['id']
    state_entity = endpoint + "_" + str(fil_id)
    start = get_start(state_entity)
    leads = gen_request(get_url(endpoint, query='view/'+str(fil_id)))
    for lead in leads:
        if lead[bookmark_prop] >= start:
            LOGGER.info("Lead {}: Syncing details".format(lead['id']))
            singer.write_record(
                "leads", lead, time_extracted=singer.utils.now())

# Fetch tasks stream


def sync_tasks():
    """
    Sync all task based on filters
    """
    endpoint = 'tasks'
    bookmark_property = 'updated_at'
    singer.write_schema(endpoint,
                        tap_utils.load_schema(endpoint),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    # Hardcoded task filters
    filters = ['open', 'due today', 'due tomorrow', 'overdue', 'completed']
    for fil in filters:
        sync_tasks_by_filter(bookmark_property, fil)

# Fetch tasks by all applicable filters


def sync_tasks_by_filter(bookmark_prop, fil):
    """
    Sync tasks for a specific filter
    """
    endpoint = 'tasks'
    state_entity = endpoint + "_" + str(fil)
    # TODO: Verify updated-at exists for tasks
    #start = get_start(state_entity)
    tasks = gen_request(get_url(endpoint, filter=fil,
                                include='owner,users,targetable'))
    for task in tasks:
        LOGGER.info("Task {}: Syncing details".format(task['id']))
        singer.write_record(endpoint, task, time_extracted=singer.utils.now())


# Fetch sales_activities stream
def sync_sales_activities():
    """Sync all sales activities, call out to individual filters
    """

    bookmark_property = 'updated_at'
    endpoint = 'sales_activities'
    state_entity = endpoint
    start = get_start(state_entity)
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

# Fetch all team appointments


def sync_appointments():
    """Sync all appointments
    """

    endpoint = 'appointments'
    bookmark_property = 'updated_at'
    filters = ['past', 'upcoming']
    singer.write_schema(endpoint,
                        tap_utils.load_schema(endpoint),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    for fil in filters:
        sync_appointments_by_filter(bookmark_property, fil)

# Fetch team appointments by filter


def sync_appointments_by_filter(bookmark_property, fil):
    """Iterate over all appointment filter to sync

    Arguments:
        bookmark_property {[str]} -- [Field used to bookmark stream]
        fil {[str]} -- [Filter string which yields a subset of the stream]
    """

    endpoint = 'appointments'
    # TODO: Verify updated_at exists for appointments
    #start = get_start(endpoint)
    appts = gen_request(get_url(endpoint, filter=fil,
                                include='creater,targetable,appointment_attendees'))
    for appoint in appts:
        LOGGER.info("Appointment {}: Syncing details".format(appoint['id']))
        singer.write_record(endpoint, appoint,
                            time_extracted=singer.utils.now())


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
    try:
        if 'contacts' in selected_streams:
            sync_contacts()
        if 'appointments' in selected_streams:
            sync_appointments()
        if 'deals' in selected_streams:
            sync_deals()
        if 'sales_activities' in selected_streams:
            sync_sales_activities()
        if 'leads' in selected_streams:
            sync_leads()
        if 'accounts' in selected_streams:
            sync_accounts()
        if 'tasks' in selected_streams:
            sync_tasks()
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
        LOGGER.info(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
