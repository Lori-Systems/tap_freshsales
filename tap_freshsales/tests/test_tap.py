"""Tap test suite uses responses to inject pseudo-freshsales API responses
and check output from stdout via singer.io API calls
"""

import json
import os
import responses
import pytest
from tap_freshsales import (
    get_start,
    load_schemas,
    discover,
    sync_endpoint,
    sync_endpoint_by_filter
)


def test_get_start():
    """Test obtaining of start time from entity state
    """
    assert get_start('test', 1) == None


def test_load_schemas():
    """Test schema loading by the tap
    """
    assert load_schemas()


@responses.activate
def test_sync_contacts_by_filter():
    """
    Test sync of contacts, inject data via responses
    """
    contact_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/contacts.json')))
    contact_url = 'https://{}.freshsales.io/api/contacts/view/1?per_page=100&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, contact_url,
                  json=contact_data, status=200, content_type='application/json')
    assert sync_endpoint_by_filter('contacts', {'id': 1}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_deals_by_filter():
    """
    Test sync of deals, inject data via responses
    """
    deal_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/deals.json')))
    deal_url = 'https://{}.freshsales.io/api/deals/view/1?per_page=100&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, deal_url,
                  json=deal_data, status=200, content_type='application/json')
    assert sync_endpoint_by_filter('deals', {'id': 1}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_tasks_by_filter():
    """
    Test sync of tasks, inject data via responses
    """
    task_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/tasks.json')))
    task_url = 'https://{}.freshsales.io/api/tasks?filter=open&include=owner,users,targetable&per_page=100&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, task_url,
                  json=task_data, status=200, content_type='application/json')
    assert sync_endpoint_by_filter('tasks', 'open') is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_accounts_by_filter():
    """
    Test sync of accounts, inject data via responses
    """
    sales_account_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/sales_accounts.json')))
    sales_account_url = 'https://{}.freshsales.io/api/sales_accounts/view/1?per_page=100&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, sales_account_url,
                  json=sales_account_data, status=200, content_type='application/json')
    assert sync_endpoint_by_filter('accounts', {'id': 1}) is None
    assert len(responses.calls) == 1


def test_tap_discover():
    """
    Test stream/metadata discovery
    """
    stream_def = discover()
    assert stream_def
    # Ensure some metadata exists
    for stream in stream_def['streams']:
        assert stream['metadata']
        print(stream['metadata'])
        assert not ('selected' in stream['metadata'][0])
