"""Tap test suite uses responses to inject pseudo-freshsales API responses
and check output from stdout via singer.io API calls
"""

import json
import os
import responses
import pytest
from tap_freshsales import discover, sync_contacts_by_filter, sync_contacts_owner
from tap_freshsales import sync_accounts_by_filter, sync_appointments_by_filter, sync_accounts_owner
from tap_freshsales import sync_deals_by_filter, sync_leads_by_filter, sync_leads_owner, sync_deals_owner
from tap_freshsales import sync_sales_activities, sync_tasks_by_filter
from tap_freshsales import load_schemas, get_start


def test_get_start():
    """Test obtaining of start time from entity state
    """
    assert get_start(None)


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
    contact_url = 'https://{}.freshsales.io/api/contacts/view/5000440489'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, contact_url,
                  json=contact_data, status=200, content_type='application/json')
    assert sync_contacts_by_filter('updated_at', {'id': 5000440489}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_contacts_owner():
    """
    Test sync of contacts owner, inject data via responses
    """
    contact_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/contacts_owner.json')))
    contact_url = 'https://{}.freshsales.io/api/contacts/view/5005386510?include=owner&per_page=100&sort=updated_at&sort_type=desc&page=1'.format(
        pytest.TEST_DOMAIN)
    # import pdb; pdb.set_trace()
    responses.add(responses.GET, contact_url,
                  json=contact_data, status=200, content_type='application/json')
    assert sync_contacts_owner('id',{'id': 5005386510}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_deals_by_filter():
    """
    Test sync of deals, inject data via responses
    """
    deal_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/deals.json')))
    deal_url = 'https://{}.freshsales.io/api/deals/view/5000440502'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, deal_url,
                  json=deal_data, status=200, content_type='application/json')
    assert sync_deals_by_filter('updated_at', {'id': 5000440502}) is None
    assert len(responses.calls) == 1
 

@responses.activate
def test_sync_deals_owner():
    """
    Test sync of leads owner, inject data via responses
    """
    deal_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/deal_owner.json')))
    deal_url = 'https://{}.freshsales.io/api/deals/view/5001264008?include=owner&per_page=100&sort=updated_at&sort_type=desc&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, deal_url,
                  json=deal_data, status=200, content_type='application/json')

    assert sync_deals_owner('id',{'id': 5001264008}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_tasks_by_filter():
    """
    Test sync of tasks, inject data via responses
    """
    task_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/tasks.json')))
    task_url = 'https://{}.freshsales.io/api/tasks?filter=open&include=owner,users,targetable&per_page=100&sort=updated_at&sort_type=desc&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, task_url,
                  json=task_data, status=200, content_type='application/json')
    assert sync_tasks_by_filter('updated_at', 'open') is None
    # assert len(responses.calls) == 1

@responses.activate
def test_sync_leads_owner():
    """
    Test sync of leads owner, inject data via responses
    """
    lead_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/leads.json')))
    lead_url = 'https://{}.freshsales.io/api/leads/view/5016404979?include=owner&per_page=100&sort=updated_at&sort_type=desc&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, lead_url,
                  json=lead_data, status=200, content_type='application/json')
    assert sync_leads_owner('is_active',{'id': 5016404979}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_accounts_by_filter():
    """
    Test sync of accounts, inject data via responses
    """
    sales_account_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/sales_accounts.json')))
    sales_account_url = 'https://{}.freshsales.io/api/sales_accounts/view/5'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, sales_account_url,
                  json=sales_account_data, status=200, content_type='application/json')
    assert sync_accounts_by_filter('updated_at', {'id': 5}) is None
    assert len(responses.calls) == 1


@responses.activate
def test_sync_accounts_owner():
    """
    Test sync of accounts owner, inject data via responses
    """
    sales_account_owner_data = json.load(
        open(os.path.join(pytest.TEST_DIR, 'mock_data/sales_accounts_owner.json')))
    sales_account_url = 'https://{}.freshsales.io/api/sales_accounts/view/5001687794?include=owner&per_page=100&sort=updated_at&sort_type=desc&page=1'.format(
        pytest.TEST_DOMAIN)
    responses.add(responses.GET, sales_account_url,
                  json=sales_account_owner_data, status=200, content_type='application/json')
    assert sync_accounts_owner('id',{'id': 5001687794}) is None
    

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
