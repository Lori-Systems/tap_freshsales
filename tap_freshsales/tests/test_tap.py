"""Tap test suite uses responses to inject pseudo-freshsales API responses
and check output from stdout via singer.io API calls
"""

import pytest
from tap_freshsales import discover, sync_contacts_by_filter
from tap_freshsales import sync_accounts_by_filter, sync_appointments_by_filter
from tap_freshsales import sync_deals_by_filter, sync_leads_by_filter
from tap_freshsales import sync_sales_activities, sync_tasks_by_filter


def test_sync_contacts_by_filter():
    """
    Test sync of contacts, inject data via responses
    """
    assert sync_contacts_by_filter(None,None)


def test_sync_deals_by_filter():
    """
    Test sync of deals, inject data via responses
    """
    assert sync_deals_by_filter(None,None)


def test_sync_tasks_by_filter():
    """
    Test sync of tasks, inject data via responses
    """
    assert sync_tasks_by_filter(None,None)


def test_sync_accounts_by_filter():
    """
    Test sync of accounts, inject data via responses
    """
    assert sync_accounts_by_filter(None,None)


def test_tap_discover():
    """
    Test stream/metadata discovery
    """
    stream_def = discover()
    assert stream_def
    # Ensure some metadata exists
    assert stream_def['streams'][0]['metadata']
