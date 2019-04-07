"""Tap test suite uses responses to inject pseudo-freshsales API responses
and check output from stdout via singer.io API calls
"""

import json
import os
import responses
import pytest
from tap_freshsales import discover, sync_current_endpoint_by_filter
from tap_freshsales import load_schemas, get_start


def test_get_start():
    """Test obtaining of start time from entity state
    """
    assert get_start(None)


def test_load_schemas():
    """Test schema loading by the tap
    """
    assert load_schemas()


TEST_CASES = (
    ('contacts', 'https://{}.freshsales.io/api/contacts/view/1?per_page=100&page=1'),
    ('deals', 'https://{}.freshsales.io/api/deals/view/1?per_page=100&page=1'),
    ('tasks', 'https://{}.freshsales.io/api/tasks?filter=open&include=owner,users,targetable&per_page=100&page=1'),
    ('sales_accounts', 'https://{}.freshsales.io/api/sales_accounts/view/1?per_page=100&page=1')
)


@pytest.mark.parametrize("endpoint,endpoint_url", TEST_CASES)
@responses.activate
def test_sync_current_endpoint_by_filter(endpoint, endpoint_url):
    """
    Test sync of various endpoints including contacts, deals, sales_accounts and tasks, inject data via responses
    """
    endpoint_data = json.load(open(os.path.join(pytest.TEST_DIR, 'mock_data/{}.json'.format(endpoint))))
    responses.add(responses.GET, endpoint_url,
                  json=endpoint_data, status=200, content_type='application/json')
    assert sync_current_endpoint_by_filter('updated_at', {'id': 1}) is None
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
