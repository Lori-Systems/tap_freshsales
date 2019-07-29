import datetime
import json
import pytest
import tap_freshsales
import responses
import os

TEST_DOMAIN = 'testdomain'
TEST_DIR = os.path.dirname(__file__)


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture():
    """Monkey patch globals in freshsales tap module
    """
    tap_freshsales.CONFIG = {}
    tap_freshsales.CONFIG['start_date'] = str(datetime.datetime.now())
    tap_freshsales.CONFIG['domain'] = TEST_DOMAIN
    pytest.TEST_DIR = TEST_DIR
    pytest.TEST_DOMAIN = TEST_DOMAIN
    # Globally activated responses from sample test data
