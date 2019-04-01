
import pytest
import tap_freshsales
import os
"""
Remove unused imports to reduce application startup time. It also reduces the memory load as libraries use up RAM too
"""

TEST_DOMAIN = 'testdomain'
TEST_DIR = os.path.dirname(__file__)


@pytest.fixture(scope="session", autouse=True)
def default_session_fixture():
    """Monkey patch globals in freshsales tap module
    """

    import datetime
    """
    Refactoring dictionary creation and replacing it with dictionary literals results in a great performance improvement
    during app startup
    """
    tap_freshsales.CONFIG = {'start_date': str(datetime.datetime.now()), 'domain': TEST_DOMAIN}
    pytest.TEST_DIR = TEST_DIR
    pytest.TEST_DOMAIN = TEST_DOMAIN
    # Globally activated responses from sample test data

    

    
