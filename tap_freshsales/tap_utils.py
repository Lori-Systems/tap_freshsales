import collections
import datetime
import functools
import json
import os
import time
# import pendulum
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
import singer


def strptime(dt):
    """
    Parse FreshSales time format
    """
    try:
        if len(dt.split('-')[-1]) == 5 or len(dt.split('+')[-1]) == 5:
            # change from '2021-06-11T08:52:40 -/+TimeZone' -> '2021-06-11T08:52:40Z' (UTC)
            return singer.utils.strptime_to_utc(dt)
    except Exception:
        pass
    return datetime.datetime.strptime(dt, DATETIME_FMT)


def strftime(dt):
    """
    Output FreshSales time format
    """
    return dt.strftime(DATETIME_FMT)


def get_abs_path(path):
    """
    Create path to json schemas
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def ratelimit(limit, every):
    """
    Function to limit API calls velocity
    """
    def limitdecorator(fn):
        """
        Rate limit decorator
        """
        times = collections.deque()

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            """
            Decorator implementation to wrap
            source function and add delays
            """
            if len(times) >= limit:
                t0 = times.pop()
                t = time.time()
                sleep_time = every - (t - t0)
                if sleep_time > 0:
                    time.sleep(sleep_time)

            times.appendleft(time.time())
            return fn(*args, **kwargs)

        return wrapper

    return limitdecorator


def chunk(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_schema(entity):
    return load_json(get_abs_path("schemas/{}.json".format(entity)))


def update_state(state, entity, dt):
    if dt is None:
        return

    if isinstance(dt, datetime.datetime):
        dt = strftime(dt)

    if entity not in state:
        state[entity] = dt

    if dt >= state[entity]:
        state[entity] = dt


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception(
            "Config is missing required keys: {}".format(missing_keys))
