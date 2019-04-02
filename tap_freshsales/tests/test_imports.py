import inspect

"""
Test function for all tap_freshsales wildcard imports
"""


def test_wildcard_import():
    tap_freshsales = __import__("tap_freshsales")

    for name in dir(tap_freshsales):
        # ignore attributes starting by underscores
        if name.startswith("_"):
            continue
        attr = getattr(tap_freshsales, name)
        if inspect.ismodule(attr):
            continue
