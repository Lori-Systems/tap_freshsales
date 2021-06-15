#!/usr/bin/env python3
import singer
from singer import utils
from singer.catalog import Catalog, write_catalog
from tap_freshsales.discover import do_discover
from tap_freshsales.client import Client
from tap_freshsales.sync import do_sync

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = ["api_key", "domain", "start_date"]


@utils.handle_top_exception(LOGGER)
def main():
    args = singer.parse_args(REQUIRED_CONFIG_KEYS)
    config, catalog, state = args.config, args.catalog or Catalog([]), args.state
    client = Client(config)
    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        LOGGER.info("Starting discovery mode")
        catalog = do_discover(client)
        write_catalog(catalog)
    else:
        do_sync(client, config, state, catalog)


if __name__ == "__main__":
    main()
