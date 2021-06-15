# Discovery code is here.
import os
import json
import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry
from tap_freshsales import tap_utils
from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def load_schemas():
    """
        Load schemas from schemas folder and yield for all streams
    """
    schemas = {}
    for filename in os.listdir(tap_utils.get_abs_path('schemas')):
        if filename.endswith(".json"):
            path = tap_utils.get_abs_path('schemas') + '/' + filename
            file_raw = filename.replace('.json', '')
            with open(path) as file:
                schemas[file_raw] = json.load(file)
    return schemas


def update_custom_schema(stream, schema, catalog_entries):
    custom_modules = stream.get_custom_modules()
    for module_name, module_schema in custom_modules.items():
        # add custom module fields to schema properties
        schema['properties'].update(module_schema)

        catalog_entry = {
            "stream": module_name,
            "tap_stream_id": module_name,
            "schema": schema,
            "metadata": metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            ),
            "key_properties": stream.key_properties,
            "replication_key": stream.replication_keys
        }
        catalog_entries.append(catalog_entry)
    return catalog_entries


def do_discover(client):
    """
        Allow discovery of all streams and metadata
    """
    raw_schemas = load_schemas()
    catalog_entries = []
    for stream_name, schema in raw_schemas.items():
        # create and add catalog entry
        stream = STREAM_OBJECTS[stream_name]

        # add support for custom modules
        if stream_name == 'custom_module':
            # pass necessary params of stream init
            stream_object = STREAM_OBJECTS[stream_name](client, client.config, client.state)
            try:
                catalog_entries = update_custom_schema(stream_object, schema, catalog_entries)
            except Exception as e:
                LOGGER.error("Custom module exception raised with message: {}".format(e))

            # catalog entry/ies added above, continue to next iteration
            continue

        # TODO: add custom fields to discovered schema for existing entities?
        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream.key_properties,
                valid_replication_keys=stream.replication_keys,
                replication_method=stream.replication_method,
            ),
            "key_properties": stream.key_properties,
            "replication_key": stream.replication_keys
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})
