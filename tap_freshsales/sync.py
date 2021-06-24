import requests
import singer
from singer import Transformer, metadata

import tap_freshsales.streams
from .streams import STREAM_OBJECTS

logger = singer.get_logger()
session = requests.Session()


def do_sync(client, config: dict, state: dict, catalog: singer.Catalog):
    """Sync data streams

        Arguments:
            config {[dict]} -- [Variables to access source stream e.g. API endpoint and access credentials]
            state {[str]} -- [State of previous ETL loaded from file]
            catalog {[str]} -- [All streams catalog string (JSON formatted)]
     """
    logger.info("Starting FreshSales sync")
    client.state.update(state)
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema

        # double check, even though leads are removed from catalog from discovery
        if stream_id == 'leads' and client.version == 'new':
            continue

        # add dynamic custom module to stream objects
        if not STREAM_OBJECTS.get(stream_id):
            STREAM_OBJECTS[stream_id] = tap_freshsales.streams.CustomModule

            # update stream name of new discovered stream (custom module name)
            tap_freshsales.streams.CustomModule.stream_name = stream_id
            tap_freshsales.streams.CustomModule.stream_id = stream_id
        try:
            stream_object = STREAM_OBJECTS.get(stream_id)(client, config, state)
        except Exception as e:
            print(e)
            continue
        schema = stream_schema.to_dict()

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        singer.write_schema(
            stream_id,
            schema,
            stream_object.key_properties,
            stream_object.replication_keys,
        )

        logger.info("Syncing stream {}".format(stream))
        with Transformer() as transformer:
            for rec in stream_object.sync():
                # update custom fields from data retrieved
                custom_fields = rec.get('custom_fields', False)
                if custom_fields:
                    rec.update(custom_fields)
                    rec.pop('custom_fields')

                singer.write_record(
                    stream_id,
                    transformer.transform(rec,
                                          schema,
                                          metadata.to_map(stream.metadata))
                )
            logger.info("Completed sync")
