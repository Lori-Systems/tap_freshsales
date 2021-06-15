# If using the class-based model, this is where all the stream classes and their corresponding functions live.
import json
import singer
import datetime
from tap_freshsales import tap_utils
import requests

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, client, config, state):
        self.client = client
        self.config = config
        self.state = state


class Accounts(Stream):
    stream_id = 'accounts'
    stream_name = 'accounts'
    endpoint = 'api/sales_accounts'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        filters = self.client.get_filters(stream)
        for acc_filter in filters:
            view_id = acc_filter['id']
            state_entity = self.stream_name + "_" + str(view_id) + " " + acc_filter['name']
            start = self.client.get_start(state_entity)
            LOGGER.info("Syncing stream {} from {}".format(stream, start))

            records = self.client.gen_request('GET', stream,
                                              self.client.url(stream, query='view/' + str(view_id) +
                                                                            '?include=owner'))

            for record in records:
                if record['updated_at'] >= start:
                    LOGGER.info("Account {}: Syncing details".format(record['id']))
                    tap_utils.update_state(self.client.state, state_entity, record['updated_at'])
                    yield record
            singer.write_state(self.client.state)


class Appointments(Stream):
    stream_id = 'appointments'
    stream_name = 'appointments'
    endpoint = 'api/appointments?filter={filter}&include={include}'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        stream = self.endpoint
        filters = ['past', 'upcoming']
        for filter_ in filters:
            url = self.client.url(stream, filter=filter_, include='creater,targetable,appointment_attendees')
            records = self.client.gen_request('GET', self.stream_id, url)
            for record in records:
                yield record


class Contacts(Stream):
    stream_id = 'contacts'
    stream_name = 'contacts'
    # endpoint = 'api/filtered_search/contact' -> response does not include all fields
    endpoint = 'api/contacts'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = ["updated_at"]

    def sync(self):
        filters = self.client.get_filters(self.endpoint)
        for contact_filter in filters:
            view_id = contact_filter['id']
            view_name = contact_filter['name']
            state_entity = self.stream_name + "_" + str(view_id) + " " + view_name
            start = self.client.get_start(state_entity)
            LOGGER.info("Syncing stream {} from {}".format(self.stream_name, start))

            records = self.client.gen_request('GET', self.endpoint,
                                              self.client.url(self.endpoint, query='view/' + str(view_id) +
                                                                                   '?include=owner,sales_account'))
            for record in records:
                if record['updated_at'] >= start:
                    LOGGER.info("Contact {}: Syncing details".format(record['id']))
                    tap_utils.update_state(self.client.state, state_entity, record['updated_at'])
                    # return records that fulfill the date condition
                    yield record
            singer.write_state(self.client.state)


class Deals(Stream):
    stream_id = 'deals'
    stream_name = 'deals'
    endpoint = 'api/deals'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        filters = self.client.get_filters(stream)
        for d_filter in filters:
            view_id = d_filter['id']
            state_entity = self.stream_name + "_" + str(view_id)
            start = self.client.get_start(state_entity)
            LOGGER.info("Syncing stream {} from {}".format(stream, start))

            records = self.client.gen_request('GET', stream,
                                              self.client.url(stream, query='view/' + str(view_id)
                                                                            + '?include=owner'))
            for record in records:
                if record['updated_at'] >= start:
                    LOGGER.info("Deal {}: Syncing details".format(record['id']))
                    # get all sub-entities and save them
                    record['amount'] = float(record['amount'])  # cast amount to float
                    record['custom_field'] = json.dumps(
                        record['custom_field'])  # Make JSON String to store

                    tap_utils.update_state(self.client.state, state_entity, record['updated_at'])
                yield record
            singer.write_state(self.client.state)


class Owners(Stream):
    stream_id = 'owners'
    stream_name = 'owners'
    endpoint = 'api/owners'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        for record in self.client.owners:
            state_entity = self.stream_name + "_" + str(record['id'])
            if state_entity not in self.state:
                LOGGER.info("Owner {}: Syncing details".format(record['id']))
                tap_utils.update_state(self.client.state, state_entity, record['id'])
                yield record
                singer.write_state(self.client.state)


class Sales(Stream):
    stream_id = 'sales'
    stream_name = 'sales'
    endpoint = 'api/sales_activities'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        start = self.client.get_start(stream)
        LOGGER.info("Syncing stream {} from {}".format(stream, start))

        records = self.client.gen_request('GET', stream, self.client.url(stream))
        for record in records:
            if record['updated_at'] >= start:
                LOGGER.info("Sale activity {}: Syncing details".format(record['id']))
                tap_utils.update_state(self.client.state, stream, record['updated_at'])
                yield record
        singer.write_state(self.client.state)


class Tasks(Stream):
    """
        The parameter filter is mandatory per task request
        Only one filter is allowed at a time. Getting multiple filtered tasks is not possible.
        For example, you can’t get both open and overdue tasks in a single request.
    """
    stream_id = 'tasks'
    stream_name = 'tasks'
    endpoint = 'api/tasks?filter={filter}'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    sales_activity_outcomes = {}
    sales_activity_types = {}

    def set_sales_activities(self):
        sales_activity_types = self.client.gen_request(method='GET',
                                                       url=self.client.url('api/selector/sales_activity_entity_types'),
                                                       stream=None)
        sales_activity_outcomes = self.client.gen_request(method='GET',
                                                          url=self.client.url('api/selector/sales_activity_outcomes'),
                                                          stream=None)
        for sale_act_type in sales_activity_types:
            self.sales_activity_types[sale_act_type['id']] = sale_act_type['name']

        for sale_act_outcome in sales_activity_outcomes:
            self.sales_activity_outcomes[sale_act_outcome['id']] = sale_act_outcome['name']

        return

    def sync(self):
        # update sales activities to match labels
        self.set_sales_activities()

        stream = self.endpoint
        # task has static filters - date related could be filtered after import
        # filters = ['open', 'due today', 'due tomorrow', 'overdue', 'completed']
        filters = ['open', 'completed']
        for state_filter in filters:
            LOGGER.info("Syncing stream {} {}".format(state_filter, stream))
            records = self.client.gen_request('GET', stream, self.client.url(stream, filter=state_filter,
                                                                             include='owner,users,targetable'))
            for record in records:
                # update boolean status with label
                record['status'] = state_filter
                record['outcome_label'] = self.sales_activity_outcomes.get(record['outcome_id'], None)
                record['task_type_label'] = self.sales_activity_types.get(record['task_type_id'], None)
                yield record


class CustomModule(Stream):
    stream_id = 'custom_module'
    stream_name = 'custom_module'
    endpoint = 'api/filtered_search/{stream}'
    custom_module = 'settings/module_customizations'
    custom_fields = 'settings/{name}/forms'
    key_properties = ["id"]
    # TODO: support incremental with possible updated_at field?
    replication_method = "FULL_TABLE"
    replication_keys = []

    def get_custom_module_schema(self):
        schema = {}
        endpoint = self.custom_fields.format(name=self.stream_name)
        custom_fields = self.client.gen_request('GET', stream=None, url=self.client.url(endpoint))
        forms = [x for x in custom_fields][0]
        if not forms:
            return schema

        # fields located inside basic_info :: ['basic_information','hidden_fields']
        custom_fields = forms.get('fields', False) and forms.get('fields', False)[0]
        for custom_field in custom_fields.get('fields', False):
            field_name, field_type = custom_field.get('name', False), custom_field.get('type', False)
            # all custom field types are strings actually
            schema[field_name] = {'type': ['null', 'string']}
        return schema

    def get_custom_modules(self):
        """
            return dict: schema per custom module
        """
        endpoint = self.custom_module
        custom_modules = self.client.gen_request('GET', self.stream_id, url=self.client.url(endpoint))
        all_custom_modules = {}
        for custom_module in custom_modules:
            entity = custom_module['entity_name']
            # override stream name
            self.stream_id = self.stream_name = entity
            self.endpoint = self.endpoint.format(stream=entity)
            schema = self.get_custom_module_schema()
            all_custom_modules[entity] = schema
        return all_custom_modules

    def sync(self):
        stream = self.endpoint.format(stream=self.stream_name)
        # GET custom modules
        records = self.client.gen_request('POST', stream, self.client.url(stream))
        for record in records:
            LOGGER.info("Lead {}: Syncing details".format(record['id']))
            yield record


STREAM_OBJECTS = {
    'accounts': Accounts,
    'appointments': Appointments,
    'contacts': Contacts,
    'deals': Deals,
    'owners': Owners,
    'sales_activities': Sales,
    'tasks': Tasks,
    'custom_module': CustomModule
}
