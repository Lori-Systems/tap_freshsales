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
    """
    The parameter filter is mandatory.
    Only one filter is allowed at a time.
    Getting multiple filtered accounts is not possible.
    Use 'include' to embed additional details in the response.
    """
    stream_id = 'sales_accounts'
    stream_name = 'accounts'
    endpoint = 'api/sales_accounts'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        """
            Accounts are filtered from views
            Same accounts could be in different views
            ex. My Accounts, All Accounts, "My Territory Accounts, etc
            All accounts could be retrieved from set(all accounts, recycle bin)
        """
        stream = self.endpoint
        filters = self.client.get_filters(stream)
        all_accounts_filters = ['All Accounts', 'Recycle Bin']  # these are default views
        for acc_filter in filters:
            # make sure to not have duplicated from different groups of view/ filters
            if acc_filter['name'] not in all_accounts_filters:
                continue

            view_id = acc_filter['id']
            view_name = acc_filter['name']
            grouped_entity = self.stream_name + "_" + str(view_id)
            start = self.client.get_start(grouped_entity)
            LOGGER.info("Syncing stream '{}' of view '{}' with ID {} from {}".format(
                self.stream_name, view_name, view_id, start))
            records = self.client.gen_request('GET', self.stream_id,
                                              self.client.url(stream, query='view/' + str(view_id) +
                                                                            '?include=owner'))
            state_date = start
            for record in records:
                # sorted by updated at
                record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
                if record_date >= start:
                    state_date = record['updated_at']
                    yield record

            # update stream state with 1 sec for the next data retrieval
            state_date = tap_utils.strftime(tap_utils.strptime(state_date) + datetime.timedelta(seconds=1))
            tap_utils.update_state(self.client.state, grouped_entity, state_date)
            singer.write_state(self.client.state)


class Appointments(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered appointments is not possible.
        Use 'include' to embed additional details in the response.
    """
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
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered contacts is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = 'contacts'
    stream_name = 'contacts'
    # endpoint = 'api/filtered_search/contact' (POST with payload)-> response does not include all fields
    endpoint = 'api/contacts'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]

    def sync(self):
        filters = self.client.get_filters(self.endpoint)
        # all inclusive filters - skip duplicated contacts
        all_contacts_filters = ['All Contacts', 'Recycle Bin']
        for contact_filter in filters:
            if contact_filter['name'] not in all_contacts_filters:
                continue

            view_id = contact_filter['id']
            view_name = contact_filter['name']
            grouped_entity = self.stream_name + "_" + str(view_id)
            start = self.client.get_start(grouped_entity)
            LOGGER.info("Syncing stream '{}' of view '{}' with ID {} from {}".format(
                self.stream_name, view_name, view_id, start))
            records = self.client.gen_request('GET', self.stream_name,
                                              self.client.url(self.endpoint, query='view/' + str(view_id) +
                                                                                   '?include=owner,sales_account'))
            state_date = start
            for record in records:
                record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
                if record_date >= start:
                    state_date = record['updated_at']
                    # return records that fulfill the date condition
                    yield record

            # update stream state with 1 sec for the next data retrieval
            state_date = tap_utils.strftime(tap_utils.strptime(state_date) + datetime.timedelta(seconds=1))
            tap_utils.update_state(self.client.state, grouped_entity, state_date)
            singer.write_state(self.client.state)


class Leads(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered deals is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = 'leads'
    stream_name = 'leads'
    endpoint = 'api/leads'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        filters = self.client.get_filters(stream)
        # possibility for duplicates in views:
        # ['My Leads', 'New Leads', 'Unassigned Leads', 'All Leads', 'Recently Modified', \
        # 'My Territory Leads', 'Never Contacted', 'Hot Leads', 'Warm Leads', '"Cold Leads"']
        all_leads_filters = ['All Leads']
        for lead_filter in filters:
            view_id = lead_filter['id']
            view_name = lead_filter['name']
            if view_name not in all_leads_filters:
                continue

            grouped_entity = self.stream_name + "_" + str(view_id)
            start = self.client.get_start(grouped_entity)
            LOGGER.info("Syncing stream '{}' of view '{}' with ID {} from {}".format(
                self.stream_name, view_name, view_id, start))
            records = self.client.gen_request('GET', self.stream_name,
                                              self.client.url(self.stream_name, query='view/' + str(view_id)
                                                                                      + '?include=owner'))
            state_date = start
            for record in records:
                # convert record date in UTC to make the comparison with state's date
                record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
                if record_date >= start:
                    LOGGER.info("Deal {} - {}: Syncing details".format(record['name'], record['id']))
                    # get all sub-entities and save them
                    record['amount'] = float(record['amount'])  # cast amount to float
                    record['custom_field'] = json.dumps(
                        record['custom_field'])  # Make JSON String to store

                    state_date = record['updated_at']
                    yield record

            # update stream state with 1 sec for the next data retrieval
            state_date = tap_utils.strftime(tap_utils.strptime(state_date) + datetime.timedelta(seconds=1))
            tap_utils.update_state(self.client.state, grouped_entity, state_date)
            singer.write_state(self.client.state)


class Deals(Stream):
    """
        The parameter filter is mandatory.
        Only one filter is allowed at a time.
        Getting multiple filtered deals is not possible.
        Use 'include' to embed additional details in the response.
    """
    stream_id = 'deals'
    stream_name = 'deals'
    endpoint = 'api/deals'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        filters = self.client.get_filters(stream)
        # possibility for duplicates in views:
        # ['My Deals', 'My Territory Deals', 'Recent Deals', 'Recently Imported', \
        # 'Hot Deals', 'Cold Deals', 'Closing this week', 'This month's sales']
        all_deals_filters = ['Open Deals', 'Lost Deals', 'Won Deals', 'Recycle Bin']
        for d_filter in filters:
            view_id = d_filter['id']
            view_name = d_filter['name']
            if view_name not in all_deals_filters:
                continue

            grouped_entity = self.stream_name + "_" + str(view_id)
            start = self.client.get_start(grouped_entity)
            LOGGER.info("Syncing stream '{}' of view '{}' with ID {} from {}".format(
                self.stream_name, view_name, view_id, start))
            records = self.client.gen_request('GET', self.stream_name,
                                              self.client.url(self.stream_name, query='view/' + str(view_id)
                                                                                      + '?include=owner'))
            state_date = start
            for record in records:
                # convert record date in UTC to make the comparison with state's date
                record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
                if record_date >= start:
                    LOGGER.info("Deal {} - {}: Syncing details".format(record['name'], record['id']))
                    # get all sub-entities and save them
                    record['amount'] = float(record['amount'])  # cast amount to float
                    record['custom_field'] = json.dumps(
                        record['custom_field'])  # Make JSON String to store

                    state_date = record['updated_at']
                    yield record

            # update stream state with 1 sec for the next data retrieval
            state_date = tap_utils.strftime(tap_utils.strptime(state_date) + datetime.timedelta(seconds=1))
            tap_utils.update_state(self.client.state, grouped_entity, state_date)
            singer.write_state(self.client.state)


class Owners(Stream):
    stream_id = 'owners'
    stream_name = 'owners'
    endpoint = 'api/selector/owners'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class Sales(Stream):
    stream_id = 'sales'
    stream_name = 'sales'
    endpoint = 'api/sales_activities'
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

    def sync(self):
        stream = self.endpoint
        start = self.client.get_start(self.stream_name)
        LOGGER.info("Syncing stream {} from {}".format(stream, start))

        records = self.client.gen_request('GET', self.stream_name, self.client.url(stream))
        state_date = start
        for record in records:
            record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
            if record_date >= start:
                state_date = record['updated_at']
                yield record

        # update stream state with 1 sec for the next data retrieval
        state_date = tap_utils.strftime(tap_utils.strptime(state_date) + datetime.timedelta(seconds=1))
        tap_utils.update_state(self.client.state, self.stream_name, state_date)
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
            records = self.client.gen_request('GET', self.stream_name,
                                              self.client.url(stream, filter=state_filter,
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
    replication_method = "INCREMENTAL"
    replication_keys = ['updated_at']

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
        start = self.client.get_start(self.stream_name)
        repl = self.replication_keys
        data = {
            "filter_rule": [
                {
                    "attribute": repl and repl[0] or False,
                    "operator": "is_after",
                    "value": start
                }
            ]
        }
        # GET custom modules
        try:
            records = self.client.gen_request('POST', stream, self.client.url(stream), payload=data)
        except Exception as e:
            # eg. may not have updated_at field. Request with date false in payload will fail
            LOGGER.error("Exception on custom module sync with message: ", e)
            return

        start_state = start
        for record in records:
            LOGGER.info("Lead {}: Syncing details".format(record['id']))
            if record.get('updated_at', False):
                record_date = tap_utils.strftime(tap_utils.strptime(record['updated_at']))
                if record_date >= start:
                    start_state = record.get('updated_at', False)
                    yield record
            else:
                yield record

        # update stream state with 1 sec for the next data retrieval
        start_state = tap_utils.strftime(tap_utils.strptime(start_state) + datetime.timedelta(seconds=1))
        tap_utils.update_state(self.client.state, self.stream_name, start_state)
        singer.write_state(self.client.state)


class Territories(Stream):
    stream_id = 'territories'
    stream_name = 'territories'
    endpoint = 'api/selector/territories'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class DealStages(Stream):
    stream_id = 'deal_stages'
    stream_name = 'deal_stages'
    endpoint = 'api/selector/deal_stages'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class DealReasons(Stream):
    stream_id = 'deal_reasons'
    stream_name = 'deal_reasons'
    endpoint = 'api/selector/deal_reasons'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class DealTypes(Stream):
    stream_id = 'deal_types'
    stream_name = 'deal_types'
    endpoint = 'api/selector/deal_types'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class IndustryTypes(Stream):
    stream_id = 'industry_types'
    stream_name = 'industry_types'
    endpoint = 'api/selector/industry_types'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class BusinessTypes(Stream):
    stream_id = 'business_types'
    stream_name = 'business_types'
    endpoint = 'api/selector/business_types'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class Campaigns(Stream):
    stream_id = 'campaigns'
    stream_name = 'campaigns'
    endpoint = 'api/selector/campaigns'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class DealPaymentStatuses(Stream):
    stream_id = 'deal_payment_statuses'
    stream_name = 'deal_payment_statuses'
    endpoint = 'api/selector/deal_payment_statuses'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class DealProducts(Stream):
    stream_id = 'deal_products'
    stream_name = 'deal_products'
    endpoint = 'api/selector/deal_products'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class DealPipelines(Stream):
    stream_id = 'deal_pipelines'
    stream_name = 'deal_pipelines'
    endpoint = 'api/selector/deal_pipelines'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class ConstactStatuses(Stream):
    stream_id = 'contact_statuses'
    stream_name = 'contact_statuses'
    endpoint = 'api/selector/contact_statuses'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class SalesActivityTypes(Stream):
    stream_id = 'sales_activity_types'
    stream_name = 'sales_activity_types'
    endpoint = 'api/selector/sales_activity_types'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class SalesActivityOutcomes(Stream):
    stream_id = 'sales_activity_outcomes'
    stream_name = 'sales_activity_outcomes'
    endpoint = 'api/selector/sales_activity_outcomes'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class SalesActivityEntityTypes(Stream):
    stream_id = 'sales_activity_entity_types'
    stream_name = 'sales_activity_entity_types'
    endpoint = 'api/selector/sales_activity_entity_types'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


class LifecycleStages(Stream):
    stream_id = 'lifecycle_stages'
    stream_name = 'lifecycle_stages'
    endpoint = 'api/selector/lifecycle_stages'
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def sync(self):
        LOGGER.info("Syncing stream '{}'".format(self.stream_name))
        records = self.client.gen_request('GET', self.stream_name, self.client.url(self.endpoint))
        for record in records:
            yield record


STREAM_OBJECTS = {
    # Main entities
    'accounts': Accounts,
    'appointments': Appointments,
    'contacts': Contacts,
    'leads': Leads,  #  Leads only work with old version of freshsales
    'deals': Deals,
    'owners': Owners,
    'sales_activities': Sales,
    'tasks': Tasks,

    # custom module support
    'custom_module': CustomModule,

    # configuration support
    'territories': Territories,
    'deal_stages': DealStages,
    'deal_reasons': DealReasons,
    'deal_types': DealTypes,
    'industry_types': IndustryTypes,
    'business_types': BusinessTypes,
    'campaigns': Campaigns,
    'deal_payment_statuses': DealPaymentStatuses,
    'deal_products': DealProducts,
    'deal_pipelines': DealPipelines,
    'contact_statuses': ConstactStatuses,
    # TODO: remove from inline entity call activity type & outcome
    'sales_activity_types': SalesActivityTypes,
    'sales_activity_outcomes': SalesActivityOutcomes,
    'sales_activity_entity_types': SalesActivityEntityTypes,
    'lifecycle_stages': LifecycleStages
}
