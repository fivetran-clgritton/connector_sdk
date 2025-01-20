
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import requests to make HTTP calls to API
import requests as rq
import traceback
import datetime
import json
import copy

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code



# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):

    return [

        {"table": "restaurant","primary_key": ["restaurantGuid"]},
        # labor tables
        {"table": "job", "primary_key": ["guid"]},
        {"table": "shift", "primary_key": ["guid"]},
        {"table": "employee", "primary_key": ["guid"]},
        {"table": "employee_job_reference", "primary_key": ["guid", "employee_guid"]},
        #{"table": "employee_wage_override", "primary_key": ["guid"]},
        {"table": "time_entry", "primary_key": ["guid"]},
        {"table": "break", "primary_key": ["guid"]},
        # config tables
        {"table": "alternate_payment_types", "primary_key": ["guid"]},
        {"table": "dining_option", "primary_key": ["guid"]},
        {"table": "discounts", "primary_key": ["guid"]},
        {"table": "menu_group", "primary_key": ["guid"]},
        {"table": "menu_item", "primary_key": ["guid"]},
        {"table": "restaurant_service", "primary_key": ["guid"]},
        {"table": "revenue_center", "primary_key": ["guid"]},
        {"table": "sale_category", "primary_key": ["guid"]},
        {"table": "service_area", "primary_key": ["guid"]},
        {"table": "tables", "primary_key": ["guid"]},
        # orders tables
        {"table": "orders", "primary_key":["guid"]},
        {"table": "orders_check", "primary_key":["guid"]},
        {"table": "orders_check_applied_discount", "primary_key":["guid"]},
        {"table": "orders_check_applied_discount_combo_item", "primary_key":["guid"]},
        {"table": "orders_check_payment", "primary_key": ["orders_check_guid", "payment_guid", "orders_guid"]},
        {"table": "orders_check_selection", "primary_key":["guid", "orders_check_guid"]},
        {"table": "orders_check_selection_applied_tax", "primary_key":["guid"]},
        {"table": "orders_check_selection_modifier", "primary_key":["guid", "orders_check_selection_guid"]},
        {"table": "orders_pricing_feature", "primary_key":["orders_guid"]},
        {"table": "orders_marketplace_facilitator_tax_info", "primary_key":["guid"]}

    ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):

    try:
        domain = configuration["domain"]
        base_url = f"https://{domain}"
        headers = make_headers(configuration, base_url)

        start_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat("T", "milliseconds").replace("+00:00", "Z")
        to_ts, from_ts = set_timeranges(state, configuration, start_timestamp)

        # Update the state with the new cursor position.
        # why do this here?
        # new_state = {"to_ts": to_ts}
        # log.fine(f"state updated, new state: {repr(new_state)}")

        # Yield a checkpoint operation to save the new state.
        yield from sync_items(base_url, headers, from_ts, to_ts, start_timestamp)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


# The function takes five parameters:
# - base_url: The URL to the API endpoint.
# - headers: Authentication headers
# ...
def sync_items(base_url, headers, ts_from, ts_to, start_timestamp):
    more_data = True
    first_pass = False

    config_endpoints = [("/config/v2/alternatePaymentTypes", "alternate_payment_types"),
                        ("/config/v2/diningOptions", "dining_option"),
                        ("/config/v2/discounts", "discounts"),
                        ("/config/v2/menuGroups", "menu_group"),
                        ("/config/v2/menuItems", "menu_item"),
                        ("/config/v2/restaurantServices", "restaurant_service"),
                        ("/config/v2/revenueCenters", "revenue_center"),
                        ("/config/v2/salesCategories", "sale_category"),
                        ("/config/v2/serviceAreas", "service_area"),
                        ("/config/v2/tables", "tables")]

    while more_data:
        # set timerange dicts
        timerange_params = {"startDate": ts_from, "endDate": ts_to}
        config_params = {"lastModified": ts_from}
        state = {"to_ts": ts_to}
        log.fine(f"timerange_params: {timerange_params}")
        log.fine(f"config_params: {config_params}")
        log.fine(f"state updated, new state: {repr(state)}")

        # Get response from API call.
        response_page, next_token = get_api_response(base_url+"/partners/v1/restaurants", headers)

        # Process the items.
        if not response_page:
            break  # End pagination if there are no records in response.

        # Iterate over each user in the 'items' list and yield an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        restaurant_count = len(response_page)
        for index, r in enumerate(response_page):
            guid = r["restaurantGuid"]
            log.info(f"***** starting restaurant {guid}, {index + 1} of {restaurant_count} ***** ")
            yield op.upsert(table="restaurant", data=r)

            # cash management endpoints
            # cashmgmt/v1/deposits
            # cashmgmt/v1/entries

            # config endpoints
            # only process these on the first pass since they don't have an end timestamp
            if first_pass:
                for endpoint, table_name in config_endpoints:
                    yield from process_config(base_url, headers, endpoint,table_name, guid, config_params)

                # no timerange_params, only sync during first pass
                for endpoint, table_name in [("/labor/v1/jobs", "job"),("/labor/v1/employees", "employee")]:
                    yield from process_labor(base_url, headers, endpoint, table_name, guid)

            # orders
            yield from process_orders(base_url, headers, "/orders/v2/ordersBulk", "orders", guid, timerange_params)

            # labor endpoints
            # with timerange_params -- can only do 30 days at a time
            yield from process_labor(base_url, headers, "/labor/v1/shifts", "shift", guid, params=timerange_params)
            yield from process_labor(base_url, headers, "/labor/v1/timeEntries", "time_entry", guid, params=timerange_params)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        # checkpointing every 30 days for convenience,
        # since we can only ask for 30 days of shifts and time entries at a time
        yield op.checkpoint(state)
        first_pass = False

        # testing only
        # more_data = False

        # Determine if we should continue pagination based on the total items and the current offset.
        if ts_to < start_timestamp:
            # get new timestamps
            ts_to, ts_from = set_timeranges(state, {}, start_timestamp)
        else:
            more_data = False

# for processing configuration endpoints
# timerange dictionary needs to be passed as data
# they use token pagination, which needs to be passed as params
def process_config(base_url, headers, endpoint, table_name, rst_guid, timerange):
    headers["Toast-Restaurant-External-ID"] = rst_guid
    more_data = True
    pagination = {}

    while more_data:
        try:
            next_token = None
            param_string = "&".join(f"{key}={value}" for key, value in timerange.items())
            response_page, next_token = get_api_response(base_url + endpoint + "?" + param_string, headers, params=pagination)
            #response_page, next_token = get_api_response(base_url + endpoint, headers, params=pagination, data=data)
            log.fine(f"restaurant {rst_guid}: response_page has {len(response_page)} items for {endpoint}")
            for o in response_page:
                o = stringify_lists(o)
                """
                # not sure if any configuration endpoints have "deleted" field
                if "deleted" in o and "guid" in o:
                    if o["deleted"]:
                        # log.fine(f"deleted record for {table_name}, {o}")
                        yield op.delete(table=table_name, keys={"guid": o["guid"]})
                    else:
                        yield op.upsert(table=table_name, data=o)
                else:
                    yield op.upsert(table=table_name, data=o)
                """
                o["restaurant_guid"] = rst_guid
                yield op.upsert(table=table_name, data=o)

            if next_token:
                pagination["pageToken"] = next_token
                log.fine(f"restaurant {rst_guid}: getting more {endpoint} with {pagination}")
            else:
                log.fine(f"restaurant {rst_guid}: last page reached for {endpoint}")
                more_data = False

        except Exception as e:
            # Return error response
            exception_message = str(e)
            stack_trace = traceback.format_exc()
            detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
            raise RuntimeError(detailed_message)

# for processing labor endpoints
# they do not use pagination
# dictionary of time ranges is optional for breaks, shifts, and time entries
# not accepted for jobs and employees
def process_labor(base_url, headers, endpoint, table_name, rst_guid, params=None):
    if params is None:
        params = {}
    headers["Toast-Restaurant-External-ID"] = rst_guid

    try:
        response_page, next_token = get_api_response(base_url + endpoint, headers, params=params)
        log.fine(f"restaurant {rst_guid}: response_page has {len(response_page)} items for {endpoint}")
        for o in response_page:
            # just call process_child with some changes? This is really ugly
            if endpoint == "/labor/v1/timeEntries" and "breaks" in o and len(o["breaks"]) > 0:
                yield from process_child(o["breaks"], "break", "time_entry_guid", o["guid"])
            if endpoint == "/labor/v1/employees":
                if "jobReferences" in o and len(o["jobReferences"]) > 0:
                    yield from process_child(o["jobReferences"], "employee_job_reference", "employee_guid", o["guid"])
                if "wageOverrides" in o and len(o["wageOverrides"]) > 0:
                    yield from process_child(o["wageOverrides"], "employee_wage_override", "employee_guid", o["guid"])
            o = stringify_lists(o)
            o["restaurant_guid"] = rst_guid
            yield op.upsert(table=table_name, data=o)
            if "deleted" in o and "guid" in o and o["deleted"]:
                yield op.delete(table=table_name, keys={"guid": o["guid"]})

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

# for processing orders
# uses timerange_params and fixed-size pagination
def process_orders(base_url, headers, endpoint, table_name, rst_guid, params):
    headers["Toast-Restaurant-External-ID"] = rst_guid
    #params = copy.deepcopy(params)
    params["pageSize"] = 100
    params["page"] = 1
    more_pages = True

    try:
        while more_pages:
            response_page, next_token = get_api_response(base_url + endpoint, headers, params=params)
            log.fine(f"restaurant {rst_guid}: response_page has {len(response_page)} items for {endpoint}")
            for o in response_page:
                if len(o["checks"]) > 0:
                    for c in o["checks"]:
                        if "payments" in c:
                            for payment in c["payments"]:
                                orders_check_payment = {"orders_check_guid": c["guid"],
                                                        "payment_guid": payment["guid"],
                                                        "orders_guid": o["guid"]}
                                yield op.upsert(table="orders_check_payment", data=orders_check_payment)
                                yield op.upsert(table="payment", data=payment)
                    yield from process_child(o["checks"], "orders_check", "orders_guid", o["guid"])

                #o.pop("checks")
                # pricingFeatures is a list of strings, not a list of dicts, needs special handling
                if len(o["pricingFeatures"]) > 0:
                    for p in o["pricingFeatures"]:
                        pricing_feature = {"orders_guid": o["guid"], "pricing_feature": p}
                        log.fine(f"upserting {pricing_feature}")
                        yield op.upsert(table="orders_pricing_feature", data=pricing_feature)
                # o.pop("pricingFeatures")
                flatten_dict(o, o["server"], "server")
                o = stringify_lists(o)
                o["restaurant_guid"] = rst_guid
                yield op.upsert(table=table_name, data=o)
                if "deleted" in o and "guid" in o and o["deleted"]:
                    yield op.delete(table=table_name, keys={"guid": o["guid"]})

            if len(response_page) == params["pageSize"]:
                params["page"] = params["page"] + 1
            else:
                more_pages = False

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def process_child (parent, table_name, id_field_name, id_field):
    relationships = {"orders_check": [
            ("selections", "orders_check_selection"),
            ("appliedDiscounts", "orders_check_applied_discount"),
            ("appliedServiceCharges", "orders_check_applied_service_charge")],
        "orders_check_applied_discount": [
            ("comboItems", "orders_check_applied_discount_combo_item"),
            ("triggers", "orders_check_applied_discount_trigger")],
        "orders_check_applied_service_charge": [
            ("appliedTax", "orders_check_applied_service_charge_applied_tax")],
        "orders_check_selection": [
            ("appliedTaxes", "orders_check_selection_applied_tax"),
            ("modifiers", "orders_check_selection_modifier")]
                     }
    for p in parent:
        log.fine(f"processing {table_name}")
        p[id_field_name] = id_field
        # DRY this out
        if table_name in relationships:
            for child_key, child_table_name in relationships[table_name]:
                if len(p.get(child_key, [])) > 0:  # Use .get() to handle missing keys gracefully
                    yield from process_child(
                        p[child_key],
                        child_table_name,
                        table_name + "_guid",
                        p["guid"]
                    )
        p = stringify_lists(p)
        yield op.upsert(table=table_name, data=p)

def make_headers(conf, base_url):
    payload = {"clientId": conf["clientId"],
               "clientSecret": conf["clientSecret"],
               "userAccessType": conf["userAccessType"]}

    auth_response = rq.post(base_url + "/authentication/v1/authentication/login", json=payload)
    auth_page = auth_response.json()
    auth_token = auth_page["token"]["accessToken"]

    headers = {"Authorization": "Bearer " + auth_token, "accept": "application/json"}
    return headers

def set_timeranges(state, configuration, start_timestamp):

    if 'to_ts' in state:
        from_ts = state['to_ts']
    else:
        from_ts = configuration["initialSyncStart"]

    if is_older_than_30_days(from_ts):
        to_ts = datetime.datetime.fromisoformat(from_ts) + datetime.timedelta(days=30)
        to_ts = datetime.datetime.isoformat(to_ts, "T", "milliseconds").replace("+00:00", "Z")
    else:
        to_ts = start_timestamp

    return to_ts, from_ts

# The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
# It performs the following tasks:
# 1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
# 2. Makes the API request using the 'requests' library, passing the URL and parameters.
# 3. Parses the JSON response from the API and returns it as a dictionary.
#
# The function takes two parameters:
# - base_url: The URL to which the API request is made.
# - params: A dictionary of query parameters to be included in the API request.
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
def get_api_response(endpoint_path, headers, **kwargs):
    # get response
    timerange_data = kwargs["data"] if "data" in kwargs else {}
    params = copy.deepcopy(kwargs["params"]) if "params" in kwargs else {}
    response = rq.get(endpoint_path, headers=headers, data=timerange_data, params=params)

    if response.status_code == 409:
        # recommended by Toast to retry without pageToken
        params.pop("pageToken")
        log.info(f"received 409 error, retrying {endpoint_path} without pageToken")
        response = rq.get(endpoint_path, headers=headers, data=params)

    if response.status_code == 400:
        log.info(response.json()["message"])

    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    response_headers = response.headers
    next_page_token = response_headers["Toast-Next-Page-Token"] if "Toast-Next-Page-Token" in response_headers else None
    return response_page, next_page_token

# The stringify_lists function changes lists to strings
#
# The function takes one parameter:
# - d: a dictionary
#
# Returns:
# - new_dict: A dictionary without any values that are lists
def stringify_lists(d):
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, list):
            new_dict[key] = str(value)
        else:
            new_dict[key] = value
    return new_dict

def flatten_dict (parent_row: dict, dict_field: dict, prefix: str):
    d = stringify_lists(dict_field)
    for key, value in d.items():
        parent_row[prefix+key] = value
    return parent_row

def is_older_than_30_days(date_to_check):

    today = datetime.date.today()
    thirty_days_ago = str(today - datetime.timedelta(days=30))
    return date_to_check < thirty_days_ago

# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "main":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

