
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
        {"table": "orders", "primary_key":["guid"]},
        {"table": "job", "primary_key": ["guid"]},
        {"table": "shift", "primary_key": ["guid"]},
        {"table": "employee", "primary_key": ["guid"]},
        {"table": "time_entry", "primary_key": ["guid"]},
        {"table": "dining_option", "primary_key": ["guid"]},
        {"table": "discounts", "primary_key": ["guid"]},
        {"table": "menu_group", "primary_key": ["guid"]},
        {"table": "menu_item", "primary_key": ["guid"]},
        {"table": "menu", "primary_key": ["guid"]}
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
        conf = configuration
        domain = conf["domain"]
        base_url = f"https://{domain}"
        to_ts = datetime.datetime.now(datetime.timezone.utc).isoformat("T", "milliseconds")
        if 'to_ts' in state:
            from_ts = state['to_ts']
        else:
            from_ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=10)
            from_ts = from_ts.isoformat("T", "milliseconds")

        payload = {"clientId": conf["clientId"],
            "clientSecret": conf["clientSecret"],
            "userAccessType": conf["userAccessType"]}

        auth_response = rq.post(base_url + "/authentication/v1/authentication/login", json=payload)
        auth_page = auth_response.json()
        auth_token = auth_page["token"]["accessToken"]

        headers = {"Authorization": "Bearer " + auth_token, "accept": "application/json"}

        # Update the state with the new cursor position, incremented by 1.
        new_state = {"to_ts": to_ts}
        log.fine(f"state updated, new state: {repr(new_state)}")

        # Yield a checkpoint operation to save the new state.
        yield op.checkpoint(state=new_state)
        yield from sync_items(base_url, headers, from_ts, to_ts, state)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


# The function takes five parameters:
# - base_url: The URL to the API endpoint.
# - headers: Authentication headers
# - state: State dictionary
# - topic: current topic to search
# - params: A dictionary of query parameters to be sent with the API request.
def sync_items(base_url, headers, ts_from, ts_to, state):
    more_data = True
    timerange_params = {"startDate": ts_from, "endDate": ts_to}
    config_params = {"lastModified": ts_from}
    # config_params = {}

    while more_data:
        # Get response from API call.
        response_page, next_token = get_api_response(base_url+"/partners/v1/restaurants", headers, {})

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

            # config endpoints
            #yield from process_endpoint(base_url, headers, config_params, "/config/v2/diningOptions", "dining_option",  guid)
            #yield from process_endpoint(base_url, headers, config_params, "/config/v2/discounts", "discounts", guid)
            #yield from process_endpoint(base_url, headers, config_params, "/config/v2/menuGroups", "menu_group", guid)
            #yield from process_endpoint(base_url, headers, config_params, "/config/v2/menuItems", "menu_item", guid)
            #yield from process_endpoint(base_url, headers, config_params, "/config/v2/menus", "menu", guid)

            # with timerange_params
            yield from process_endpoint(base_url, headers, timerange_params, "/orders/v2/ordersBulk", "orders", guid)
            #yield from process_endpoint(base_url, headers, timerange_params, "/labor/v1/shifts", "shift", guid)
            #yield from process_endpoint(base_url, headers, timerange_params, "/labor/v1/timeEntries", "time_entry", guid)

            # no timerange_params
            #yield from process_endpoint(base_url, headers, {}, "/labor/v1/jobs", "job", guid)
            #yield from process_endpoint(base_url, headers, {}, "/labor/v1/employees", "employee", guid)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.
        yield op.checkpoint(state)

        # Determine if we should continue pagination based on the total items and the current offset.
        #more_data, params = should_continue_pagination(params, response_page)
        more_data = False

def process_endpoint(base_url, headers, params, endpoint, table_name, rst_guid):
    headers["Toast-Restaurant-External-ID"] = rst_guid
    more_data = True
    params_copy = copy.deepcopy(params)

    while more_data:
        try:
            next_token = None
            response_page, next_token = get_api_response(base_url + endpoint, headers, params_copy)
            log.fine(f"restaurant {rst_guid}: response_page has {len(response_page)} items for {endpoint}")
            for o in response_page:
                o = stringify_lists(o)
                if "deleted" in o and "guid" in o:
                    if o["deleted"]:
                        log.fine(f"deleted record for {table_name}, {o}")
                        yield op.delete(table=table_name, keys={"guid": o["guid"]})
                    else:
                        yield op.upsert(table=table_name, data=o)
                else:
                    yield op.upsert(table=table_name, data=o)
            if next_token:
                params_copy["pageToken"] = next_token
                log.fine(f"restaurant {rst_guid}: getting more {endpoint} with {params_copy}")
            else:
                log.fine(f"restaurant {rst_guid}: last page reached for {endpoint}")
                more_data = False

        except Exception as e:
            # Return error response
            exception_message = str(e)
            stack_trace = traceback.format_exc()
            detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
            raise RuntimeError(detailed_message)

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
def get_api_response(endpoint_path, headers, params):
    # get response
    params_copy = copy.deepcopy(params)
    response = rq.get(endpoint_path, headers=headers, params=params_copy)

    if response.status_code == 409:
        # recommended by Toast to retry without pageToken
        params_copy.pop("pageToken")
        log.info(f"received 409 error, retrying {endpoint_path} without pageToken")
        response = rq.get(endpoint_path, headers=headers, params=params_copy)

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


# The should_continue_pagination function determines whether pagination should continue based on the
# current page number and the total number of results in the API response.
# It performs the following tasks:
# 1. Calculates the number of pages needed to retrieve all results based on pageSize parameter specified in
# configuration.json.
# 2. If the current page number is less than the total, updates the parameters with the new offset
# for the next API request.
# 3. If the current offset + current_page_size is greater than or equal to the total, or the next page will exceed the
# API result limit of 100 results per call, sets the flag to end the pagination process.
#
# Parameters:
# - params: A dictionary of query parameters used in the API request. It will be updated with the new offset.
# - response_page: A dictionary representing the parsed JSON response from the API.
#
# Returns:
# - has_more_pages: A boolean indicating whether there are more pages to retrieve.
# - params: The updated query parameters for the next API request.
#
def should_continue_pagination(params, response_page):
    has_more_pages = True

    # Determine if there are more pages to continue the pagination
    current_page = int(params["page"])
    total_pages = divmod(int(response_page["totalResults"]), int(params["pageSize"]))[0] + 1

    # 100 results is a temporary limit for dev API -- this limit can be removed if you have a paid API key
    if current_page and total_pages and current_page < total_pages and current_page * int(params["pageSize"]) < 100:
        # Increment the page number for the next request in params
        params["page"] = current_page + 1
    else:
        has_more_pages = False  # End pagination if there is no more pages pending.

    return has_more_pages, params

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

