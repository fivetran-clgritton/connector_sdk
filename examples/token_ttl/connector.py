import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
import json
import time

TOKEN_EXPIRY = 15 * 60  # 15 minutes in seconds
MINIMUM_VALIDITY = 60  # At least 1 minute remaining before expiry


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    :param configuration: a dictionary that holds the configuration settings for the connector.
    :return: any necessary schema definition
    """
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "updatedAt": "UTC_DATETIME",
                "createdAt": "UTC_DATETIME",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    The state dictionary is empty for the first sync or for any full re-sync
    :param configuration: dictionary contains any secrets or payloads you configure when deploying the connector
    :param state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    :return:
    """
    log.warning("Example: Common Patterns For Connectors - Authentication - API KEY")
    print("RECOMMENDATION: Please ensure the base url is properly set.")
    base_url = "http://127.0.0.1:5001/auth/session_token"
    yield from sync_items(base_url, {}, state, configuration)


def get_session_token(base_url, config, state):
    """
    Get a session token if there is none in the state dictionary or if it is about to expire
    :param base_url: The URL to the API endpoint.
    :param config: dictionary contains any secrets or payloads you configure when deploying the connector
    :param state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    :return: authentication token
    """
    current_time = time.time()
    token = state.get("token")
    timestamp = state.get("timestamp", 0)

    if token and (current_time - timestamp < TOKEN_EXPIRY - MINIMUM_VALIDITY):
        log.fine(f"reusing existing token from {state}")
        return token

    log.fine(f"token from state is expired or about to expire, getting new one")
    username = config.get('username')
    password = config.get('password')

    if username is None or password is None:
        raise ValueError("Username or Password is missing in the configuration.")

    token_url = base_url + "/login"
    body = {"username": username, "password": password}
    log.info(f"Making API call to url: {token_url} with body: {body}")

    response = rq.post(token_url, json=body)
    response.raise_for_status()
    response_page = response.json()

    state["token"] = response_page.get("token")
    state["timestamp"] = current_time

    return state["token"]


def get_auth_headers(session_token):
    """
    Define the get_auth_headers function, which is your custom function to generate auth headers for making API calls.
    :param session_token: session token
    :return: headers with session token
    """
    return {"Authorization": f"Token {session_token}", "Content-Type": "application/json"}


def sync_items(base_url, params, state, configuration):
    """
    The sync_items function handles the retrieval of API data.
    It performs the following tasks:
    1. Sends an API request to the specified URL with the provided parameters.
    2. Processes the items returned in the API response by yielding upsert operations to Fivetran.
    3. Saves the state periodically to ensure the sync can resume from the correct point.

    :param base_url: The URL to the API endpoint.
    :param params: A dictionary of query parameters to be sent with the API request.
    :param state: A dictionary representing the current state of the sync, including the last retrieved key.
    :param configuration: A dictionary contains any secrets or payloads you configure when deploying the connector.
    """
    session_token = get_session_token(base_url, configuration, state)
    items_url = base_url + "/data"
    response_page = get_api_response(items_url, params, get_auth_headers(session_token))

    items = response_page.get("data", [])
    if not items:
        return

    for user in items:
        yield op.upsert(table="user", data=user)

    yield op.checkpoint(state)


def get_api_response(base_url, params, headers):
    """
    The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
    It performs the following tasks:
    1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
    2. Makes the API request using the 'requests' library, passing the URL and parameters.
    3. Parses the JSON response from the API and returns it as a dictionary.

    :param base_url: The URL to the API endpoint.
    :param params: A dictionary of query parameters to be sent with the API request.
    :param headers: A dictionary of headers
    :return: A dictionary containing the parsed JSON response from the API.
    """
    log.info(f"Making API call to url: {base_url} with params: {params} and headers: {headers}")
    response = rq.get(base_url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    """
    Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
    be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
    Note this method is not called by Fivetran when executing your connector in production. Please test using the
    Fivetran debug command prior to finalizing and deploying your connector.
    """
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
