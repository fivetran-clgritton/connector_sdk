# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import requests to make HTTP calls to API
import requests as rq
import traceback
import datetime
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import urllib.parse

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):

    return [
        {
            "table": "album",
            "primary_key": ["id"]
        },
        {
            "table": "track",
            "primary_key": ["id"]
        }
    ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    # business_cursor = state["business_cursor"] if "business_cursor" in state else '0001-01-01T00:00:00Z'
    # department_cursor = state["department_cursor"] if "department_cursor" in state else {}

    try:
        conf = configuration
        client_id = conf['client_id']
        client_secret = conf['client_secret']
        artist_url = conf['artist_url']
        redirect_uri = conf['redirect']  # Replace with your callback URL

        sp_oauth = SpotifyOAuth(client_id=client_id,
                                client_secret=client_secret,
                                redirect_uri=redirect_uri,
                                scope="user-library-read"
                                )
        auth_url = sp_oauth.get_authorize_url()
        log.info(auth_url)

        parsed_url = urllib.parse.urlparse(auth_url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        code = query_params.get('code', [None])[0]
        token_info = sp_oauth.get_access_token(code)

        sp = spotipy.Spotify(auth=token_info['access_token'])

        yield from sync_items(sp, "artist_albums", artist_url)


    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


# The function takes three parameters:
# - base_url: The URL to the API endpoint.
# - params: A dictionary of query parameters to be sent with the API request.
def sync_items(obj, method, payload):
    # Get response from API call.
    response_page = get_api_response(obj, method, payload)
    albums = response_page["items"]
    log.info("total items " + str(response_page["total"]))

    # Process the items.
    while response_page["next"]:
        response_page = get_api_response(obj, "next", response_page)
        log.info("total items " + str(response_page["total"]) + ", current items " + str(len(response_page["items"])))
        albums.extend(response_page["items"])
        if not albums:
            break  # End pagination if there are no records in response.

    # Iterate over each user in the 'items' list and yield an upsert operation.
    # The 'upsert' operation inserts the data into the destination.
    # Update the state with the 'updatedAt' timestamp of the current item.
    summary_first_item = {'id': albums[0]['id'], 'name': albums[0]['name']}
    log.info(f"processing items. First item starts: {summary_first_item}, Total items: {len(albums)}")

    for a in albums:
        album_data = flatten_dict(a)
        response_page = get_api_response(obj, "album_tracks", a["id"])
        tracks = response_page["items"]
        summary_first_item = {'id': tracks[0]['id'], 'name': tracks[0]['name']}
        log.info(f"processing tracks. First item starts: {summary_first_item}, Total items: {len(tracks)}")
        for t in tracks:
            track_data = flatten_dict(t)
            yield op.upsert(table="track", data=track_data)

        yield op.upsert(table="album", data=album_data)



    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of interruptions.
    # yield op.checkpoint(state)


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
def get_api_response(obj, method_name, payload=""):
    log.info(f"Making call to {method_name}")
    method = getattr(obj, method_name)
    response_page = method(payload)
    return response_page

# The get_next_page_url_from_response function extracts the URL for the next page of data from the API response.
#
# The function takes one parameter:
# - response_page: A dictionary representing the parsed JSON response from the API.
#
# Returns:
# - The URL for the next page if it exists, otherwise None.

def flatten_dict(d):
    flattened_dict = {}
    for key, value in d.items():
        if isinstance(value, list):
            pass
        else:
            flattened_dict[key] = value

    return flattened_dict

# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
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

