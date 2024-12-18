# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


# Import requests to make HTTP calls to API
import requests as rq
import traceback
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
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
            "table": "playlist",
            "primary_key": ["id", "artist_searched"]
        },
        {
            "table": "user",
            "primary_key": ["id"]
        },
        {
            "table": "playlist_owner",
            "primary_key": ["playlist", "owner_user"]
        },
        {
            "table": "playlist_track",
            "primary_key": ["playlist", "id"]
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
        artist_list = json.loads(conf['artist_list'])
        playlist_limit = conf["playlist_limit"]
        return_limit = conf["return_limit"]
        log.info("about to authenticate")

        auth_manager = SpotifyClientCredentials(client_id=client_id,
                                client_secret=client_secret
                                )

        sp = spotipy.Spotify(auth_manager=auth_manager)

        #parsed_url = urllib.parse.urlparse(auth_url)
        #query_params = urllib.parse.parse_qs(parsed_url.query)
        #code = query_params.get('code', [None])[0]
        token_info = auth_manager.get_access_token()

        for artist in artist_list:
            log.info(f"starting sync of at most {playlist_limit} playlists for {artist}")
            search_args = {"q": f"\"{artist}\"",
                           "limit": return_limit,
                           "type": "playlist",
                           "market": "US"}
            yield from sync_items(sp, "search", search_args, token_info, artist, playlist_limit)


    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


# The function takes three parameters:
# - base_url: The URL to the API endpoint.
# - params: A dictionary of query parameters to be sent with the API request.
def sync_items(obj, method, payload, token_info, artist, playlist_limit):
    # Get response from API call.
    playlists = []
    headers = {"Authorization": f"Bearer {token_info['access_token']}" }
    response_page = get_api_response(obj, method, payload)
    playlists.extend(item for item in response_page["playlists"]["items"] if item)
    log.fine("total items " + str(response_page["playlists"]["total"]))

    try:
        # Process the items.
        while response_page["playlists"]["next"] and len(playlists) < int(playlist_limit):
            response = rq.get(response_page["playlists"]["next"], headers=headers)
            response_page = response.json()
            log.fine("total items " + str(response_page["playlists"]["total"]) + ", current items " + str(len(response_page["playlists"]["items"])))
            playlists.extend(item for item in response_page["playlists"]["items"] if item)
            log.fine(str(len(playlists))+ " items in playlists")
            if not response_page["playlists"]:
                break  # End pagination if there are no records in response.

        # Iterate over each user in the 'items' list and yield an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        # Update the state with the 'updatedAt' timestamp of the current item.

        for p in playlists:
            if p:
                yield from process_playlist(obj, p, artist, payload["q"])
                yield op.checkpoint({})

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of interruptions.


    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def process_playlist(obj, p: dict, artist, q):
    pl_data = flatten_dict(p)
    playlist_id = pl_data["id"]
    pl_data["track_count"] = p["tracks"]["total"]
    pl_data["artist_searched"] = artist
    pl_data["q"] = q

    # if the playlist has an owner, process the owner info
    if pl_data["owner"]:
        pl_data["owner_user_id"] = pl_data["owner"]["id"]
        yield from process_owner(pl_data["owner"], playlist_id)
    pl_data.pop('owner')
    yield op.upsert(table="playlist", data=pl_data)
    playlist_tracks_params = {"playlist_id": playlist_id, "additional_types": "track"}
    pl_response_page = get_api_response(obj, "playlist_items", playlist_tracks_params)

    # if the playlist has tracks, process the tracks
    if pl_response_page["items"]:
        yield from process_tracks(pl_response_page["items"], playlist_id)


def process_owner (pl_owner, playlist_id):
    user_data = flatten_dict(pl_owner)
    yield op.upsert(table="user", data=user_data)

def process_tracks (tracklist: list, playlist_id):
    summary_first_item = {'id': tracklist[0]['track']['id'], 'name': tracklist[0]['track']['name']}
    log.fine(f"processing tracks. First item starts: {summary_first_item}, Total items: {len(tracklist)}")
    for t in tracklist:
        if t["track"]:
            track_data = flatten_dict(t['track'])
            # don't write local tracks, they don't have much info
            if not t["is_local"]:
                track_data["playlist"] = playlist_id
                yield op.upsert(table="playlist_track", data=track_data)

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
def get_api_response(obj, method_name, payload=None):
    try:
        if payload is None:
            payload = {}
        method = getattr(obj, method_name)
        response_page = method(**payload)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

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
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
