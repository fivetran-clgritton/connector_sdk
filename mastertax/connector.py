# Import requests to make HTTP calls to API
import requests as rq
import traceback
import datetime
import time
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code

def schema(configuration: dict):
    """
    # Define the schema function which lets you configure the schema your connector delivers.
    # See the technical reference documentation for more details on the schema function:
    # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    :param configuration: a dictionary that holds the configuration settings for the connector.
    :return: a list of tables with primary keys and any datatypes that we want to specify
    """
    return []


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):

    try:
        token_header = make_headers(configuration, state)
        log.info(str(token_header))
        from_ts = state['to_ts'] if 'to_ts' in state else datetime.datetime.now() - datetime.timedelta(days=1)
        now = datetime.datetime.now()
        to_ts = now.strftime("%Y-%m-%dT%H:%M:%S")

        # Update the state with the new cursor position, incremented by 1.
        new_state = {"to_ts": to_ts}
        log.fine(f"state updated, new state: {repr(new_state)}")

        # Yield a checkpoint operation to save the new state.
        yield op.checkpoint(state=new_state)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

def make_headers(conf, state):
    """
    Create authentication headers, reusing a cached token if possible.

    :param conf: Dictionary containing authentication details.
    :param state: Dictionary storing token and expiration details.
    :return: Tuple (headers, updated_state)
    """

    url = "https://api.adp.com/auth/oauth/v2/token"
    cert_path = "InsperityCorpMutualSSL.crt"
    key_path = "InsperityCorpMutualSSL_auth.key"
    write_to_file(conf["crtFile"], cert_path)
    write_to_file(conf["keyFile"], key_path)
    payload = f"grant_type=client_credentials&client_id={conf['clientId']}&client_secret={conf['clientSecret']}"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }

    # No valid token OR token expiring within 1 hour, request a new one
    try:
        auth_response = rq.request("POST", url, headers=headers, data=payload, cert=(cert_path, key_path))
        auth_response.raise_for_status()
        auth_page = auth_response.json()

        # Extract token safely
        auth_token = auth_page.get("access_token")
        token_expiry = auth_page.get("expires_in", 3600)  # Default to 1 hour

        if not auth_token:
            raise ValueError("Authentication failed: accessToken missing in response")

        return {"Authorization": f"Bearer {auth_token}", "Accept": "application/json"}

    except rq.exceptions.RequestException as e:
        raise RuntimeError(f"❌ Failed to authenticate: {e}")

def write_to_file(text: str, filename: str):
    """
    Writes the given text to a .pem file.

    :param text: The text to be written to the file
    :param filename: The name of the file
    """
    try:
        with open(filename, "w") as pem_file:
            pem_file.write(text)
        print(f"Successfully written to {filename}")
    except Exception as e:
        print(f"Error writing to file: {e}")

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

