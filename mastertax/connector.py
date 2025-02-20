# Import requests to make HTTP calls to API
from time import sleep

import requests as rq
import traceback
import datetime
import time
import os
import json
import csv
import uuid
import zipfile

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code

cert_path = "InsperityCorpMutualSSL.crt"
key_path = "InsperityCorpMutualSSL_auth.key"
mv_deph_cols = ["DEPOSIT_ID", "BATCH_DEPOSIT_ID", "PROCESS_ID", "PROCESS_TYPE", "PROCESS_TIMESTAMP", "TAX_CODE_ID", "TAX_CODE", "TAX_DESCRIPTION", "SHORT_DESC", "TAX", "TAXABLE", "EXEMPT_WAGES", "EMPLOYEE_COUNT", "PAYMENT_TYPE", "PAYMENT_STATUS", "CONFIRMATION", "FILING_CONFIRMATION", "COMPANY_ID", "COMPANY_NAME", "REPORTING_PAYROLL", "FEIN", "EIN", "PAYMENT_FREQUENCY", "PERIOD_START", "PERIOD_END", "DUE_DATE", "DEPOSIT_DATE", "DEPOSIT_LATE", "PAYMENT_METHOD", "EFT_REFERENCE", "BANK_ACCOUNT", "BANK_TRA", "AP_VENDOR", "AP_SITE", "PAYEE_NAME", "PAYEE_ADDRESS_NAME", "PAYEE_ADDRESS_LINE1", "PAYEE_ADDRESS_LINE2", "PAYEE_CITY", "PAYEE_STATE", "PAYEE_ZIP", "EFT_ADDENDA_DETAIL", "RETURN_TYPE", "CREATOR", "CREATE_DATE", "MODIFIER", "MODIFY_DATE", "VALIDATOR", "VALIDATE_DATE", "VALIDATE_STATUS", "FILE_NAME", "FILE_CREATE_DATE", "FILE_RECORD_NUM", "EFT_BATCH_ID", "ACTUAL_SETTLEMENT_DATE", "CHECK_PRINTED_WITH", "CHECK_PRINT_DATE", "CHECK_PRINT_PROC_ID", "BANK_RECON_ID", "BANK_RECON_STATUS", "BANK_RECON_TRAN_DATE", "BANK_RECON_TIMESTAMP", "BANK_RECON_BY", "NOTES", "REFUND_AMOUNT_REQUESTED", "REFUND_AMOUNT_EXPECTED", "REFUND_AMOUNT_RECEIVED", "REFUND_RECEIVE_DATE", "REFUND_NOTE", "REFUND_STATUS", "COUPON_FORM_CODE", "COUPON_FORM_DESCRIPTION", "COUPON_GENERIC_FLAG", "COUPON_FILING_METHOD", "COUPON_STATUS", "COUPON_CONFIRMATION"]

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
        now = datetime.datetime.now()
        to_ts = now.strftime("%Y-%m-%dT%H:%M:%S")

        # Update the state with the new cursor position, incremented by 1.
        new_state = {"to_ts": to_ts}
        log.fine(f"state updated, new state: {repr(new_state)}")

        for e in json.loads(configuration["dataExtracts"]):
            print(e)
            yield from sync_items(configuration, state, token_header, e)

        # Yield a checkpoint operation to save the new state.
        yield op.checkpoint(state=new_state)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

def sync_items(configuration: dict, state: dict, headers: dict, extract: dict):
    # Get response from API call.
    complete = False
    conversation_id = str(uuid.uuid4())
    headers["ADP-ConversationID"] = conversation_id

    submit_endpoint = "/tax/v1/organization-tax-data/processing-jobs/actions/submit"
    base_url = configuration["baseURL"]
    status, response_id = submit_process(base_url+submit_endpoint, headers, extract)
    log.info(status)
    status_endpoint = f"/tax/v1/organization-tax-data/processing-jobs/{response_id}/processing-status?processName=DATA_EXTRACT"

    while not complete:
        sleep(10)
        log.info(f"checking export status for {conversation_id}")
        status, output_id = get_process_status(base_url+status_endpoint, headers)
        if status == "completed":
            complete = True
            log.info(f"process complete for {extract}")
            content_endpoint = f"/tax/v1/organization-tax-data/processing-job-outputs/{output_id}/content?processName=DATA_EXTRACT"
            download_file(base_url+content_endpoint, headers, "test")

    zip_path = "test.zip"
    extract_path = configuration["unzipDirectory"]

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    print(f"ZIP file extracted to: {extract_path}")

    matching_files = [fn for fn in os.listdir(extract_path) if "MV_DEPH" in fn]
    for m in matching_files:
        log.fine(f"processing {m}")
        yield from upsert_rows(f"{extract_path}{m}", mv_deph_cols)


    #log.info(str(response_page["totalResults"]) + " results for topic " + topic)

    #items = response_page.get("articles", [])

    #for a in items:
    #    yield op.upsert(table="article", data=r)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of interruptions.
    yield op.checkpoint(state)

def upsert_rows (filename: str, colnames: list):
    #json_data = []
    log.fine(f"upserting rows for {filename}")
    with open(filename, "r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter="\t")  # Tab-delimited

        for row in reader:
            yield op.upsert(table="test", data=dict(zip(colnames, row)))


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
def submit_process(url: str, headers: dict, payload: dict):
    retry_wait_seconds = 60
    max_retries = 3

    for attempt in range(max_retries + 1):
        response = rq.post(url, headers=headers, data=json.dumps(payload), cert=(cert_path, key_path))

        if response.status_code == 400 and attempt < max_retries:
            print(f"Received 400 response, waiting {retry_wait_seconds} seconds to retry...")
            time.sleep(retry_wait_seconds)
            continue  # Retry the request

        response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
        response_page = response.json()
        log.fine(response_page)
        status = response_page.get("_confirmMessage", {}).get("requestStatus")
        resource_id = response_page.get("_confirmMessage", {}).get("messages", [{}])[0].get("resourceID")

        return status, resource_id

def get_process_status (url: str, headers: dict):

    response = rq.get(url, headers=headers, cert=(cert_path, key_path))
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    status = response_page.get("processingJob").get("processingJobStatusCode")
    output_id = response_page.get("processingJob").get("processOutputID")

    return status, output_id

def download_file(url: str, headers: dict, name: str):

    headers["Range"] = "bytes=0-"
    response = rq.get(url, headers=headers, stream=True, cert=(cert_path, key_path))
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.

    with open(f"{name}.zip", "wb") as file:
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)

    log.info("ZIP file downloaded successfully!")

    # handle 400s
    """{
        "_confirmMessage": {
            "requestStatus": "failure",
            "applicationID": "MasterTaxTest",
            "messageID": "96a87c39-35de-4990-93a9-045a717fd017",
            "messageDateTime": "2025-02-20T08:57:56.060Z",
            "messages": [
                {
                    "messageCode": "400",
                    "messageTypeCode": "error",
                    "messageText": "The request cannot be completed. The requested content is no longer available for download."
                }
            ]
        }
    }
    """

def make_headers(conf, state):
    """
    Create authentication headers, reusing a cached token if possible.

    :param conf: Dictionary containing authentication details.
    :param state: Dictionary storing token and expiration details.
    :return: Tuple (headers, updated_state)
    """

    url = "https://api.adp.com/auth/oauth/v2/token"
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

        return {"Authorization": f"Bearer {auth_token}","Content-Type": "application/json"}

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

