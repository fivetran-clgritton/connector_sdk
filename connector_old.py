from fivetran_connector_sdk import Connector  # Required for defining the Connector object
from fivetran_connector_sdk import Operations as op  # For Upsert, Update, Delete, and Checkpoint
from fivetran_connector_sdk import Logging as log  # For enabling logs
import requests
import json
import traceback
from base64 import b64encode


# Define the `update` function (required)
def update(configuration: dict, state: dict):
    log.info(str(configuration))
    try:
        log.info("Starting the update function.")

        # Extract configuration parameters
        username = configuration.get("username", "default_user")
        password = configuration.get("password", "default_pass")

        # Set up API request parameters
        start_date = "2024-12-04T00:00:00Z"
        end_date = "2024-12-07T00:00:00Z"
        interval = "PT1H"
        parameters = "t_2m:C,precip_1h:mm,wind_speed_10m:ms"
        location = "52.520551,13.461804"
        output_format = "json"

        url = "https://api.meteomatics.com/"+start_date+"--"+end_date+":"+interval+"/"+parameters+"/"+location+"/"+output_format

        # Perform the API request
        auth_string = f"{username}:{password}"
        b64_encoded_auth = b64encode(auth_string.encode('utf-8')).decode('utf-8')

        # Set the Authorization header
        headers = {"Authorization": f"Basic {b64_encoded_auth}"}

        response = requests.get(url, headers=headers)
        #response = requests.get(url, auth=(username, password))
        response.raise_for_status()

        weather_data = response.json()

        # Validate the response structure
        if "data" not in weather_data:
            log.severe("API response does not contain 'data' key.")
            return

        table_name = "weather_data"
        log.info(weather_data)
        for record in weather_data["data"]:
            # Safely access fields with default fallback
            timestamp = record.get("date")
            temperature = record.get("t_2m:C", None)
            precipitation = record.get("precip_1h:mm", None)
            wind_speed = record.get("wind_speed_10m:ms", None)

            yield op.upsert(
                table=table_name,
                data={
                    "timestamp": timestamp,
                    "temperature": temperature,
                    "precipitation": precipitation,
                    "wind_speed": wind_speed,
                },
            )

            # Checkpoint state
            new_state = {"last_synced": end_date}
            yield op.checkpoint(state=new_state)
            log.info("Checkpoint completed successfully.")

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)

# Define the `schema` function 
def schema(configuration: dict):
    """
    Defines the schema for the weather data.
    """

    return [
            {
                "table": "weather_data",
                "primary_key": ["timestamp"],
                "columns": {
                    "timestamp", "UTC_DATETIME",
                    "temperature", "FLOAT",
                    "precipitation", "FLOAT",
                    "wind_speed", "FLOAT"
                }
            }
        ]



# Instantiate the Connector object
connector = Connector(update=update, schema=schema)

# Entry point for running the connector
if __name__ == "__main__":
    connector.debug()

# fivetran deploy --api-key V3Myc2s0YWtia2tRcW5sdzpHS0VLN0hud013SHdVMWthNmg1R2dQbDZQZTJtd0w3Yg== --destination Snowflake_Sandbox --connection meteomatics_weather