from fivetran_connector_sdk import Connector  # Required for defining the Connector object
from fivetran_connector_sdk import Operations as op  # For Upsert, Update, Delete, and Checkpoint
from fivetran_connector_sdk import Logging as log  # For enabling logs
import requests
import json
from base64 import b64encode

def extract_parameter_data(data, parameter):
    for item in data:
        if item["parameter"] == parameter:
            return {entry["date"]: entry["value"] for entry in item["coordinates"][0]["dates"]}
    return {}

# Define the `update` function (required)
def update(configuration: dict, state: dict):
    log.info("Starting the update function.")

    # Extract configuration parameters
    username = configuration.get("username", "default_user")
    password = configuration.get("password", "default_pass")

    # Set up API request parameters
    start_date = "2024-12-11T00:00:00Z"
    end_date = "2024-12-12T00:00:00Z"
    interval = "PT1H"
    parameters = "t_2m:C,precip_1h:mm,wind_speed_10m:ms"
    location = "52.520551,13.461804"
    output_format = "json"

    try:

        # Perform the API request
        auth_string = f"{username}:{password}"
        b64_encoded_auth = b64encode(auth_string.encode('utf-8')).decode('utf-8')

        # Set the Authorization header
        headers = {"Authorization": f"Basic {b64_encoded_auth}"}
        log.info(str(headers))

        token = requests.get('https://login.meteomatics.com/api/v1/token', headers=headers).json()["access_token"]
        url = (f"https://api.meteomatics.com/{start_date}--{end_date}:{interval}/{parameters}/"
               f"{location}/{output_format}?access_token={token}")

        # Perform the API request
        response = requests.get(url)
        response.raise_for_status()

        weather_data = response.json()
        # Validate the response structure
        if "data" not in weather_data:
            log.severe("API response does not contain 'data' key.")

        table_name = "weather_data"
        temperature_data = extract_parameter_data(weather_data["data"], "t_2m:C")
        precipitation_data = extract_parameter_data(weather_data["data"], "precip_1h:mm")
        wind_speed_data = extract_parameter_data(weather_data["data"], "wind_speed_10m:ms")
        timestamps = sorted(temperature_data.keys())
        for timestamp in timestamps:
            try:
                # Safely access fields with default fallback
                yield op.upsert(
                    table=table_name,
                    data={
                        "timestamp": timestamp,
                        "temperature": temperature_data.get(timestamp),
                        "precipitation": precipitation_data.get(timestamp),
                        "wind_speed": wind_speed_data.get(timestamp)
                    },
                )
            except KeyError as e:
                log.warning(f"Missing expected key in record: {e}")

        # Checkpoint state
        new_state = {"last_synced": end_date}
        yield op.checkpoint(state=new_state)
        log.info("Checkpoint completed successfully.")

    except Exception as e:
        log.severe(str(e))


# Define the `schema` function 
def schema(configuration: dict):
    """
    Defines the schema for the weather data.
    """
    log.info("Entering the schema function.")

    schema_def = [
            {
                "table": "weather_data",
                "primary_key": ["timestamp"],
                "columns": {"timestamp": "UTC_DATETIME"}
            }
        ]

    #log.info("Validating schema format.")
        #if not isinstance(schema_def, dict):
        #    log.severe("Schema definition is not a dictionary!")
        #    raise TypeError("Schema definition must be a dictionary.")
        #log.info(f"Schema successfully defined: {json.dumps(schema_def, indent=2)}")

    #except Exception as e:
    #    log.severe("Error while defining schema.", exception=e)
    #    raise

    log.info("Exiting the schema function.")
    return schema_def


# Instantiate the Connector object
connector = Connector(update=update, schema=schema)

# Entry point for running the connector
if __name__ == "__main__":
    connector.run()

# fivetran deploy --api-key V3Myc2s0YWtia2tRcW5sdzpHS0VLN0hud013SHdVMWthNmg1R2dQbDZQZTJtd0w3Yg== --destination Snowflake_Sandbox --connection meteomatics_weather