# This is a simple example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to work with data frames.
# It also shows how to handle NaN values, as we do not support NaN inputs.
# Customers should convert NaN values to None before sending data.
# It shows the use of a requirements.txt file and a connector that calls a publicly available API to get random profile data
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
# Import the requests module for making HTTP requests, aliased as rq.
import requests as rq
import pandas as pd
import time


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.


def schema(configuration: dict):
    return [
        {
            "table": "profile_1",
            "primary_key": ["id"],
            # Columns and data types will be inferred by Fivetran
        },
        {
            "table": "profile_2",
            "primary_key": ["id"],
            # Columns and data types will be inferred by Fivetran
        },
        {
            "table": "profile_3",
            "primary_key": ["id"],
            # Columns and data types will be inferred by Fivetran
        },
        {
            "table": "profile_4",
            "primary_key": ["id"],
            # Columns and data types will be inferred by Fivetran
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.


def update(configuration: dict, state: dict):
    log.warning("Example: QuickStart Examples - User Profiles")

    # Retrieve the last processed profile ID from state or set it to 0 if not present
    profile_cursor = 0

    # Fetch new data and return DataFrames for profile, location, and login tables
    profile_df, cursor = get_data(0)

    # Approaches to upsert the records from dataFrame
    start_time = time.time()
    log.info(f"starting method 1 at {start_time}")
    yield from upsert_dataframe_approach_1(profile_df=profile_df, state=state, cursor=cursor)
    elapsed_time = time.time() - start_time
    log.info(f"upserted in {elapsed_time} with method 1")

    start_time = time.time()
    log.info(f"starting method 2 at {start_time}")
    yield from upsert_dataframe_approach_2(profile_df=profile_df, state=state, cursor=cursor)
    elapsed_time = time.time() - start_time
    log.info(f"upserted in {elapsed_time} with method 2")

    start_time = time.time()
    log.info(f"starting method 3 at {start_time}")
    yield from upsert_dataframe_approach_3(profile_df=profile_df, state=state, cursor=cursor)
    elapsed_time = time.time() - start_time
    log.info(f"upserted in {elapsed_time} with method 3")

    start_time = time.time()
    log.info(f"starting method 4 at {start_time}")
    yield from upsert_dataframe_approach_4(profile_df=profile_df, state=state, cursor=cursor)
    elapsed_time = time.time() - start_time
    log.info(f"upserted in {elapsed_time} with method 4")

    # Approaches to handle NaN values in dataframes
    # yield from handle_tables_with_nan(state)


# Function to fetch data from an API and process it into DataFrames
def get_data(cursor):
    # Initialize empty DataFrames for profile, location, and login tables
    profile_df = pd.DataFrame([])

    # Fetch data for 3 iterations, each with a new profile
    for i in range(5000):
        # Increment primary key
        cursor += 1
        if cursor % 500 == 0:
            log.info(f"{cursor} records gathered")
        # Request data from an external API
        response = rq.get("https://randomuser.me/api/")
        data = response.json()
        data = data["results"][0]
        # log.fine(data)

        # Process and store profile data
        profile_data = {
            "id": cursor,
            "fullName": data["name"]["title"] + " " + data["name"]["first"] + " " + data["name"]["last"],
            "gender": data["gender"],
            "email": data["email"],
            "age": data["dob"]["age"],
            "date": data["dob"]["date"],
            "mobile": data["cell"],
            "nationality": data["nat"],
            # Stores the JSON data e.g. {"large":"https://randomuser.me/api/portraits/women/67.jpg","medium":"https://randomuser.me/api/portraits/med/women/67.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/67.jpg"}
            "picture": data["picture"]
        }
        profile_df = pd.concat(
            [profile_df, pd.DataFrame([profile_data])], ignore_index=True)

    return profile_df, cursor



def upsert_dataframe_approach_1(profile_df, state, cursor):
    # APPROACH 1: Gives you direct access to individual row values by column name, Slower approach, helpful for custom row handling
    # UPSERT all profile table data, checkpoint periodically to save progress. In this example every 5 records.
    for index, row in profile_df.iterrows():
        yield op.upsert("profile_1", {col: row[col] for col in profile_df.columns})
        if index % 5 == 0:
            state["profile_cursor"] = row["id"]

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            # yield op.checkpoint(state)

    # Checkpointing at the end of the "profile" table data processing
    state["profile_cursor"] = cursor

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


def upsert_dataframe_approach_2(profile_df, state, cursor):
    # APPROACH 2: Generally faster and more memory-efficient, Simplifies the code since rows are already dictionaries and can be used directly
    # UPSERT all location table data.
    # Iterate over each row in the DataFrame, converting it to a dictionary
    for row in profile_df.to_dict("records"):
        yield op.upsert("profile_2", row)

    # Checkpointing at the end of the "location" table data processing
    state["profile_cursor"] = cursor

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


def upsert_dataframe_approach_3(profile_df, state, cursor):
    # APPROACH 3: Faster approach, Keeps track of the original indices of the rows, which can be useful for certain operations that require indexing information.
    # UPSERT all login table data.
    # Iterate over the values of the dictionary (which are the DataFrame rows)
    for value in profile_df.to_dict("index").values():
        yield op.upsert("profile_3", value)

    # Checkpointing at the end of the "login" table data processing
    state["profile_cursor"] = cursor

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


def upsert_dataframe_approach_4(profile_df, state, cursor):
    # APPROACH 3: Faster approach, Keeps track of the original indices of the rows, which can be useful for certain operations that require indexing information.
    # UPSERT all login table data.
    # Iterate over the values of the dictionary (which are the DataFrame rows)
    for index, row in profile_df.iterrows():
        yield op.upsert("profile_4", data={col: row[col] for col in profile_df.columns})

    # Checkpointing at the end of the "login" table data processing
    state["profile_cursor"] = cursor

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the "fivetran debug" command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()
