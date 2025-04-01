# This is a simple example illustrating how to work with the fivetran_connector_sdk module.
# This is an example to show how we can sync records from AWS Athena by using Connector SDK and SQLAlchemy and PyAthena.
# You need to provide your credentials for this example to work.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

import pyodbc
import pandas as pd
import json  # Import the json module to handle JSON data.
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import gcsfs


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    """
    return [
        {
            "table": TABLE_NAME,  # Name of the table in the destination.
            "primary_key": ["customer_id"]  # Primary key column(s) for the table.
        }
    ]
    """
    return []

def create_connection(configuration):
    conn = pyodbc.connect(f'DSN={configuration["DSN"]};UID={configuration["user"]};PWD={configuration["password"]}')

    #conn_str = (
    #    f'DRIVER=Sage Line 50 v28 ODBC 32-bit;UID={configuration["user"]};PWD={configuration["password"]};'
    #    f'CompanyFile=C:\\Path\\To\\Sage\\Company.001\\ACCDATA;'
    #)
    #conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to Sage 50 ODBC successfully.")

    return cursor


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    db_cursor = create_connection(configuration)

    if state.get("tables"):
        tables=state["tables"]
    else:
        tables = [row.table_name for row in db_cursor.tables(tableType='TABLE')]
        log.fine(str(tables))

    for t in tables:
        log.fine(f"Processing table: {t}")

        db_cursor.execute(f"SELECT * FROM {t}")
        row_count = db_cursor.rowcount
        log.fine(f"Table {t} has {row_count} rows")
        columns = [col[0] for col in db_cursor.description]
        #log.fine(str(columns))# Get column names
        batch_size = 100

        while True:
            rows = db_cursor.fetchmany(batch_size)  # Fetch rows in batches
            if not rows:
                break  # Exit when no more rows are left

            for row in rows:
                row_dict = dict(zip(columns, row))  # Convert row to dictionary
                yield op.upsert(table=t, data=row_dict)  # Stream the result

        tables.remove(t)
        state["tables"] = tables
        yield op.checkpoint(state)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Test it by using the `debug` command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
