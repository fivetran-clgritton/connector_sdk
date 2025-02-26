# This is a simple example for how to work with the fivetran_connector_sdk module.
# It defines a simple `update` method, which upserts some data to a table named "hello".
# This example is the simplest possible as it doesn't define a schema() function, however it does not therefore provide a good template for writing a real connector.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

import datetime
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def update(configuration: dict, state: dict):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    # See the technical reference documentation for more details on the update function
    # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    # The function takes two parameters:
    # - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
    # - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    # The state dictionary is empty for the first sync or for any full re-sync
    """

    start_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat("T", "milliseconds").replace("+00:00", "Z")
    from_ts, to_ts = set_timeranges(state, start_timestamp)
    state["to_ts"] = to_ts
    log.fine(f"start: {from_ts} end: {to_ts}, until we reach {start_timestamp}")

    more_data = True
    while more_data:

        state["to_ts"] = to_ts
        # The yield statement returns a generator object.
        # This generator will yield an upsert operation to the Fivetran connector.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "hello".
        # - The second argument is a dictionary containing the data to be upserted,
        yield op.upsert(table="timestamps", data={"message": f"from {from_ts} to {to_ts}"})

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state)

        if to_ts < start_timestamp:
            # get new timestamp range if we haven't reached the start of the sync yet
            ts_from, to_ts = set_timeranges(state, start_timestamp)
        else:
            more_data = False

def set_timeranges(state, start_timestamp):
    """
    Takes in current state and start timestamp of current sync.
    from_ts is always either the end of the last sync or the initialSyncStart found in the config file.
    If from_ts is more than 30 days ago, then set a to_ts that is 30 days later than from_ts.
    Otherwise, to_ts is the time that this sync was triggered.
    :param state:
    :param start_timestamp:
    :return: from_ts, to_ts
    """
    if 'to_ts' in state:
        from_ts = state['to_ts']
    else:
        from_ts = "2024-01-01T00:00:00.000Z"

    if is_older_than_30_days(from_ts):  # Pass the string, since function handles conversion
        from_ts_dt = datetime.datetime.fromisoformat(from_ts.replace("Z", "+00:00"))
        to_ts = from_ts_dt + datetime.timedelta(days=30)
        to_ts = to_ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    else:
        to_ts = start_timestamp

    return from_ts, to_ts

def is_older_than_30_days(date_to_check):
    """
    Checks whether date_to_check is older than 30 days.
    Is time-zone aware and handles date_to_check being a string and not a datetime
    :param date_to_check:
    :return: boolean based on whether date is older than 30 days
    """
    now = datetime.datetime.now(datetime.UTC)  # Timezone-aware UTC datetime

    # Convert to datetime if input is a string
    if isinstance(date_to_check, str):
        date_to_check = datetime.datetime.fromisoformat(date_to_check.replace("Z", "+00:00"))

    return date_to_check < now - datetime.timedelta(days=30)

# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

